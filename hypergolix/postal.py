'''
LICENSING
-------------------------------------------------

hypergolix: A python Golix client.
    Copyright (C) 2016 Muterra, Inc.
    
    Contributors
    ------------
    Nick Badger
        badg@muterra.io | badg@nickbadger.com | nickbadger.com

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the
    Free Software Foundation, Inc.,
    51 Franklin Street,
    Fifth Floor,
    Boston, MA  02110-1301 USA

------------------------------------------------------
'''


# Global dependencies
import logging
import collections
import weakref
import queue
import threading
import traceback
import asyncio
import loopa

# Local dependencies
from .persistence import _GidcLite
from .persistence import _GeocLite
from .persistence import _GobsLite
from .persistence import _GobdLite
from .persistence import _GdxxLite
from .persistence import _GarqLite

from .utils import SetMap
from .utils import WeakSetMap
from .utils import weak_property

from .gao import GAO


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports.
__all__ = [
    # 'PersistenceCore',
]


# ###############################################
# Lib
# ###############################################
            

_MrPostcard = collections.namedtuple(
    typename = '_MrPostcard',
    field_names = ('subscription', 'notification'),
)

            
class _PostmanBase(loopa.TaskLooper):
    ''' Tracks, delivers notifications about objects using **only weak
    references** to them. Threadsafe.
    
    ♫ Please Mister Postman... ♫
    
    Question: should the distributed state management of GARQ recipients
    be managed here, or in the bookie (where it currently is)?
    '''
    _bookie = weak_property('__bookie')
    _librarian = weak_property('__librarian')
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # The scheduling queue is created at loop init.
        self._scheduled = None
        # The delayed lookup. <awaiting ghid>: set(<subscribed ghids>)
        self._deferred = SetMap()
        
        # Resolve primitives into their schedulers.
        self._scheduler_lookup = {
            _GidcLite: self._schedule_gidc,
            _GeocLite: self._schedule_geoc,
            _GobsLite: self._schedule_gobs,
            _GobdLite: self._schedule_gobd,
            _GdxxLite: self._schedule_gdxx,
            _GarqLite: self._schedule_garq
        }
        
    def assemble(self, librarian, bookie):
        # Links the librarian and bookie.
        self._librarian = weakref.proxy(librarian)
        self._bookie = weakref.proxy(bookie)
        
    async def await_idle(self):
        ''' Wait until the postman has no more deliveries to perform.
        '''
        await self._scheduled.join()
        
    async def loop_init(self):
        ''' Init all of the needed async primitives.
        '''
        self._scheduled = asyncio.Queue()
        
    async def loop_run(self):
        ''' Deliver notifications as soon as they are available.
        TODO: support parallel sending.
        '''
        try:
            subscription, notification = await self._scheduled.get()
            
            logger.info(
                'Postman out for delivery on {!s}.'.format(subscription)
            )
            logger.debug(
                (
                    'Additionally, {!s} missing ghids are blocking updates ' +
                    'for other subscriptions.'
                ).format(len(self._deferred))
            )
            # We can't spin this out into a thread because some of our
            # delivery mechanisms want this to have an event loop.
            await self._deliver(subscription, notification)
            
        except asyncio.CancelledError:
            raise
        
        except Exception:
            logger.error(
                (
                    'Exception during subscription delivery for {!s} w/ ' +
                    'notification {!s} w/ traceback:\n'
                ).format(subscription, notification) +
                ''.join(traceback.format_exc())
            )
            
        finally:
            self._scheduled.task_done()
        
    async def loop_stop(self):
        ''' Clear the async primitives.
        '''
        # Ehhhhh, should the queue be emptied before being destroyed?
        self._scheduled = None
        
    async def schedule(self, obj, removed=False):
        ''' Schedules update delivery for the passed object.
        '''
        # It's possible we're being told to schedule nothing, so catch that
        # here.
        for deferred in self._has_deferred(obj):
            await self._scheduled.put(deferred)
        
        try:
            scheduler = self._scheduler_lookup(type(obj))
        
        except KeyError:
            raise TypeError(
                'Could not schedule: does not appear to be a Golix ' +
                'primitive.'
            ) from None
        
        else:
            await scheduler(obj, removed)
            
        return True
        
    async def _schedule_gidc(self, obj, removed):
        # GIDC will never trigger a subscription.
        pass
        
    async def _schedule_geoc(self, obj, removed):
        # GEOC will never trigger a subscription directly, though they might
        # have deferred updates.
        # Note that these have already been put into _MrPostcard form.
        for deferred in self._deferred.pop_any(obj.ghid):
            await self._scheduled.put(deferred)
        
    async def _schedule_gobs(self, obj, removed):
        # GOBS will never trigger a subscription.
        pass
        
    async def _schedule_gobd(self, obj, removed):
        # GOBD might trigger a subscription! But, we also might to need to
        # defer it. Or, we might be removing it.
        if removed:
            debinding_ghids = self._bookie.debind_status(obj.ghid)
            if not debinding_ghids:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            for debinding_ghid in debinding_ghids:
                await self._scheduled.put(
                    _MrPostcard(obj.ghid, debinding_ghid)
                )
        else:
            notifier = _MrPostcard(obj.ghid, obj.frame_ghid)
            if not (await self._librarian.contains(obj.target)):
                self._defer_update(
                    awaiting_ghid = obj.target,
                    postcard = notifier,
                )
            else:
                await self._scheduled.put(notifier)
        
    async def _schedule_gdxx(self, obj, removed):
        # GDXX will never directly trigger a subscription. If they are removing
        # a subscribed object, the actual removal (in the undertaker GC) will
        # trigger a subscription without us.
        pass
        
    async def _schedule_garq(self, obj, removed):
        # GARQ might trigger a subscription! Or we might be removing it.
        if removed:
            debinding_ghids = self._bookie.debind_status(obj.ghid)
            if not debinding_ghids:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            for debinding_ghid in debinding_ghids:
                await self._scheduled.put(
                    _MrPostcard(obj.recipient, debinding_ghid)
                )
        else:
            await self._scheduled.put(
                _MrPostcard(obj.recipient, obj.ghid)
            )
            
    def _defer_update(self, awaiting_ghid, postcard):
        ''' Defer a subscription notification until the awaiting_ghid is
        received as well.
        '''
        self._deferred.add(awaiting_ghid, postcard)
        logger.debug('Postman update deferred for ' + str(awaiting_ghid))
            
    async def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        # We need to freeze the listeners before we operate on them, but we
        # don't need to lock them while we go through all of the callbacks.
        # Instead, just sacrifice any subs being added concurrently to the
        # current delivery run.
        pass


class MrPostman(_PostmanBase):
    ''' Postman to use for local persistence systems.
    
    Note that MrPostman doesn't need to worry about silencing updates,
    because the persistence ingestion tract will only result in a mail
    run if there's a new object there. So, by definition, any re-sent
    objects will be DOA.
    '''
    
    def __init__(self):
        super().__init__()
        self._rolodex = None
        self._golcore = None
        self._oracle = None
        self._salmonator = None
        
    def assemble(self, golcore, oracle, librarian, bookie, rolodex,
                 salmonator):
        super().assemble(librarian, bookie)
        self._golcore = weakref.proxy(golcore)
        self._rolodex = weakref.proxy(rolodex)
        self._oracle = weakref.proxy(oracle)
        self._salmonator = weakref.proxy(salmonator)
            
    async def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        # We just got a garq for our identity. Rolodex handles these.
        if subscription == self._golcore.whoami:
            await self._rolodex.notification_handler(
                subscription,
                notification
            )
        
        # Anything else is an object subscription. Handle those by directly,
        # but only if we have them in memory.
        elif subscription in self._oracle:
            # The ingestion pipeline will already have applied any new updates
            # to the ghidproxy.
            obj = await self._oracle.get_object(GAO, subscription)
            logger.debug(''.join((
                'SUBSCRIPTION ',
                str(subscription),
                ' delivery STARTING. Notification: ',
                str(notification)
                
            )))
            await obj.pull(notification)
                
        # We don't have the sub in memory, so we need to remove it.
        else:
            logger.debug(''.join((
                'SUBSCRIPTION ',
                str(subscription),
                ' delivery IGNORED: not in memory. Notification: ',
                str(notification)
                
            )))
            self._salmonator.deregister(subscription)
        
        
class PostOffice(_PostmanBase):
    ''' Postman to use for remote persistence servers.
    '''
    
    def __init__(self):
        super().__init__()
        # By using WeakSetMap we can automatically handle dropped connections
        # Lookup <subscribed ghid>: set(<subscribed callbacks>)
        self._opslock_listen = threading.Lock()
        self._listeners = WeakSetMap()
        
    def subscribe(self, ghid, callback):
        ''' Tells the postman that the watching_session would like to be
        updated about ghid.
        
        TODO: instead of postoffices subscribing with a callback, they
        should subscribe with a session. That way, we're not spewing off
        extra strong references and just generally mangling up our
        object lifetimes.
        '''
        # First add the subscription listeners
        with self._opslock_listen:
            self._listeners.add(ghid, callback)
            
        # Now manually reinstate any desired notifications for garq requests
        # that have yet to be handled
        for existing_mail in self._bookie.recipient_status(ghid):
            
            # HEY LOOK AT ME THIS IS AN ERROR! This is a call to a coro, but
            # it's within a function. But Postal needs a total workover anyways
            # so punt on it for now
            
            obj = self._librarian.summarize(existing_mail)
            self.schedule(obj)
            
    def unsubscribe(self, ghid, callback):
        ''' Remove the callback for ghid. Indempotent; will never raise
        a keyerror.
        '''
        self._listeners.discard(ghid, callback)
            
    async def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        # We need to freeze the listeners before we operate on them, but we
        # don't need to lock them while we go through all of the callbacks.
        # Instead, just sacrifice any subs being added concurrently to the
        # current delivery run.
        callbacks = self._listeners.get_any(subscription)
        postcard = _MrPostcard(subscription, notification)
                
        for callback in callbacks:
            callback(*postcard)
