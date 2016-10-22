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
    
    def __init__(self):
        self._bookie = None
        self._librarian = None
        
        # The scheduling queue
        self._scheduled = None
        
        # The delayed lookup. <awaiting ghid>: set(<subscribed ghids>)
        self._deferred = SetMap()
        
    def assemble(self, librarian, bookie):
        # Links the librarian and bookie.
        self._librarian = weakref.proxy(librarian)
        self._bookie = weakref.proxy(bookie)
        
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
        
    async def loop_stop(self):
        ''' Clear the async primitives.
        '''
        # Ehhhhh, should the queue be emptied before being destroyed?
        self._scheduled = None
        
    async def schedule(self, obj, removed=False):
        ''' Schedules update delivery for the passed object.
        '''
        for deferred in self._has_deferred(obj):
            await self._scheduled.put(deferred)
            
        for primitive, scheduler in ((_GidcLite, self._schedule_gidc),
                                     (_GeocLite, self._schedule_geoc),
                                     (_GobsLite, self._schedule_gobs),
                                     (_GobdLite, self._schedule_gobd),
                                     (_GdxxLite, self._schedule_gdxx),
                                     (_GarqLite, self._schedule_garq)):
            if isinstance(obj, primitive):
                await scheduler(obj, removed)
                break
        else:
            raise TypeError('Could not schedule: wrong obj type.')
            
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
            if obj.target not in self._librarian:
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
        
        # self._listeners = SetMap()
        # NOTE! This means that the listeners CANNOT be methods, as methods
        # will be DOA.
        self._listeners = WeakSetMap()
        
    def assemble(self, golix_core, librarian, bookie, rolodex):
        super().assemble(librarian, bookie)
        self._golcore = weakref.proxy(golix_core)
        self._rolodex = weakref.proxy(rolodex)
        
    def register(self, gao):
        ''' Registers a GAO with the postman, so that it will receive
        any updates from upstream/downstream remotes. By using the handy
        WeakSetMap and gao._weak_touch, we can ensure that python GCing
        the GAO will also result in removal of the listener.
        
        Theoretically, we should only ever have one registered GAO for
        a given ghid at the same time, so maybe in the future there's
        some optimization to be had there.
        '''
        self._listeners.add(gao.ghid, gao._weak_touch)
            
    async def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        if subscription == self._golcore.whoami:
            self._rolodex.notification_handler(subscription, notification)
        else:
            callbacks = self._listeners.get_any(subscription)
            logger.debug(
                'MrPostman starting delivery for ' + str(len(callbacks)) +
                ' updates on sub ' + str(subscription) + ' with notif ' +
                str(notification)
            )
            for callback in callbacks:
                callback(subscription, notification)
        
        
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
