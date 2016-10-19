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

RemoteNak status code conventions:
-----
0x0001: Does not appear to be a Golix object.
0x0002: Failed to verify.
0x0003: Unknown or invalid author or recipient.
0x0004: Unbound GEOC; immediately garbage collected
0x0005: Existing debinding for address; (de)binding rejected.
0x0006: Invalid or unknown target.
0x0007: Inconsistent author.
0x0008: Object does not exist at persistence provider.
0x0009: Attempt to upload illegal frame for dynamic binding. Indicates
        uploading a new dynamic binding without the root binding, or that
        the uploaded frame does not contain any existing frames in its
        history.
'''

# Global dependencies
import abc
import collections
import weakref
import concurrent.futures
import threading
import traceback
import asyncio
import loopa

from golix import Ghid
from golix import SecurityError

from golix.utils import generate_ghidlist_parser

# Local dependencies
from .persistence import PersistenceCore
from .persistence import Doorman
from .persistence import PostOffice
from .persistence import Undertaker
from .persistence import Lawyer
from .persistence import Enforcer
from .persistence import Bookie
from .persistence import DiskLibrarian
from .persistence import MemoryLibrarian
from .persistence import Enlitener

from .persistence import _GidcLite
from .persistence import _GeocLite
from .persistence import _GobsLite
from .persistence import _GobdLite
from .persistence import _GdxxLite
from .persistence import _GarqLite

from .exceptions import RemoteNak
from .exceptions import MalformedGolixPrimitive
from .exceptions import VerificationFailure
from .exceptions import UnboundContainer
from .exceptions import InvalidIdentity
from .exceptions import DoesNotExist
from .exceptions import AlreadyDebound
from .exceptions import InvalidTarget
from .exceptions import InconsistentAuthor
from .exceptions import IllegalDynamicFrame
from .exceptions import IntegrityError
from .exceptions import UnavailableUpstream

from .utils import _JitSetDict
from .utils import call_coroutine_threadsafe
from .utils import _generate_threadnames

from .comms import RequestResponseProtocol
from .comms import request


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Rolodex',
]


# ###############################################
# Library
# ###############################################


ERROR_CODES = {
    b'\x00\x00': Exception,
    b'\x00\x01': MalformedGolixPrimitive,
    b'\x00\x02': VerificationFailure,
    b'\x00\x03': InvalidIdentity,
    b'\x00\x04': UnboundContainer,
    b'\x00\x05': AlreadyDebound,
    b'\x00\x06': InvalidTarget,
    b'\x00\x07': InconsistentAuthor,
    b'\x00\x08': DoesNotExist,
    b'\x00\x09': IllegalDynamicFrame,
    b'\xFF\xFF': RemoteNak,
}
        
        
class RemotePersistenceProtocol(metaclass=RequestResponseProtocol,
                                error_codes=ERROR_CODES):
    ''' Defines the protocol for remote persisters.
    '''
    
    def __init__(self, *args, **kwargs):
        ''' Add intentionally invalid initialization for the components
        that need to be assembled.
        '''
        self._percore = None
        self._bookie = None
        self._librarian = None
        self._postman = None
        
        super().__init__(*args, **kwargs)
        
    def assemble(self, persistence_core, bookie, librarian, postman):
        # Link to the remote core.
        self._percore = weakref.proxy(persistence_core)
        self._bookie = weakref.proxy(bookie)
        self._postman = weakref.proxy(postman)
        self._librarian = weakref.proxy(librarian)
    
    @request(b'!!')
    async def subscription_update(self, connection, subscription_ghid,
                                  notification_ghid):
        ''' Send a subscription update to the connection.
        '''
        # TODO: move this to the call in the postman. It won't work here.
        if notification_ghid not in self._silenced:
            return bytes(subscription_ghid) + bytes(notification_ghid)
        
    @subscription_update.request_handler
    async def subscription_update(self, connection, body):
        ''' Handles an incoming subscription update.
        '''
        subscribed_ghid = Ghid.from_bytes(body[0:65])
        notification_ghid = Ghid.from_bytes(body[65:130])
        
        # TODO: make this properly async.
        for callback in self._subscriptions[subscribed_ghid]:
            await callback(subscribed_ghid, notification_ghid)
        
        return b'\x01'
        
    @request(b'??')
    async def ping(self, connection):
        ''' Check a remote for availability.
        '''
        return b''
        
    @ping.request_handler
    async def ping(self, connection, body):
        # Really not much to see here.
        return '\x01'
        
    @ping.response_handler
    async def ping(self, connection, response, exc):
        # This will suppress any errors during pinging.
        if response == b'\x01':
            return True
        else:
            return False
            
    @request(b'PB')
    async def publish(self, connection, obj):
        ''' Publish a packed Golix object.
        '''
        return obj
        
    @publish.request_handler
    async def publish(self, connection, body):
        ''' Handle a published object.
        '''
        # TODO: something that isn't this.
        obj = await self._loop.run_in_executor(
            self._ingester,         # executor
            self._percore.ingest,   # func
            body,                   # packed
            False                   # remotable
        )
        
        # If we already have an exact copy of the object, do not go on a mail
        # run.
        if obj is not None:
            # Silence any notifications for the object ghid.
            # If it has a frame_ghid, silence that
            try:
                session.silence(obj.frame_ghid)
            # Otherwise, silence the object ghid
            except AttributeError:
                session.silence(obj.ghid)
                
            # Execute a parallel call to postman.do_mail_run()
            # Well, it was parallel, until I made it not parallel.
            try:
                await self._loop.run_in_executor(
                    executor = self._mailrunner,
                    func = self._postman.do_mail_run
                )
            except Exception:
                logger.error(
                    'Error during mail run: \n' +
                    ''.join(traceback.format_exc())
                )
            
        # We don't need to wait for the mail run to have a successful return
        return b'\x01'
        
    @publish.response_handler
    async def publish(self, connection, response, exc):
        ''' Handle responses to publish requests.
        '''
        if exc is not None:
            raise exc
        else:
            return True
        
    @request(b'GT')
    async def get(self, connection, ghid):
        ''' Request an object from the persistence provider.
        '''
        return bytes(ghid)
        
    @get.request_handler
    async def get(self, connection, body):
        ''' Handle get requests.
        '''
        ghid = Ghid.from_bytes(body)
        # TODO: librarian should be async calls.
        return self._librarian.retrieve(ghid)
        
    @request(b'+S')
    async def subscribe(self, connection, ghid):
        ''' Subscribe to updates from the remote.
        '''
        return bytes(ghid)
        
    @subscribe.request_handler
    async def subscribe(self, connection, body):
        ''' Handle subscription requests.
        '''
        ghid = Ghid.from_bytes(body)
        # TODO: uhhhh, postman... suboptimal...
        self._postman.subscribe(connection, ghid)
        return b'\x01'
        
    @subscribe.response_handler
    async def subscribe(self, connection, response, exc):
        ''' Handle responses to subscription requests.
        '''
        if exc is not None:
            raise exc
        
        return True
        
    @request(b'-S')
    async def unsubscribe(self, connection, ghid):
        ''' Unsubscribe from updates at a remote.
        '''
        return bytes(ghid)
        
    @unsubscribe.request_handler
    async def unsubscribe(self, connection, body):
        ''' Handle unsubscription requests.
        '''
        ghid = Ghid.from_bytes(body)
        had_subscription = self._postman.unsubscribe(connection, ghid)
        
        if had_subscription:
            return b'\x01'
        
        # Still successful, but idempotent
        else:
            return b'\x00'
        
    @unsubscribe.response_handler
    async def unsubscribe(self, connection, response, exc):
        ''' Handle responses to unsubscription requests.
        '''
        if exc is not None:
            raise exc
            
        # For now, ignore (success & UNsub) vs (success & NOsub)
        else:
            return True
        
    @request(b'?S')
    async def query_subscriptions(self, connection):
        ''' Request a list of all currently subscribed ghids.
        '''
        return b''
        
    @query_subscriptions.request_handler
    async def query_subscriptions(self, connection, body):
        ''' Handle subscription query requests.
        '''
        ghidlist = list(self._postman.list_subs(connection))
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
        
    @query_subscriptions.response_handler
    async def query_subscriptions(self, connection, response, exc):
        ''' Handle responses to subscription queries.
        '''
        if exc is not None:
            raise exc
        
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
        
    @request(b'?B')
    async def query_bindings(self, connection, ghid):
        ''' Get a list of all bindings for the ghid.
        '''
        return bytes(ghid)
        
    @query_bindings.request_handler
    async def query_bindings(self, connection, body):
        ''' Handle binding query requests.
        '''
        ghid = Ghid.from_bytes(body)
        # TODO: probably should asyncify this
        ghidlist = self._bookie.bind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
        
    @query_bindings.response_handler
    async def query_bindings(self, connection, response, exc):
        ''' Handle responses to binding queries.
        '''
        if exc is not None:
            raise exc
            
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
        
    @request(b'?D')
    async def query_debindings(self, connection, ghid):
        ''' Query which, if any, ghid(s) have debindings for <ghid>.
        '''
        return bytes(ghid)
        
    @query_debindings.request_handler
    async def query_debindings(self, connection, body):
        ''' Handles debinding query requests.
        '''
        ghid = Ghid.from_bytes(body)
        # TODO: should probably make this async
        ghidlist = self._bookie.debind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
        
    @query_debindings.response_handler
    async def query_debindings(self, connection, response, exc):
        ''' Handle responses to debinding queries.
        '''
        if exc is not None:
            raise exc
            
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
        
    @request(b'?E')
    async def query_existence(self, connection, ghid):
        ''' Checks if the passed <ghid> exists at the remote.
        '''
        return bytes(ghid)
        
    @query_existence.request_handler
    async def query_existence(self, connection, body):
        ''' Handle existence queries.
        '''
        ghid = Ghid.from_bytes(body)
        if ghid in self._librarian:
            return b'\x01'
        else:
            return b'\x00'
        
    @query_existence.response_handler
    async def query_existence(self, connection, response, exc):
        ''' Handle responses to existence queries.
        '''
        if exc is not None:
            raise exc
        elif response == b'\x00':
            return False
        else:
            return True
        
    @request(b'XX')
    async def disconnect(self, connection):
        ''' Terminates all subscriptions and requests.
        '''
        return b''
        
    @disconnect.request_handler
    async def disconnect(self, connection, body):
        ''' Handle disconnect requests.
        '''
        self._postman.clear_subs(connection)
        return b'\x01'


class Salmonator(loopa.TaskLooper):
    ''' Responsible for disseminating Golix objects upstream and
    downstream. Handles all comms with them as well.
    '''
    
    def __init__(self, *args, **kwargs):
        ''' Yarp.
        '''
        self._opslock = threading.Lock()
        
        self._percore = None
        self._golcore = None
        self._postman = None
        self._librarian = None
        self._doorman = None
        
        self._pull_q = None
        self._push_q = None
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        
        self._upstream_remotes = set()
        self._downstream_remotes = set()
        
        # WVD lookup for <registered ghid>: GAO.
        self._registered = weakref.WeakValueDictionary()
        
        super().__init__(*args, **kwargs)
        
    def assemble(self, golix_core, persistence_core, doorman, postman,
                 librarian):
        self._golcore = weakref.proxy(golix_core)
        self._percore = weakref.proxy(persistence_core)
        self._postman = weakref.proxy(postman)
        self._librarian = weakref.proxy(librarian)
    
    def add_upstream_remote(self, persister):
        ''' Adds an upstream persister.
        
        PersistenceCore will attempt to have a constantly consistent
        state with upstream persisters. That means that any local
        resources will either be subscribed to upstream, or checked for
        updates before ingestion by the Hypergolix service.
        
        HOWEVER, the Salmonator will make no attempt to synchronize
        state **between** upstream remotes.
        '''
        # Before we do anything, we should try pushing up our identity.
        try:
            if not persister.query(self._golcore.whoami):
                persister.publish(self._golcore._identity.second_party.packed)
            
            with self._opslock:
                self._upstream_remotes.add(persister)
                # Subscribe to our identity, assuming we actually have a golix core
                try:
                    persister.subscribe(
                        self._golcore.whoami,
                        self._remote_callback
                    )
                except AttributeError:
                    logger.error(
                        'Missing golcore while attempting to add upstream ' +
                        'remote!'
                    )
                    
                # Subscribe to every active GAO's ghid
                for registrant in self._registered:
                    persister.subscribe(registrant, self._remote_callback)
        
        except Exception:
            logger.error(
                'Failed to add upstream remote w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
        
    def remove_upstream_remote(self, persister):
        ''' Inverse of above.
        '''
        with self._opslock:
            self._upstream_remotes.remove(persister)
            # Remove all subscriptions
            persister.disconnect()
        
    def add_downstream_remote(self, persister):
        ''' Adds a downstream persister.
        
        PersistenceCore will not attempt to keep a consistent state with
        downstream persisters. Instead, it will simply push updates to
        local objects downstream. It will not, however, look to them for
        updates.
        
        Therefore, to create synchronization **between** multiple
        upstream remotes, also add them as downstream remotes.
        '''
        raise NotImplementedError()
        self._downstream_remotes.add(persister)
        
    def remove_downstream_remote(self, persister):
        ''' Inverse of above.
        '''
        raise NotImplementedError()
        self._downstream_remotes.remove(persister)
        
    def _remote_callback(self, subscription, notification):
        ''' Callback to use when subscribing to things at remotes.
        '''
        logger.debug('Hitting remote callback.')
        # TODO: make this non-blocking
        self.schedule_pull(notification)
        
    async def loop_init(self, *args, **kwargs):
        ''' On top of the usual stuff, set up our queues.
        '''
        await super().loop_init(*args, **kwargs)
        self._pull_q = asyncio.Queue(loop=self._loop)
        self._push_q = asyncio.Queue(loop=self._loop)
        
    async def loop_stop(self, *args, **kwargs):
        ''' On top of the usual stuff, clear our queues.
        '''
        # Would be good to do, but not currently working
        # for remote in self._persisters:
        #     await self._stop_persister(remote)
        
        await super().loop_stop(*args, **kwargs)
        self._pull_q = None
        self._push_q = None
        
    async def loop_run(self, *args, **kwargs):
        ''' Launch two tasks: one manages the push queue, and the other
        the pull queue.
        '''
        to_pull = asyncio.ensure_future(self._pull_q.get())
        to_push = asyncio.ensure_future(self._push_q.get())
        
        finished, pending = await asyncio.wait(
            fs = [to_pull, to_push],
            return_when = asyncio.FIRST_COMPLETED
        )
        
        # One or the other has to be done. Here, it's pull.
        if to_pull in finished:
            to_push.cancel()
            await self.pull(to_pull.result())
        
        # Here, it's push.
        else:
            to_pull.cancel()
            await self.push(to_push.result())
        
    def schedule_pull(self, ghid):
        ''' Tells the salmonator to pull the ghid at the earliest
        opportunity and returns. Call from a different thread.
        '''
        call_coroutine_threadsafe(
            coro = self._pull_q.put(ghid),
            loop = self._loop
        )
        
    def schedule_push(self, ghid):
        ''' Grabs the ghid from librarian and sends it to all applicable
        remotes.
        '''
        call_coroutine_threadsafe(
            coro = self._push_q.put(ghid),
            loop = self._loop
        )
        
    async def push(self, ghid):
        ''' Push a single ghid to all remotes.
        '''
        try:
            data = self._librarian.retrieve(ghid)
        
        except Exception as exc:
            logger.error(
                'Error while pushing an object upstream:\n' +
                ''.join(traceback.format_exc()) +
                repr(exc)
            )
            return
        
        tasks_available = []
        for remote in self._upstream_remotes:
            this_push = asyncio.ensure_future(
                self._loop.run_in_executor(
                    self._executor,
                    remote.publish,
                    data
                )
            )
            tasks_available.append(this_push)
            
        if tasks_available:
            finished, pending = await asyncio.wait(
                fs = tasks_available,
                return_when = asyncio.ALL_COMPLETED
            )
            
            # Pending will be empty (or asyncio bugged up)
            for task in finished:
                exc = task.exception()
                if exc is not None:
                    logger.error(
                        'Error while pushing to remote:\n' +
                        ''.join(traceback.format_tb(exc.__traceback__)) +
                        repr(exc)
                    )
                
    async def pull(self, ghid):
        ''' Gets a ghid from upstream. Returns on the first result. Note
        that this is not meant to be called on a dynamic address, as a
        subs update from a slower remote would always be overridden by
        the faster one.
        '''
        finished = None
        tasks_available = set()
        for remote in self._upstream_remotes:
            this_pull = asyncio.ensure_future(
                self._loop.run_in_executor(
                    self._executor,
                    self._attempt_pull_single,
                    ghid,
                    remote
                )
            )
            tasks_available.add(this_pull)
            
        # Wait until the first successful task completion
        while tasks_available and not finished:
            finished, pending = await asyncio.wait(
                fs = tasks_available,
                return_when = asyncio.FIRST_COMPLETED
            )
        
            finished = finished.pop()
            tasks_available.discard(finished)
            exc = finished.exception()
            
            # If there's been an exception, continue waiting for the rest.
            if exc is not None:
                logger.error(
                    'Error while pulling from remote:\n' +
                    ''.join(traceback.format_tb(exc.__traceback__)) +
                    repr(exc)
                )
                finished = None
            
            # Completed successfully, but it could be a 404 (or other error),
            # which would present as result() = False. So, assign the result to
            # finished and let the while loop handle the rest.
            else:
                finished = finished.result()
                
        # No dice. Either finished is None (no remotes), None (no successful
        # pulls), or False (exactly one remote had the object, but it was
        # unloadable). Raise.
        if not finished:
            raise UnavailableUpstream(
                'Object was unavailable or unacceptable at all '
                'currently-registered remotes.'
            )
                
        logger.debug(
            'Successfully pulled ' + str(ghid) + ' from upstream remote. ' +
            'Handling...'
        )
        # Now we have one complete task (finished) and potentially still some
        # pending. So, cancel all remaining...
        for task in pending:
            task.cancel()
        # And handle the result.
        await self._handle_successful_pull(finished)
            
    async def _handle_successful_pull(self, maybe_obj):
        ''' Dispatches the object in the successful pull.
        
        maybe_obj can be either True (if the object already existed
        locally), or the object itself (if the object was new).
        '''
        # If we got a gobd, make sure its target is in the librarian
        if isinstance(maybe_obj, _GobdLite):
            if maybe_obj.target not in self._librarian:
                # Catch unavailableupstream and log a warning.
                # TODO: add logic to retry a few times and then discard
                try:
                    await self.pull(maybe_obj.target)
                    
                except UnavailableUpstream:
                    logger.warning(
                        'Received a subscription notification for ' +
                        str(maybe_obj.ghid) + ', but the sub\'s target was '
                        'missing both locally and upstream.'
                    )
        
        # We know the pull was successful, but that could indicate that the
        # requested ghid was already known locally. In that case, there's no
        # harm in doing an extra mail run, so make it happen regardless.
        await self._loop.run_in_executor(
            self._executor,
            self._postman.do_mail_run
        )
        
        logger.debug('Successful pull handled.')
        
    def attempt_pull(self, ghid, quiet=False):
        ''' Grabs the ghid from remotes, if available, and puts it into
        the ingestion pipeline.
        '''
        # TODO: check locally, run _inspect, check if mutable before blindly
        # pulling.
        try:
            call_coroutine_threadsafe(
                coro = self.pull(ghid),
                loop = self._loop
            )
            
        except UnavailableUpstream:
            if not quiet:
                raise
            # Suppress errors if we were called quietly.
            else:
                logger.info(
                    'Object was unavailable or unacceptable upstream, but '
                    'pull was called quietly: ' + str(ghid)
                )
        
    def _attempt_pull_single(self, ghid, remote):
        ''' Attempt to fetch a single object from a single remote. If
        successful, put it into the ingestion pipeline.
        '''
        # This may error, but any errors here will be caught by the parent.
        data = remote.get(ghid)
        
        # This may or may not be an update we already have.
        try:
            # Call as remotable=False to avoid infinite loops.
            obj = self._percore.ingest(data, remotable=False)
        
        # Couldn't load. Return False.
        except Exception as exc:
            logger.warning(
                'Error while pulling from upstream: \n' +
                ''.join(traceback.format_exc()) +
                repr(exc)
            )
            return False
            
        # As soon as we have it, return True so parent can stop checking
        # other remotes.
        else:
            # Note that ingest can either return None, if we already have
            # the object, or the object itself, if it's new.
            if obj is None:
                return True
            else:
                return obj
        
    def _inspect(self, ghid):
        ''' Checks librarian for an existing ghid. If it has it, checks
        the object's integrity by re-parsing it. If it is dynamic, also
        queries upstream remotes for newer versions.
        
        returns None if no local copy exists
        returns True if local copy exists and is valid
        raises IntegrityError if local copy exists, but is corrupted.
        ~~(returns False if dynamic and local copy is out of date)~~
            Note: this is unimplemented currently, blocking on several
            upstream changes.
        '''
        # Load the object locally
        try:
            obj = self._librarian.summarize(ghid)
        
        # Librarian has no copy.
        except KeyError:
            return None
            
        # The only object that can mutate is a Gobd
        # This is blocking on updates to the remote persistence spec, which is
        # in turn blocking on changes to the Golix spec.
        # if isinstance(obj, _GobdLite):
        #     self._check_for_updates(obj)
        
        self._verify_existing(obj)
        return True
            
    def _check_for_updates(self, obj):
        ''' Checks with all upstream remotes for new versions of a
        dynamic object.
        '''
        # Oh wait, we can't actually do this, because the remote persistence
        # protocol doesn't support querying what the current binding frame is
        # without just loading the whole binding.
        # When the Golix spec changes to semi-stateless dynamic bindings, using
        # a counter for validating monotonicity instead of a hash chain, then
        # the remote persistence spec should be expanded to include a query_ctr
        # command for ultra-lightweight checks. For now, we're stuck with ~1kB
        # dynamic bindings.
        raise NotImplementedError()
            
    def _verify_existing(self, obj):
        ''' Re-loads an object to make sure it's still good.
        Obj should be a lightweight hypergolix representation, not the
        packed Golix object, unpacked Golix object, nor the GAO.
        '''
        packed = self._librarian.retrieve(obj.ghid)
        
        for primitive, loader in ((_GidcLite, self._doorman.load_gidc),
                                  (_GeocLite, self._doorman.load_geoc),
                                  (_GobsLite, self._doorman.load_gobs),
                                  (_GobdLite, self._doorman.load_gobd),
                                  (_GdxxLite, self._doorman.load_gdxx),
                                  (_GarqLite, self._doorman.load_garq)):
            # Attempt this loader
            if isinstance(obj, primitive):
                try:
                    loader(packed)
                except (MalformedGolixPrimitive, SecurityError) as exc:
                    logger.error('Integrity of local object appears '
                                 'compromised.')
                    raise IntegrityError('Local copy of object appears to be '
                                         'corrupt or compromised.') from exc
                else:
                    break
                    
        # If we didn't find a loader, typeerror.
        else:
            raise TypeError('Invalid object type while verifying object.')
            
    def register(self, gao, skip_refresh=False):
        ''' Tells the Salmonator to listen upstream for any updates
        while the gao is retained in memory.
        '''
        with self._opslock:
            self._registered[gao.ghid] = gao
        
            if gao.dynamic:
                for remote in self._upstream_remotes:
                    try:
                        remote.subscribe(gao.ghid, self._remote_callback)
                    except Exception:
                        logger.warning(
                            'Exception while subscribing to upstream updates '
                            'for GAO at ' + str(gao.ghid) + '\n' +
                            ''.join(traceback.format_exc())
                        )
                    else:
                        logger.debug(
                            'Successfully subscribed to upstream updates for '
                            'GAO at ' + str(gao.ghid)
                        )
                        
            # Add deregister as a finalizer, but don't call it atexit.
            finalizer = weakref.finalize(gao, self.deregister, gao.ghid)
            finalizer.atexit = False
            
        # This should also catch any upstream deletes.
        if not skip_refresh:
            self.attempt_pull(gao.ghid, quiet=True)
                
    def deregister(self, ghid):
        ''' Tells the salmonator to stop listening for upstream
        object updates. Primarily intended for use as a finalizer
        for GAO objects.
        '''
        with self._opslock:
            try:
                # We shouldn't really actually need to do this, but let's
                # explicitly do it anyways, to ensure there is no race
                # condition between object removal and someone else sending an
                # update. The weak-value-ness of the _registered lookup can be
                # used as a fallback.
                del self._registered[ghid]
            except KeyError:
                logger.debug(
                    str(ghid) + ' missing in deregistration w/ traceback:\n' +
                    ''.join(traceback.format_exc())
                )
            
            for remote in self._upstream_remotes:
                try:
                    remote.unsubscribe(ghid, self._remote_callback)
                except Exception:
                    logger.warning(
                        'Exception while unsubscribing from upstream updates '
                        'during GAO cleanup for ' + str(ghid) + '\n' +
                        ''.join(traceback.format_exc())
                    )
                
                
class SalmonatorNoop:
    ''' Currently used in remote persistence servers to hush everything
    upstream/downstream while still making use of standard librarians.
    
    And by "used", I mean "unused, but is intended to be added in at
    some point, because I forgot I was just using a standard Salmonator
    because the overhead is unimportant right now".
    '''
    
    def __init__(self, *args, **kwargs):
        pass
    
    def assemble(*args, **kwargs):
        pass
        
    def add_upstream_remote(*args, **kwargs):
        pass
        
    def remove_upstream_remote(*args, **kwargs):
        pass
        
    def add_downstream_remote(*args, **kwargs):
        pass
        
    def remove_downstream_remote(*args, **kwargs):
        pass
        
    def schedule_push(*args, **kwargs):
        pass
        
    def attempt_pull(*args, **kwargs):
        pass
        
    def register(*args, **kwargs):
        pass
        
    def deregister(*args, **kwargs):
        pass


class _PersisterBase(metaclass=abc.ABCMeta):
    ''' Base class for persistence providers.
    '''
    
    @abc.abstractmethod
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        Note that this is going to (unfortunately) result in packing +
        unpacking the object twice for ex. a MemoryPersister. At some
        point, that should be fixed -- maybe through ex. publish_unsafe?
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def subscribe(self, ghid, callback):
        ''' Request that the persistence provider update the client on
        any changes to the object addressed by ghid. Must target either:
        
        1. Dynamic ghid
        2. Author identity ghid
        
        Upon successful subscription, the persistence provider will
        publish to client either of the above:
        
        1. New frames to a dynamic binding
        2. Asymmetric requests with the indicated GHID as a recipient
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to
        the passed ghid at the persistence provider. Removes only the
        passed callback.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def list_bindings(self, ghid):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def list_debindings(self, ghid):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise RemoteNak
        '''
        pass
        
    @abc.abstractmethod
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def disconnect(self):
        ''' Terminates all subscriptions and requests. Not required for
        a disconnect, but highly recommended, and prevents an window of
        attack for address spoofers. Note that such an attack would only
        leak metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass


class MemoryPersister(PersistenceCore):
    ''' Basic in-memory persister.
    
    This is a deprecated legacy thing we're keeping around so that we
    don't need to completely re-write our already inadequate test suite.
    '''
    
    def __init__(self):
        super().__init__()
        self.assemble(Doorman(), Enforcer(), Lawyer(), Bookie(),
                      MemoryLibrarian(), PostOffice(), Undertaker(),
                      SalmonatorNoop())
        
        self.subscribe = self.postman.subscribe
        self.unsubscribe = self.postman.unsubscribe
        # self.silence_notification = self.postman.silence_notification
        # self.publish = self.core.ingest
        self.list_bindings = self.bookie.bind_status
        self.list_debindings = self.bookie.debind_status
        
    def assemble(self, doorman, enforcer, lawyer, bookie, librarian, postman,
                 undertaker, salmonator):
        self.doorman = doorman
        self.enlitener = Enlitener
        self.enforcer = enforcer
        self.lawyer = lawyer
        self.bookie = bookie
        self.postman = postman
        self.undertaker = undertaker
        self.librarian = librarian
        self.salmonator = salmonator
        
        self.doorman.assemble(librarian)
        self.postman.assemble(librarian, bookie)
        self.undertaker.assemble(librarian, bookie, postman)
        self.lawyer.assemble(librarian)
        self.enforcer.assemble(librarian)
        self.bookie.assemble(librarian, lawyer, undertaker)
        self.librarian.assemble(self, self.salmonator)
        self.salmonator.assemble(self, self, self.doorman, self.postman,
                                 self.librarian)
        
    def publish(self, *args, **kwargs):
        # This is a temporary fix to force memorypersisters to notify during
        # publishing. Ideally, this would happen immediately after returning.
        self.ingest(*args, **kwargs)
        self.postman.do_mail_run()
        
    def ping(self):
        ''' Queries the persistence provider for availability.
        '''
        return True
        
    def get(self, ghid):
        ''' Returns a packed Golix object.
        '''
        try:
            return self.librarian.retrieve(ghid)
        except KeyError as exc:
            raise DoesNotExist(
                '0x0008: Not found at persister: ' + str(ghid)
            ) from exc
        
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected
        client.
        '''
        # TODO: figure out what to do instead of this
        return tuple(self.postman._listeners)
            
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise RemoteNak
        '''
        if ghid in self.librarian:
            return True
        else:
            return False
        
    def disconnect(self):
        ''' Terminates all subscriptions. Not required for a disconnect,
        but highly recommended, and prevents an window of attack for
        address spoofers. Note that such an attack would only leak
        metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        # TODO: figure out something different to do here.
        self.postman._listeners = {}
        return True
        
        
class DiskCachePersister(MemoryPersister):
    ''' Persister that caches to disk.
    Replicate MemoryPersister, just replace Librarian.
    
    Same note re: deprecated.
    '''
    
    def __init__(self, cache_dir):
        super().__init__()
        self.assemble(Doorman(), Enforcer(), Lawyer(), Bookie(),
                      DiskLibrarian(cache_dir=cache_dir), PostOffice(),
                      Undertaker(), SalmonatorNoop())
        
        self.subscribe = self.postman.subscribe
        self.unsubscribe = self.postman.unsubscribe
        # self.silence_notification = self.postman.silence_notification
        # self.publish = self.core.ingest
        self.list_bindings = self.bookie.bind_status
        self.list_debindings = self.bookie.debind_status


class _PersisterBridgeSession:
    def __init__(self, transport, *args, **kwargs):
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(transport, weakref.ProxyTypes):
            self._transport = transport
        else:
            self._transport = weakref.proxy(transport)
            
        self._subscriptions = {}
        
        # This is in charge of muting notifications for things we just sent.
        # Maxlen may need some tuning, and this is a bit of a hack.
        self._silenced = collections.deque(maxlen=5)
    
        # NOTE: these must be done as a closure upon functions, instead of
        # normal bound methods, because otherwise the weakrefs used in the
        # post office subscription weakset will be DOA
                
        async def send_subs_update_ax(subscribed_ghid, notification_ghid):
            ''' Deliver any subscription updates.
            
            Also, temporary workaround for not re-delivering updates for
            objects we just sent up.
            '''
            if notification_ghid not in self._silenced:
                try:
                    await self._transport.send(
                        session = self,
                        msg = (bytes(subscribed_ghid) +
                               bytes(notification_ghid)),
                        request_code = self._transport.REQUEST_CODES[
                            'send_subs_update'
                        ],
                        # Note: for now, just don't worry about failures.
                        # await_reply = False
                    )
                    
                # KeyErrors have been happening when connections/sessions
                # disconnect without calling close (or something similar to
                # that, I never tracked down the original source of the
                # problem). This is a quick, hacky fix to remove the bad
                # subscription when we first get an update for an object.
                # This is far, far from ideal, but hey, what can you do
                # without any time to do it in?
                except KeyError as exc:
                    # Well, this is an awkward way to handle an unsub, but it
                    # gets the job done until we fix the way all of this plays
                    # together.
                    await self._transport.unsubscribe_wrapper(
                        session = self,
                        request_body = bytes(subscribed_ghid)
                    )
                    logger.warning(
                        'Application client subscription persisted longer ' +
                        'than the connection itself w/ traceback:\n' +
                        ''.join(traceback.format_exc())
                    )
                
                except Exception:
                    logger.error(
                        'Application client failed to receive sub update at ' +
                        str(subscribed_ghid) + ' for notification ' +
                        str(notification_ghid) + ' w/ traceback: \n' +
                        ''.join(traceback.format_exc())
                    )
        
        def send_subs_update(subscribed_ghid, notification_ghid):
            ''' Send the connection its subscription update.
            Note that this is going to be called from within an event loop,
            but not asynchronously (no await).
            
            TODO: make persisters async.
            '''
            return call_coroutine_threadsafe(
                coro = send_subs_update_ax(subscribed_ghid, notification_ghid),
                loop = self._transport._loop
            )
            
            # asyncio.run_coroutine_threadsafe(
            #     coro = self.send_subs_update_ax(
            #         subscribed_ghid,
            #         notification_ghid
            #     )
            #     loop = self._transport._loop
            # )
            
        self.send_subs_update = send_subs_update
        self.send_subs_update_ax = send_subs_update_ax
        
        super().__init__(*args, **kwargs)
        
    def silence(self, ghid):
        # Silence any notifications for the passed ghid.
        self._silenced.appendleft(ghid)


class PersisterBridgeServer:
    ''' Serialization mixins for persister bridges.
    '''
    REQUEST_CODES = {
        # Receive an update for an existing object.
        'send_subs_update': b'!!',
    }
    
    def __init__(self, *args, **kwargs):
        self._percore = None
        self._bookie = None
        self._librarian = None
        self._postman = None
        
        req_handlers = {
            # ping
            b'??': self.ping_wrapper,
            # publish
            b'PB': self.publish_wrapper,
            # get
            b'GT': self.get_wrapper,
            # subscribe
            b'+S': self.subscribe_wrapper,
            # unsubscribe
            b'xS': self.unsubscribe_wrapper,
            # list subs
            b'LS': self.list_subs_wrapper,
            # list binds
            b'LB': self.list_bindings_wrapper,
            # list debindings
            b'LD': self.list_debindings_wrapper,
            # query (existence)
            b'QE': self.query_wrapper,
            # disconnect
            b'XX': self.disconnect_wrapper,
        }
        
        # Create an executor for mail runs.
        self._mailrunner = concurrent.futures.ThreadPoolExecutor()
        # Create an executor for ingesting.
        self._ingester = concurrent.futures.ThreadPoolExecutor()
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            error_lookup = ERROR_CODES,
            *args, **kwargs
        )
        
    def assemble(self, persistence_core, bookie, librarian, postman):
        # Link to the remote core.
        self._percore = weakref.proxy(persistence_core)
        self._bookie = weakref.proxy(bookie)
        self._postman = weakref.proxy(postman)
        self._librarian = weakref.proxy(librarian)
            
    def session_factory(self):
        ''' Added for easier subclassing. Returns the session class.
        '''
        logger.debug('Session created.')
        return _PersisterBridgeSession(
            transport = self,
        )
            
    async def ping_wrapper(self, session, request_body):
        ''' Deserializes a ping request; forwards it to the persister.
        '''
        # Yep, we're available.
        return b'\x01'
            
    async def publish_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        obj = await self._loop.run_in_executor(
            self._ingester,         # executor
            self._percore.ingest,   # func
            request_body,           # packed
            False                   # remotable
        )
        
        # If we already have an exact copy of the object, do not go on a mail
        # run.
        if obj is not None:
            # Silence any notifications for the object ghid.
            # If it has a frame_ghid, silence that
            try:
                session.silence(obj.frame_ghid)
            # Otherwise, silence the object ghid
            except AttributeError:
                session.silence(obj.ghid)
            # Execute a parallel call to postman.do_mail_run()
            asyncio.ensure_future(self._handle_mail_run())
            
        # We don't need to wait for the mail run to have a successful return
        return b'\x01'
        
    async def _handle_mail_run(self):
        ''' Wraps running a mail run with error handling. This needs to
        be totally autonomous, or asyncio will get angry.
        '''
        try:
            await self._loop.run_in_executor(
                executor = self._mailrunner,
                func = self._postman.do_mail_run
            )
        except Exception:
            logger.error(
                'Error during mail run: \n' + ''.join(traceback.format_exc())
            )
            
    async def get_wrapper(self, session, request_body):
        ''' Deserializes a get request; forwards it to the persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        return self._librarian.retrieve(ghid)
            
    async def subscribe_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        
        def updater(subscribed_ghid, notification_ghid,
                    call=session.send_subs_update):
            call(subscribed_ghid, notification_ghid)
        
        self._postman.subscribe(ghid, updater)
        session._subscriptions[ghid] = updater
        return b'\x01'
            
    async def unsubscribe_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        callback = session._subscriptions[ghid]
        unsubbed = self._postman.unsubscribe(ghid, callback)
        del session._subscriptions[ghid]
        if unsubbed:
            return b'\x01'
        else:
            return b'\x00'
            
    async def list_subs_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghidlist = list(session._subscriptions)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def list_bindings_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        ghidlist = self._bookie.bind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def list_debindings_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        ghidlist = self._bookie.debind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def query_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        if ghid in self._librarian:
            return b'\x01'
        else:
            return b'\x00'
            
    async def disconnect_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        for sub_ghid, sub_callback in session._subscriptions.items():
            self._postman.unsubscribe(sub_ghid, sub_callback)
        session._subscriptions.clear()
        return b'\x01'
        
        
class PersisterBridgeClient:
    ''' Websockets request/response persister (the client half).
    '''
            
    REQUEST_CODES = {
        # ping
        'ping': b'??',
        # publish
        'publish': b'PB',
        # get
        'get': b'GT',
        # subscribe
        'subscribe': b'+S',
        # unsubscribe
        'unsubscribe': b'xS',
        # list subs
        'list_subs': b'LS',
        # list binds
        'list_bindings': b'LB',
        # list debindings
        'list_debindings': b'LD',
        # query (existence)
        'query': b'QE',
        # disconnect
        'disconnect': b'XX',
    }
    
    def __init__(self, *args, **kwargs):
        # Note that these are only for unsolicited contact from the server.
        req_handlers = {
            # Receive/dispatch a new object.
            b'!!': self.deliver_update_wrapper,
        }
        
        self._subscriptions = _JitSetDict()
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            error_lookup = ERROR_CODES,
            *args, **kwargs
        )
        
    async def deliver_update_wrapper(self, session, response_body):
        ''' Handles update pings.
        '''
        # # Shit, I have a race condition somewhere.
        # time.sleep(.01)
        subscribed_ghid = Ghid.from_bytes(response_body[0:65])
        notification_ghid = Ghid.from_bytes(response_body[65:130])
        
        # for callback in self._subscriptions[subscribed_ghid]:
        #     callback(notification_ghid)
                
        # Well this is a huge hack. But something about the event loop
        # itself is fucking up control flow and causing the world to hang
        # here. May want to create a dedicated thread just for pushing
        # updates? Like autoresponder, but autoupdater?
        # TODO: fix this gross mess
            
        def run_callbacks(subscribed_ghid, notification_ghid):
            for callback in self._subscriptions[subscribed_ghid]:
                callback(subscribed_ghid, notification_ghid)
        
        worker = threading.Thread(
            target = run_callbacks,
            daemon = True,
            args = (subscribed_ghid, notification_ghid),
            name = _generate_threadnames('remoupd')[0],
        )
        worker.start()
        
        return b'\x01'
    
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['ping']
        )
        
        if response == b'\x01':
            return True
        else:
            return False
    
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        Note that this is going to (unfortunately) result in packing +
        unpacking the object twice for ex. a MemoryPersister. At some
        point, that should be fixed -- maybe through ex. publish_unsafe?
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = packed,
            request_code = self.REQUEST_CODES['publish']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError(
                'Unknown response code while publishing object:\n' +
                str(response)
            )
    
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['get']
        )
            
        return response
    
    def subscribe(self, ghid, callback):
        ''' Request that the persistence provider update the client on
        any changes to the object addressed by ghid. Must target either:
        
        1. Dynamic ghid
        2. Author identity ghid
        
        Upon successful subscription, the persistence provider will
        publish to client either of the above:
        
        1. New frames to a dynamic binding
        2. Asymmetric requests with the indicated GHID as a recipient
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        if ghid not in self._subscriptions:
            self.await_session_threadsafe()
            response = self.send_threadsafe(
                session = self.any_session,
                msg = bytes(ghid),
                request_code = self.REQUEST_CODES['subscribe']
            )
            
            if response != b'\x01':
                raise RuntimeError(
                    'Unknown response code while subscribing to ' + str(ghid)
                )
                
        self._subscriptions[ghid].add(callback)
        return True
    
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to
        the passed ghid at the persistence provider. Removes only the
        passed callback.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        if ghid not in self._subscriptions:
            raise ValueError('Not currently subscribed to ' + str(ghid))
            
        self._subscriptions[ghid].discard(callback)
        
        if len(self._subscriptions[ghid]) == 0:
            del self._subscriptions[ghid]
            
            self.await_session_threadsafe()
            response = self.send_threadsafe(
                session = self.any_session,
                msg = bytes(ghid),
                request_code = self.REQUEST_CODES['unsubscribe']
            )
        
            if response == b'\x01':
                # There was a subscription, and it was successfully removed.
                pass
            elif response == b'\x00':
                # This means there was no subscription to remove.
                pass
            else:
                raise RuntimeError(
                    'Unknown response code while unsubscribing from ' +
                    str(ghid) + '\nThe persister might still send '
                    'updates, but the callback has been removed.'
                )
                
        return True
    
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise RemoteNak
        '''
        # This would probably be a good time to reconcile states between the
        # persistence provider and our local set of subs!
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['list_subs']
        )
        
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
    
    def list_bindings(self, ghid):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['list_bindings']
        )
        
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
    
    def list_debindings(self, ghid):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['list_debindings']
        )
        
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
        
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['query']
        )
        
        if response == b'\x00':
            return False
        else:
            return True
    
    def disconnect(self):
        ''' Terminates all subscriptions and requests. Not required for
        a disconnect, but highly recommended, and prevents an window of
        attack for address spoofers. Note that such an attack would only
        leak metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['disconnect']
        )
        
        if response == b'\x01':
            self._subscriptions.clear()
            return True
        else:
            raise RuntimeError('Unknown status code during disconnection.')
