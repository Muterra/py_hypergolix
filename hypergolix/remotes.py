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
0x0000: Non-specific exception
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
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api

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

from .utils import call_coroutine_threadsafe

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
                                error_codes=ERROR_CODES,
                                default_version=b'\x00\x00'):
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
        
        # Note that obj will be None if the object already existed.
        # TODO: how do we handle silencing the update to this connection?
            
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


class Salmonator(loopa.TaskLooper, metaclass=API):
    ''' Responsible for disseminating Golix objects upstream and
    downstream. Handles all comms with them as well.
    '''
    
    @public_api
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
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
        
    def assemble(self, golix_core, persistence_core, doorman, postman,
                 librarian):
        self._golcore = weakref.proxy(golix_core)
        self._percore = weakref.proxy(persistence_core)
        self._postman = weakref.proxy(postman)
        self._librarian = weakref.proxy(librarian)
    
    @public_api
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
        
    @add_upstream_remote.fixture
    def add_upstream_remote(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
        
    @public_api
    def remove_upstream_remote(self, persister):
        ''' Inverse of above.
        '''
        with self._opslock:
            self._upstream_remotes.remove(persister)
            # Remove all subscriptions
            persister.disconnect()
        
    @remove_upstream_remote.fixture
    def remove_upstream_remote(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
        
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
        
        # Avoid race conditions by immediately cancelling the unfinished task.
        # Note that it's not guaranteed that we'll only get a single task.
        for task in pending:
            task.cancel()
        
        if to_pull in finished:
            await self.pull(to_pull.result())
        
        if to_push in finished:
            await self.push(to_push.result())
        
    @public_api
    def schedule_pull(self, ghid):
        ''' Tells the salmonator to pull the ghid at the earliest
        opportunity and returns. Call from a different thread.
        '''
        call_coroutine_threadsafe(
            coro = self._pull_q.put(ghid),
            loop = self._loop
        )
        
    @schedule_pull.fixture
    def schedule_pull(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
    
    @public_api
    def schedule_push(self, ghid):
        ''' Grabs the ghid from librarian and sends it to all applicable
        remotes.
        '''
        call_coroutine_threadsafe(
            coro = self._push_q.put(ghid),
            loop = self._loop
        )
        
    @schedule_push.fixture
    def schedule_push(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
        
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
        pull_complete = None
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
        while tasks_available and not pull_complete:
            finished, pending = await asyncio.wait(
                fs = tasks_available,
                return_when = asyncio.FIRST_COMPLETED
            )
        
            # Despite FIRST_COMPLETED, asyncio may return more than one task
            for task in finished:
                # The task finished, so discard it from the available and reap
                # any exception
                tasks_available.discard(task)
                exc = finished.exception()
                
                # If there's been an exception, continue waiting for the rest.
                if exc is not None:
                    logger.error(
                        'Error while pulling from remote:\n' +
                        ''.join(traceback.format_tb(exc.__traceback__)) +
                        repr(exc)
                    )
                    pull_complete = None
                
                # Completed successfully, but it could be a 404 (or other error),
                # which would present as result() = False.
                # Instead of letting the while loop handle this, since more
                # than one task can complete simultaneously, make sure we don't
                # already have a finalized result before blindly assigning the
                # task's result.
                elif not pull_complete:
                    pull_complete = task.result()
                    
                # Multiple tasks completed at once. An earlier one was
                # successful. We need to grab the result to suppress asyncio
                # complaints.
                else:
                    task.result()
                
        # No dice. Either finished is None (no remotes), None (no successful
        # pulls), or False (exactly one remote had the object, but it was
        # unloadable). Raise.
        if not pull_complete:
            raise UnavailableUpstream(
                'Object was unavailable or unacceptable at all '
                'currently-registered remotes.'
            )
            
        # Log success.
        else:
            logger.debug(
                'Successful remote pull for {!s}. Handling...'.format(ghid)
            )
        
        # We may still have some pending tasks. Cancel them. Note that we have
        # not yielded control to the event loop, so there is no race.
        for task in pending:
            task.cancel()
            
        # Now handle the result.
        await self._handle_successful_pull(pull_complete)
            
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
        
    @public_api
    async def attempt_pull(self, ghid, quiet=False):
        ''' Grabs the ghid from remotes, if available, and puts it into
        the ingestion pipeline.
        '''
        # TODO: check locally, run _inspect, check if mutable before blindly
        # pulling.
        try:
            await self.pull(ghid)
            
        except UnavailableUpstream:
            if not quiet:
                raise
            # Suppress errors if we were called quietly.
            else:
                logger.info(
                    'Object was unavailable or unacceptable upstream, but '
                    'pull was called quietly: ' + str(ghid)
                )
        
    @attempt_pull.fixture
    async def attempt_pull(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
        
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
    
    @public_api
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
    
    @register.fixture
    def register(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
        
    @public_api
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
                
    @deregister.fixture
    def deregister(self, *args, **kwargs):
        ''' Do nothing.
        '''
        pass
        
                
class SalmonatorNoop:
    ''' Currently used in remote persistence servers to hush everything
    upstream/downstream while still making use of standard librarians.
    
    And by "used", I mean "unused, but is intended to be added in at
    some point, because I forgot I was just using a standard Salmonator
    because the overhead is unimportant right now".
    
    This is deprecated and replaced by Salmonator.__fixture__().
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
