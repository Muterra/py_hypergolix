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
import logging
import weakref
import collections
import traceback
import asyncio
import loopa

from loopa.utils import make_background_future

from golix import Ghid
from golix import SecurityError

from golix.utils import generate_ghidlist_parser

# Local dependencies
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api
from .hypothetical import fixture_noop
from .hypothetical import fixture_return

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

from .utils import weak_property
from .utils import readonly_property
from .utils import ListMap

from .comms import RequestResponseAPI
from .comms import request
from .comms import ConnectionManager


# ###############################################
# Logging boilerplate
# ###############################################


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


class RemotePersistenceProtocol(metaclass=RequestResponseAPI,
                                error_codes=ERROR_CODES,
                                default_version=b'\x00\x00'):
    ''' Defines the protocol for remote persisters.
    '''
    _percore = weak_property('__percore')
    _librarian = weak_property('__librarian')
    _postman = weak_property('__postman')
    
    @fixture_api
    def __init__(self, percore=None, librarian=None, *args, **kwargs):
        super(RemotePersistenceProtocol.__fixture__, self).__init__(
            *args,
            **kwargs
        )
        if percore is not None:
            self._percore = percore
        if librarian is not None:
            self._librarian = librarian
        
    def assemble(self, percore, librarian, postman):
        # Link to the remote core.
        self._percore = percore
        self._postman = postman
        self._librarian = librarian
        
    @request(b'??')
    async def ping(self, connection):
        ''' Check a remote for availability.
        '''
        return b''
        
    @ping.request_handler
    async def ping(self, connection, body):
        # Really not much to see here.
        return b'\x01'
        
    @ping.response_handler
    async def ping(self, connection, response, exc):
        # This will suppress any errors during pinging.
        if response == b'\x01':
            return True
        else:
            return False
    
    @public_api
    @request(b'PB')
    async def publish(self, connection, packed):
        ''' Publish a packed Golix object.
        '''
        return packed
        
    @publish.fixture
    async def publish(self, connection, packed):
        ''' Just slap the thing into librarian with no checking for
        fixtures.
        '''
        obj = await self._percore.attempt_load(packed)
        await self._librarian.store(obj, packed)
        
    @publish.request_handler
    async def publish(self, connection, body):
        ''' Handle a published object.
        '''
        obj = await self._percore.ingest(
            packed = body,
            remotable = False,
            skip_conn = weakref.ref(connection)
        )
        
        # Object already existed.
        if obj is None:
            response = b'\x00'
        
        # Object is new
        else:
            response = b'\x01'
            
        # We don't need to wait for the mail run to have a successful return
        return response
        
    @publish.response_handler
    async def publish(self, connection, response, exc):
        ''' Handle responses to publish requests.
        '''
        if exc is not None:
            raise exc
        else:
            return True
    
    @public_api
    @request(b'GT')
    async def get(self, connection, ghid):
        ''' Request an object from the persistence provider.
        '''
        return bytes(ghid)
        
    @get.fixture
    async def get(self, connection, ghid):
        ''' Fixture to just pull directly from librarian.
        '''
        return (await self._librarian.retrieve(ghid))
        
    @get.request_handler
    async def get(self, connection, body):
        ''' Handle get requests.
        '''
        ghid = Ghid.from_bytes(body)
        return (await self._librarian.retrieve(ghid))
    
    @public_api
    @request(b'+S')
    async def subscribe(self, connection, ghid):
        ''' Subscribe to updates from the remote.
        '''
        return bytes(ghid)
        
    @subscribe.fixture
    async def subscribe(self, connection, ghid):
        ''' Manual noop.
        '''
        
    @subscribe.request_handler
    async def subscribe(self, connection, body):
        ''' Handle subscription requests.
        '''
        ghid = Ghid.from_bytes(body)
        await self._postman.subscribe(connection, ghid)
        return b'\x01'
        
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
        had_subscription = await self._postman.unsubscribe(connection, ghid)
        
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
    
    @public_api
    @request(b'!!')
    async def subscription_update(self, connection, subscription_ghid,
                                  notification_ghid):
        ''' Send a subscription update to the connection.
        '''
        payload = await self._librarian.retrieve(notification_ghid)
        return bytes(subscription_ghid) + payload
        
    @subscription_update.fixture
    async def subscription_update(self, connection, subscription_ghid,
                                  notification_ghid):
        ''' Make a manual no-op fixture, since inspect signatures
        apparently don't from_callable on a descriptor... (grrr)
        '''
        
    @subscription_update.request_handler
    async def subscription_update(self, connection, body):
        ''' Handles an incoming subscription update.
        '''
        # subscribed_ghid = Ghid.from_bytes(body[0:65])
        notification = body[65:]
        
        # Note that this handles postman scheduling as well.
        await self._percore.ingest(
            notification,
            remotable = False,
            skip_conn = connection
        )
        
        return b'\x01'
        
    @request(b'?S')
    async def query_subscriptions(self, connection):
        ''' Request a list of all currently subscribed ghids.
        '''
        return b''
        
    @query_subscriptions.request_handler
    async def query_subscriptions(self, connection, body):
        ''' Handle subscription query requests.
        '''
        ghidlist = list(await self._postman.list_subs(connection))
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
        
    @query_subscriptions.response_handler
    async def query_subscriptions(self, connection, response, exc):
        ''' Handle responses to subscription queries.
        '''
        if exc is not None:
            raise exc
        
        parser = generate_ghidlist_parser()
        return set(parser.unpack(response))
        
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
        ghidlist = await self._librarian.bind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(list(ghidlist))
        
    @query_bindings.response_handler
    async def query_bindings(self, connection, response, exc):
        ''' Handle responses to binding queries.
        '''
        if exc is not None:
            raise exc
            
        parser = generate_ghidlist_parser()
        return set(parser.unpack(response))
        
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
        ghidlist = await self._librarian.debind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(list(ghidlist))
        
    @query_debindings.response_handler
    async def query_debindings(self, connection, response, exc):
        ''' Handle responses to debinding queries.
        '''
        if exc is not None:
            raise exc
            
        parser = generate_ghidlist_parser()
        return set(parser.unpack(response))
    
    @public_api
    @request(b'?E')
    async def query_existence(self, connection, ghid):
        ''' Checks if the passed <ghid> exists at the remote.
        '''
        return bytes(ghid)
        
    @query_existence.fixture
    async def query_existence(self, connection, ghid):
        ''' Just always return false.
        '''
        return (await self._librarian.contains(ghid))
        
    @query_existence.request_handler
    async def query_existence(self, connection, body):
        ''' Handle existence queries.
        '''
        ghid = Ghid.from_bytes(body)
        if (await self._librarian.contains(ghid)):
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
        await self._postman.clear_subs(connection)
        return b'\x01'


class Salmonator(loopa.TaskLooper, metaclass=API):
    ''' Responsible for disseminating Golix objects upstream and
    downstream. Handles all comms with them as well.
    '''
    _golcore = weak_property('__golcore')
    _percore = weak_property('__percore')
    _librarian = weak_property('__librarian')
    _remote_protocol = weak_property('__remote_protocol')
    
    @public_api
    def __init__(self, *args, **kwargs):
        ''' Yarp.
        '''
        super().__init__(*args, **kwargs)
        
        self._deferred = ListMap()
        self._clear_q = None
        
        self._upstream_remotes = set()
        self._downstream_remotes = set()
        
        # Lookup for <registered ghid>
        self._registered = set()
        
        self._bootstrapped = False
        
    @fixture_api
    def RESET(self):
        ''' Clear stuff out, yo.
        '''
        self._upstream_remotes.clear()
        self._downstream_remotes.clear()
        self._registered.clear()
        
    def assemble(self, golcore, percore, librarian, remote_protocol):
        self._golcore = golcore
        self._percore = percore
        self._librarian = librarian
        self._remote_protocol = remote_protocol
    
    @fixture_noop
    @public_api
    def add_upstream_remote(self, task_commander, connection_cls, *args,
                            **kwargs):
        ''' Adds an upstream remote persister.
        *args and **kwargs will be passed to the task_commander task.
        '''
        remote = ConnectionManager(
            connection_cls = connection_cls,
            msg_handler = self._remote_protocol,
            conn_init = self.restore_connection
        )
        task_commander.register_task(remote, *args, **kwargs)
        self._upstream_remotes.add(remote)
        
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
        
    async def loop_init(self, *args, **kwargs):
        ''' On top of the usual stuff, set up our queues.
        '''
        await super().loop_init(*args, **kwargs)
        # This "must" be unbounded, because we don't have any control over
        # finalization of objects.
        self._clear_q = asyncio.Queue()
        
    async def loop_stop(self, *args, **kwargs):
        ''' On top of the usual stuff, clear our queues.
        '''
        # Would be good to do, but not currently working
        # for remote in self._persisters:
        #     await self._stop_persister(remote)
        
        await super().loop_stop(*args, **kwargs)
        self._clear_q = None
        
        disconnections = set()
        for remote in self._upstream_remotes:
            disconnections.add(
                make_background_future(remote.disconnect())
            )
        
        # Need to make sure it's not empty
        if disconnections:
            await asyncio.wait(
                fs = disconnections,
                return_when = asyncio.ALL_COMPLETED
            )
        
    async def loop_run(self, *args, **kwargs):
        ''' Wait for object finalizers, and then immediately unsub the
        remotes when the objects are removed from memory.
        '''
        to_clear = await self._clear_q.get()
        await self.deregister(to_clear)
    
    @fixture_noop
    @public_api
    async def register(self, gao):
        ''' Tells the Salmonator to listen upstream for any updates
        while the gao is retained in memory.
        '''
        if gao.dynamic:
            logger.info(
                'GAO ' + str(gao.ghid) + ' upstream registration starting.'
            )
            self._registered.add(gao.ghid)
    
            subscriptions = set()
            for remote in self._upstream_remotes:
                subscriptions.add(
                    make_background_future(remote.subscribe(gao.ghid))
                )
                
            # Need to make sure it's not empty
            if subscriptions:
                await asyncio.wait(
                    fs = subscriptions,
                    return_when = asyncio.ALL_COMPLETED
                )
                    
            # Add deregister as a finalizer, but don't call it atexit.
            finalizer = weakref.finalize(gao, self._deregister, gao.ghid)
            finalizer.atexit = False
        
    def _deregister(self, ghid):
        ''' Finalizer for GAO objects that executes async deregister()
        from within the event loop. Must be called from within our own
        thread, which it should be (finalizers are called from object
        thread, and all gao must be created within event loop's thread).
        '''
        if self._clear_q is not None:
            # This needs to be a function, not a coro, so use nowait.
            self._clear_q.put_nowait(ghid)
    
    @fixture_noop
    @public_api
    async def deregister(self, ghid):
        ''' Tells the salmonator to stop listening for upstream object
        updates.
        '''
        # This should maybe use remove instead of discard?
        self._registered.discard(ghid)
    
        unsubscriptions = set()
        for remote in self._upstream_remotes:
            unsubscriptions.add(
                make_background_future(remote.unsubscribe(ghid))
            )
        
        # Need to make sure it's not empty
        if unsubscriptions:
            await asyncio.wait(
                fs = unsubscriptions,
                return_when = asyncio.ALL_COMPLETED
            )
    
    @fixture_noop
    @public_api
    async def push(self, ghid):
        ''' Push a single ghid to all remotes.
        
        TODO: smartly defer any pushes to unavailable remotes using
        "if remote.has_connection()"
        '''
        data = await self._librarian.retrieve(ghid)
        
        tasks = set()
        for remote in self._upstream_remotes:
            if remote.has_connection:
                tasks.add(
                    make_background_future(remote.publish(data))
                )
            else:
                self._deferred.append(remote, data)
            
        # Need to make sure it's not empty
        if tasks:
            await asyncio.wait(
                fs = tasks,
                return_when = asyncio.ALL_COMPLETED
            )
    
    @fixture_noop
    @public_api
    async def pull(self, ghid):
        ''' Gets a ghid from upstream. Returns on the first result. Note
        that this is not meant to be called on a dynamic address, as a
        subs update from a slower remote would always be overridden by
        the faster one.
        '''
        pull_complete = None
        tasks_available = set()
        for remote in self._upstream_remotes:
            tasks_available.add(
                asyncio.ensure_future(
                    self._attempt_pull_single(ghid, remote)
                )
            )
            
        # Wait until the first successful task completion
        # Note that this also shields us against having no tasks
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
                exc = task.exception()
                
                # If there's been an exception, continue waiting for the rest.
                if exc is not None:
                    logger.error(
                        'Error while pulling from remote:\n' +
                        ''.join(traceback.format_tb(exc.__traceback__)) +
                        repr(exc)
                    )
                    pull_complete = None
                
                # Completed successfully, but it could be a 404 (or other
                # error), which would present as result() = False.
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
            logger.debug('Cancelling pending pulls.')
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
            if not (await self._librarian.contains(maybe_obj.target)):
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
        
        logger.debug('Successful pull handled.')
    
    @fixture_noop
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
        
    async def _attempt_pull_single(self, ghid, remote):
        ''' Attempt to fetch a single object from a single remote. If
        successful, put it into the ingestion pipeline.
        '''
        # This may error, but any errors here will be caught by the parent.
        data = await remote.get(ghid)
        
        # Call as remotable=False to avoid infinite loops.
        obj = await self._percore.ingest(data, remotable=False)
        
        # Note that ingest can either return None, if we already have
        # the object, or the object itself, if it's new.
        if obj is None:
            return True
        else:
            return obj
            
    async def bootstrap(self, account):
        ''' Bootstrapping publishes our identity upstream.
        '''
        for remote in self._upstream_remotes:
            await remote.publish(account._identity.second_party.packed)
            await remote.subscribe(account._identity.ghid)
            
        self._bootstrapped = True
    
    @fixture_noop
    @public_api
    async def restore_connection(self, remote, connection):
        ''' Start or re-start a connection.
        '''
        # We need to subscribe to our identity if we're restoring a terminated
        # connection. If we haven't bootstrapped though, that will be handled
        # there.
        if self._bootstrapped:
            await self._remote_protocol.subscribe(
                connection,
                self._golcore.whoami
            )
        
        # For every every active (salmonator-registered) GAO's ghid...
        tasks = set()
        for registrant in self._registered:
            # Record that we need to perform...
            tasks.add(
                # ...as a background future (which handles its own errors)...
                make_background_future(
                    # ...a subscription call with our connection at protocol
                    self._remote_protocol.subscribe(connection, registrant)
                )
            )
        
        # We need to make sure there's at least one task.
        if tasks:
            # And now just wait for all of that to complete.
            await asyncio.wait(
                fs = tasks,
                return_when = asyncio.ALL_COMPLETED
            )
        
        # Now, we need to destructively iterate over our deferreds until the
        # remote list is exhausted. We have to do this in order, sequentially,
        # and serially, because eg. containers require bindings, etc.
        to_push = collections.deque(self._deferred.pop_key(remote))
        try:
            while to_push:
                data = to_push.popleft()
                await self._remote_protocol.publish(connection, data)
        
        # Restore the last to_push, put it back into the deferred list, and
        # then re-raise
        except Exception:
            to_push.appendleft(data)
            self._deferred.extend(remote, to_push)
            raise
