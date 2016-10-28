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

Some notes:

'''

# External dependencies
import logging
import collections
import weakref
import os
import abc
import traceback
import asyncio
import loopa

# Intra-package dependencies
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api

from .core import _GAO

from .utils import WeakSetMap
from .utils import call_coroutine_threadsafe
from .utils import NoContext

from .exceptions import DispatchError
from .exceptions import UnknownToken


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'Dispatcher',
]

        
# ###############################################
# Lib
# ###############################################
            
            
_AppDef = collections.namedtuple(
    typename = '_AppDef',
    field_names = ('app_token',),
)
            
            
_DispatchableState = collections.namedtuple(
    typename = '_DispatchableState',
    field_names = ('api_id', 'state'),
)
            
            
# Identity here can be either a sender or recipient dependent upon context
_ShareLog = collections.namedtuple(
    typename = '_ShareLog',
    field_names = ('ghid', 'identity'),
)


class Dispatcher(loopa.TaskLooper, metaclass=API):
    ''' The Dispatcher decides which objects should be delivered where.
    This is decided through either:
    
    1. An API identifier (schema) dictating general compatibility as a
        dispatchable
    2. Declaring an object to be private, at which point the Dispatcher
        will internally (though distributedly) maintain that object to
        be exclusively available to an app token.
        
    Objects, once declared as non-private, cannot be retroactively
    privatized. That cat has officially left the bag/building/rodeo.
    However, a private object can be made non-private at a later time,
    provided it has defined an API ID.
    
    Private objects do not track their owning app tokens internally.
    Instead, this is managed through a Dispatcher-internal GAO. As such,
    even a bug resulting in a leaked private object will not result in a
    leaked app token.
    
    Ideally, the dispatcher will eventually also enforce whatever user
    restrictions on sharing are desired BETWEEN INSTALLED APPLICATIONS.
    Sharing restrictions between external parties are within the purview
    of the Rolodex.
    
    TODO: support notification mechanism to push new objects to other
    concurrent hypergolix instances. See note in ipc.ipccore.send_object
    '''
    
    @public_api
    def __init__(self, *args, **kwargs):
        ''' Yup yup yup yup yup yup yup
        '''
        super().__init__(*args, **kwargs)
        
        self._ipc_protocol_server = None
        
        # Temporarily set distributed state to None.
        # Lookup for all known tokens: set(<tokens>)
        self._all_known_tokens = None
        # Lookup (dict-like) for <app token>: <startup ghid>
        self._startup_by_token = None
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = None
        # Distributed lock for adding app tokens
        self._token_lock = None
        
        # Set of incoming shared ghids that had no endpoint
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = None
        # Setmap-like lookup for share acks that had no endpoint
        # <app token>: set(<ghids>)
        self._orphan_share_acks = None
        # Setmap-like lookup for share naks that had no endpoint
        # <app token>: set(<ghids>)
        self._orphan_share_naks = None
        
        # This is used for updating apps
        self._dispatch_q = None
        
        # Lookup <app token>: <connection/session/endpoint>
        self._endpoint_from_token = weakref.WeakValueDictionary()
        # Reverse lookup <connection/session/endpoint>: <app token>
        self._token_from_endpoint = weakref.WeakKeyDictionary()
        
        # Lookup <api ID>: set(<connection/session/endpoint>)
        self._endpoints_from_api = WeakSetMap()
        
        # This lookup directly tracks who has a copy of the object
        # Lookup <object ghid>: set(<connection/session/endpoint>)
        self._update_listeners = WeakSetMap()
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' Create a dispatch fixture.
        '''
        self._all_known_tokens = set()
        # Lookup (dict-like) for <app token>: <startup ghid>
        self._startup_by_token = {}
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = {}
        # Distributed lock for adding app tokens
        self._token_lock = NoContext()
        
        # Lookup <api ID>: set(<connection/session/endpoint>)
        self._endpoints_from_api = WeakSetMap()
        
        # Lookup <app token>: <connection/session/endpoint>
        self._endpoint_from_token = weakref.WeakValueDictionary()
        # Reverse lookup <connection/session/endpoint>: <app token>
        self._token_from_endpoint = weakref.WeakKeyDictionary()
        
    @fixture_api
    def RESET(self):
        ''' Reset the fixture to a pristine state.
        '''
        self.__init__()
        
    def assemble(self, ipc_protocol_server):
        # Set up a weakref to the ipc system
        self._ipc_protocol_server = weakref.proxy(ipc_protocol_server)
        
    def bootstrap(self, all_tokens, startup_objs, private_by_ghid, token_lock,
                  incoming_shares, orphan_acks, orphan_naks):
        ''' Initialize distributed state.
        '''
        # Now init distributed state.
        # All known tokens must already contain a key for b'\x00\x00\x00\x00'.
        if b'\x00\x00\x00\x00' not in all_tokens:
            all_tokens.add(b'\x00\x00\x00\x00')
            
        # Lookup for all known tokens: set(<tokens>)
        self._all_known_tokens = all_tokens
        # Lookup (set-map-like) for <app token>: set(<startup ghids>)
        self._startup_by_token = startup_objs
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = private_by_ghid
        
        # These need to be distributed but aren't yet. TODO!
        # Distributed lock for adding app tokens
        self._token_lock = token_lock
        
        # Set of incoming shared ghids that had no endpoint
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = incoming_shares
        # Setmap-like lookup for share acks that had no endpoint
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_acks = orphan_acks
        # Setmap-like lookup for share naks that had no endpoint
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_naks = orphan_naks
            
    async def loop_init(self):
        ''' Init the two async queues.
        '''
        self._dispatch_q = asyncio.Queue()
        
    async def loop_run(self):
        ''' Wait for anything incoming.
        '''
        try:
            dispatch_coro, args, kwargs = await self._dispatch_q.get()
            await dispatch_coro(*args, **kwargs)
            
        except asyncio.CancelledError:
            raise
        
        except Exception:
            # At some point we'll need some kind of proper handling for this.
            logger.error(
                'Dispatch raised with traceback:\n' +
                ''.join(traceback.format_exc())
            )
        
    async def loop_stop(self):
        ''' Remove the async queues.
        '''
        self._dispatch_q = None
        
    @public_api
    def add_api(self, connection, api_id):
        ''' Register the connection as currently tracking the api_id.
        '''
        self._endpoints_from_api.add(api_id, connection)
        
    @public_api
    def remove_api(self, connection, api_id):
        ''' Remove a connection's registration for the api_id. Happens
        automatically when connections are GC'd.
        '''
        self._endpoints_from_api.discard(api_id, connection)
        
    def new_token(self):
        # Use a dummy api_id to force the while condition to be true initially
        token = b'\x00\x00\x00\x00'
        # Birthday paradox be damned; we can actually *enforce* uniqueness
        while token in self._all_known_tokens:
            token = os.urandom(4)
        return token
        
    @public_api
    def start_application(self, connection, token=None):
        ''' Ensures that an application is known to the dispatcher.
        
        Currently just checks within all known tokens. In the future,
        this be responsible for starting the application (unless we move
        that responsibility elsewhere), and then sending all of the
        startup objects to the application.
        '''
        if token is None:
            with self._token_lock:
                token = self.new_token()
                # Do this right away to prevent race condition
                self._all_known_tokens.add(token)
            
        elif token not in self._all_known_tokens:
            raise UnknownToken('App token unknown to dispatcher.')
            
        # TODO: should these be enclosed within an operations lock?
        self._endpoint_from_token[token] = connection
        self._token_from_endpoint[connection] = token
        
        return token
        
    @start_application.fixture
    def start_application(self, connection, token=None):
        if token is None:
            token = os.urandom(4)
        
        # Don't emulate normal dispatcher behavior here; let everything start,
        # regardless of status re: "exists in tokens"
        self._all_known_tokens.add(token)
        self._endpoint_from_token[token] = connection
        self._token_from_endpoint[connection] = token
        
        return token
            
    @public_api
    def track_object(self, connection, ghid):
        ''' Registers a connection as tracking a ghid.
        '''
        self._update_listeners.add(ghid, connection)
        
    @track_object.fixture
    def track_object(self, connection, ghid):
        ''' Don't do anything for this fixture.
        '''
        
    @public_api
    def untrack_object(self, connection, ghid):
        ''' Remove a connection as tracking a ghid.
        '''
        self._update_listeners.discard(ghid, connection)
        
    @untrack_object.fixture
    def untrack_object(self, connection, ghid):
        ''' Don't do anything for this fixture.
        '''
        
    @public_api
    async def register_object(self, connection, ghid, private):
        ''' Call this every time a new object is created to register it
        with the dispatcher, recording it as private or distributing it
        to other applications as needed.
        '''
        # If the object is private, register it as such.
        if private:
            try:
                token = self._token_from_endpoint[connection]
            
            except KeyError as exc:
                raise UnknownToken(
                    'Must register app token before creating private objects.'
                ) from exc
                
            else:
                logger.debug(
                    'Creating private object for ' + str(connection) +
                    '; bypassing distribution.'
                )
                self._private_by_ghid[ghid] = token
            
        # Otherwise, make sure to notify any other interested parties.
        else:
            await self.schedule_share_distribution(
                ghid,
                origin = self._golcore.whoami,
                skip_conn = connection
            )
            
    @register_object.fixture
    async def register_object(self, connection, ghid, private):
        ''' Just don't distribute the object.
        '''
        if private:
            try:
                token = self._token_from_endpoint[connection]
            
            except KeyError as exc:
                raise UnknownToken(
                    'Must register app token before creating private objects.'
                ) from exc
                
            else:
                logger.debug(
                    'Creating private object for ' + str(connection) +
                    '; bypassing distribution.'
                )
                self._private_by_ghid[ghid] = token
    
    async def schedule_share_distribution(self, ghid, origin, skip_conn=None):
        ''' Schedules a distribution of an object share.
        '''
        logger.debug('Scheduling share dispatch for {!s}.'.format(ghid))
        await self._dispatch_q.put((
            self._distribute_update, ghid, origin, skip_conn
        ))
        
    async def _distribute_share(self, ghid, origin, skip_conn):
        ''' Perform an actual share distribution.
        '''
        callsheet = set()
        private_owner = self.get_parent_token(ghid)
        
        # The object has a private owner, so we're exclusively going to it.
        if private_owner:
            try:
                callsheet.add(
                    self._endpoint_from_token[private_owner]
                )
            except KeyError:
                logger.warning(
                    'Connection unavailable for app token {!s}.'.format(
                        private_owner
                    )
                )
            
        # This is not a private object, so we're going to anyone who wants it
        else:
            obj = self._oracle.get_object(
                gaoclass = _Dispatchable,
                ghid = ghid,
                dispatch = self._dispatch,
                ipc_core = self
            )
            
            callsheet.update(
                # Get any connections that have registered the api_id
                self._endpoints_from_api.get_any(obj.api_id)
            )
            callsheet.update(
                # Get any connections that have an instance of the object
                self._update_listeners.get_any(obj.ghid)
            )
            
        # Now, check to see if we have anything in the callsheet. Do it before
        # discarding, so that we know if it's actually an orphan share.
        if callsheet:
            # Discard the skipped connection, if one is defined
            callsheet.discard(skip_conn)
            logger.debug(
                'Distributing {!s} share from {!s} to {!r}.'.format(
                    ghid, origin, callsheet
                )
            )
            
            await self._distribute(
                self._ipc_protocol_server.share_obj,    # distr_coro
                callsheet,
                ghid,
                origin
            )
            
        else:
            sharelog = _ShareLog(ghid, origin)
            self._orphan_incoming_shares.add(sharelog)
    
    async def schedule_update_distribution(self, ghid, deleted=False,
                                           skip_conn=None):
        ''' Schedules a distribution of an object update.
        '''
        logger.debug('Scheduling update dispatch for {!s}.'.format(ghid))
        await self._dispatch_q.put((
            self._distribute_update, ghid, deleted, skip_conn
        ))
        
    async def _distribute_update(self, ghid, deleted, skip_conn):
        ''' Perform an actual update distribution.
        '''
        # Get any connections that have an instance of the object
        callsheet = set()
        # Note that this call returns a frozenset, hence the copy.
        callsheet.update(self._update_listeners.get_any(ghid))
        # Skip the connection if one is passed.
        callsheet.discard(skip_conn)
        
        logger.debug(
            'Distributing {!s} update to {!r}.'.format(ghid, callsheet)
        )
        
        if deleted:
            await self._distribute(
                self._ipc_protocol_server.delete_obj,   # distr_coro
                callsheet,
                ghid
            )
            
        else:
            await self._distribute(
                self._ipc_protocol_server.update_obj,   # distr_coro
                callsheet,
                ghid
            )
            
    async def schedule_sharesuccess_distribution(self, ghid, recipient,
                                                 tokens):
        ''' Schedules notification of any available connections for all
        passed <tokens> of a share failure.
        '''
        logger.debug(
            'Scheduling share success dispatch for {!s}.'.format(ghid)
        )
        await self._dispatch_q.put((
            self._distribute_sharesuccess, ghid, recipient, tokens
        ))
    
    async def _distribute_sharesuccess(self, ghid, recipient, tokens):
        ''' Notify any available connections for all passed <tokens> of
        a share failure.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._endpoint_from_token[token])
            except KeyError:
                logger.info('No connection for token ' + str(token))
        
        # Distribute the share failure to all apps that requested its delivery
        await self._distribute(
            self._ipc_protocol_server.notify_share_success,   # distr_coro
            callsheet,
            ghid,
            recipient
        )
            
    async def schedule_sharefailure_distribution(self, ghid, recipient,
                                                 tokens):
        ''' Schedules notification of any available connections for all
        passed <tokens> of a share failure.
        '''
        logger.debug(
            'Scheduling share failure dispatch for {!s}.'.format(ghid)
        )
        await self._dispatch_q.put((
            self._distribute_sharefailure, ghid, recipient, tokens
        ))
    
    async def _distribute_sharefailure(self, ghid, recipient, tokens):
        ''' Notify any available connections for all passed <tokens> of
        a share failure.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._endpoint_from_token[token])
            except KeyError:
                logger.info('No connection for token ' + str(token))
        
        # Distribute the share failure to all apps that requested its delivery
        await self._distribute(
            self._ipc_protocol_server.notify_share_failure,   # distr_coro
            callsheet,
            ghid,
            recipient
        )
                
    async def _distribute(self, distr_coro, callsheet, *args, **kwargs):
        ''' Call distr_coro with *args and **kwargs at each of the
        connections in the callsheet. Log (but don't raise) any errors.
        '''
        distributions = []
        for connection in callsheet:
            # For each connection...
            distributions.append(
                # ...in parallel, schedule a single execution of the
                # distribution coroutine.
                asyncio.ensure_future(distr_coro(connection, *args, **kwargs))
            )
            
        # And gather the results, logging (but not raising) any exceptions
        try:
            distribution_task = asyncio.gather(
                *distributions,
                return_exceptions = True
            )
            results = await distribution_task
            
        except asyncio.CancelledError:
            distribution_task.cancel()
            raise
        
        else:
            exceptions = [
                result for result in results if isinstance(result, Exception)
            ]
            for exc in exceptions:
                logger.error(
                    'Error distributing share:\n' +
                    ''.join(traceback.format_exception(
                        type(exc),
                        exc,
                        exc.__traceback__
                    ))
                )
            
    @public_api
    def which_token(self, connection):
        ''' Return the token associated with the connection, or None if
        there is no currently defined token.
        '''
        try:
            return self._token_from_endpoint[connection]
            
        except KeyError as exc:
            return None
            
    @public_api
    def which_connection(self, token):
        ''' Returns the current connection associated with the token, or
        None if there is no currently available connection.
        '''
        try:
            return self._endpoint_from_token[token]
            
        except KeyError as exc:
            return None
            
    @public_api
    def register_startup(self, connection, ghid):
        ''' Registers a ghid to be used as a startup object for token.
        '''
        try:
            token = self._token_from_endpoint[connection]
            
        except KeyError as exc:
            raise UnknownToken(
                'Must register app token before registering startup objects.'
            ) from exc
            
        else:
            if token in self._startup_by_token:
                raise ValueError(
                    'Startup object already defined for that application. '
                    'Deregister it before registering a new startup object.'
                )
            else:
                self._startup_by_token[token] = ghid
                
    @public_api
    def deregister_startup(self, token):
        ''' Deregisters a ghid to be used as a startup object for token.
        '''
        with self._token_lock:
            if token not in self._all_known_tokens:
                raise UnknownToken()
            elif token not in self._startup_by_token:
                raise ValueError(
                    'Startup object has not been defined for that application.'
                )
            else:
                del self._startup_by_token[token]
    
    @public_api
    def get_startup_obj(self, token):
        ''' Returns the ghid of the declared startup object for that
        token, or None if none has been declared.
        '''
        with self._token_lock:
            if token not in self._all_known_tokens:
                raise UnknownToken()
            elif token not in self._startup_by_token:
                return None
            else:
                return self._startup_by_token[token]
        
    def make_public(self, ghid):
        ''' Makes a private object public.
        '''
        try:
            del self._private_by_ghid[ghid]
            
        except KeyError as exc:
            raise ValueError(
                'Obj w/ passed ghid is unknown or already public: ' + str(ghid)
            ) from exc
            
    def get_parent_token(self, ghid):
        ''' Returns the app_token parent for the passed ghid, if (and
        only if) it's private. Otherwise, returns None.
        '''
        try:
            return self._private_by_ghid[ghid]
        except KeyError:
            logger.debug(str(ghid) + ' has no parent token.')
            return None
            
            
class _Dispatchable(_GAO):
    ''' A dispatchable object.
    '''
    
    def __init__(self, dispatch, ipc_core, api_id=None, private=False,
                 state=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Dispatch is already a weakref.proxy...
        self._dispatch = dispatch
        # But ipc_core is not.
        self._ipccore = weakref.proxy(ipc_core)
        
        self.state = state
        self.api_id = api_id
        
    @property
    def parent_token(self):
        ''' Read-only proxy to dispatch to check for a private parent.
        Returns the parent app token, or None if not a private object.
        '''
        return self._dispatch.get_parent_token(self.ghid)
        
    @property
    def private(self):
        ''' Returns true/false if the object is private.
        '''
        return bool(self._dispatch.get_parent_token(self.ghid))
        
    def pull(self, *args, **kwargs):
        ''' Refreshes self from upstream. Should NOT be called at object
        instantiation for any existing objects. Should instead be called
        directly, or through _weak_pull for any new status.
        '''
        modified = super().pull(*args, **kwargs)
        if modified:
            logger.debug('Pull detected modifications. Sending to ipc.')
            call_coroutine_threadsafe(
                coro = self._ipccore.notify_update(self.ghid, deleted=False),
                loop = self._ipccore._loop
            )
            logger.debug('IPC completed update notifications.')
        return modified
        
    @staticmethod
    def _pack(state):
        ''' Packs state into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        '''
        version = b'\x00'
        return b'hgxd' + version + state[0] + state[1]
        
    @staticmethod
    def _unpack(packed):
        ''' Unpacks state from a bytes object. May be overwritten in
        subs to unpack more complex objects. Should always be a
        staticmethod or classmethod.
        '''
        magic = packed[0:4]
        version = packed[4:5]
        
        if magic != b'hgxd':
            raise DispatchError('Object does not appear to be dispatchable.')
        if version != b'\x00':
            raise DispatchError('Incompatible dispatchable version number.')
            
        api_id = packed[5:70]
        state = packed[70:]
        
        return _DispatchableState(api_id, state)
        
    def apply_state(self, state):
        ''' Apply the UNPACKED state to self.
        '''
        # TODO: make sure this doesn't accidentally change api_id or app_token
        # Maybe set the _attributes directly or something as well?
        self.api_id = state[0]
        self.state = state[1]
        
    def extract_state(self):
        ''' Extract self into a packable state.
        '''
        return _DispatchableState(self.api_id, self.state)
        
    @property
    def api_id(self):
        # Warn if both api_id and app_token are undefined
        # Is this the appropriate time to check this?
        if self._api_id == bytes(65):
            return None
        else:
            return self._api_id
        
    @api_id.setter
    def api_id(self, value):
        if value is None:
            value = bytes(65)
        else:
            value = bytes(value)
            if len(value) != 65:
                raise ValueError('API IDs must be 65 bytes long.')
            
        self._api_id = value
        
    def update(self, state):
        ''' Wrapper to apply state that reuses api_id and app_token, and
        then call push.
        '''
        if not self.dynamic:
            raise DispatchError(
                'Object is not dynamic. Cannot update.'
            )
            
        self.apply_state(
            state = (self.api_id, state)
        )
        self.push()
        
    def apply_delete(self):
        super().apply_delete()
        call_coroutine_threadsafe(
            coro = self._ipccore.notify_update(self.ghid, deleted=True),
            loop = self._ipccore._loop
        )
