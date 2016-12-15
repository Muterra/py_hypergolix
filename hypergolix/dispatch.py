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

from loopa.utils import make_background_future

# Intra-package dependencies
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api
from .hypothetical import fixture_noop

from .gao import GAOCore

from .utils import ApiID
from .utils import WeakSetMap
from .utils import NoContext
from .utils import weak_property
from .utils import immutable_property

from .exceptions import DispatchError
from .exceptions import UnknownToken
from .exceptions import ExistantAppError
from .exceptions import CatOuttaBagError


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
            
            
# Identity here can be either a sender or recipient dependent upon context
_ShareLog = collections.namedtuple(
    typename = '_ShareLog',
    field_names = ('ghid', 'identity'),
)


class Dispatcher(metaclass=API):
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
    
    TODO: move most of the misc stuff into dedicated new_object and
    get_object methods within dispatch, which (for the most part) just
    pass on straight to the oracle, but add all of the extra tracking
    stuff (and so on) internally.
    '''
    _account = weak_property('__account')
    _ipc_protocol = weak_property('__ipc_protocol')
    _oracle = weak_property('__oracle')
    
    @public_api
    def __init__(self, *args, **kwargs):
        ''' Yup yup yup yup yup yup yup
        '''
        super().__init__(*args, **kwargs)
        
        # Temporarily set distributed state to None.
        # Lookup for all known tokens: set(<tokens>)
        self._all_known_tokens = None
        # Lookup (dict-like) for <app token>: <startup ghid>
        self._startup_by_token = None
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = None
        # Distributed lock for adding app tokens
        self._token_lock = None
        
        # Set of incoming shared ghids that had no conn
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = None
        # Setmap-like lookup for share acks that had no conn
        # <app token>: set(<ghids>)
        self._orphan_share_acks = None
        # Setmap-like lookup for share naks that had no conn
        # <app token>: set(<ghids>)
        self._orphan_share_naks = None
        
        # Lookup <app token>: <connection/session/conn>
        self._conn_from_token = weakref.WeakValueDictionary()
        # Reverse lookup <connection/session/conn>: <app token>
        self._token_from_conn = weakref.WeakKeyDictionary()
        
        # Lookup <api ID>: set(<connection/session/conn>)
        self._conns_from_api = WeakSetMap()
        
        # This lookup directly tracks who has a copy of the object
        # Lookup <object ghid>: set(<connection/session/conn>)
        self._update_listeners = WeakSetMap()
        # This lookup preserves a strong reference to the dispatchable **as the
        # key**, allowing the object to be GC'd from local memory only when no
        # connections remain that have it
        self._obj_binding = WeakSetMap()
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' Create a dispatch fixture.
        '''
        super(Dispatcher.__fixture__, self).__init__(*args, **kwargs)
        
        self._all_known_tokens = set()
        # Lookup (dict-like) for <app token>: <startup ghid>
        self._startup_by_token = {}
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = {}
        # Distributed lock for adding app tokens
        self._token_lock = NoContext()
        
        # Lookup <api ID>: set(<connection/session/conn>)
        self._conns_from_api = WeakSetMap()
        
        # Lookup <app token>: <connection/session/conn>
        self._conn_from_token = weakref.WeakValueDictionary()
        # Reverse lookup <connection/session/conn>: <app token>
        self._token_from_conn = weakref.WeakKeyDictionary()
        
    @fixture_api
    def RESET(self):
        ''' Reset the fixture to a pristine state.
        '''
        self._all_known_tokens.clear()
        # Lookup (dict-like) for <app token>: <startup ghid>
        self._startup_by_token.clear()
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid.clear()
        # Lookup <api ID>: set(<connection/session/conn>)
        self._conns_from_api.clear_all()
        
        # Lookup <app token>: <connection/session/conn>
        self._conn_from_token.clear()
        # Reverse lookup <connection/session/conn>: <app token>
        self._token_from_conn.clear()
        
    def assemble(self, oracle, ipc_protocol):
        self._oracle = oracle
        self._ipc_protocol = ipc_protocol
        
    @public_api
    def bootstrap(self, account):
        ''' Initialize distributed state.
        '''
        self._account = account
        
        # Now init distributed state.
            
        # Lookup for all known tokens: set(<tokens>)
        self._all_known_tokens = account.dispatch_tokens
        # Lookup (set-map-like) for <app token>: set(<startup ghids>)
        self._startup_by_token = account.dispatch_startup
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = account.dispatch_private
        
        # These need to be distributed but aren't yet. TODO!
        # Distributed lock for adding app tokens
        self._token_lock = NoContext()
        
        # Set of incoming shared ghids that had no conn
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = account.dispatch_incoming
        # Setmap-like lookup for share acks that had no conn
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_acks = account.dispatch_orphan_acks
        # Setmap-like lookup for share naks that had no conn
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_naks = account.dispatch_orphan_naks
        
        # All known tokens must already contain a key for b'\x00\x00\x00\x00'.
        # This is necessary because dispatch uses the zero token to indicate a
        # lack of a token in an exchange
        if b'\x00\x00\x00\x00' not in self._all_known_tokens:
            self._all_known_tokens.add(b'\x00\x00\x00\x00')
            
    @public_api
    def token_lookup(self, connection):
        ''' Return the token associated with the connection, or None if
        there is no currently defined token.
        '''
        try:
            return self._token_from_conn[connection]
            
        except KeyError as exc:
            return None
            
    @public_api
    def connection_lookup(self, token):
        ''' Returns the current connection associated with the token, or
        None if there is no currently available connection.
        '''
        try:
            return self._conn_from_token[token]
            
        except KeyError as exc:
            return None
    
    @public_api
    def private_parent_lookup(self, ghid):
        ''' Returns the app_token parent for the passed ghid, if (and
        only if) it's private. Otherwise, returns None.
        '''
        try:
            return self._private_by_ghid[ghid]
        except KeyError:
            return None
    
    @public_api
    async def make_public(self, ghid):
        ''' Makes a private object public.
        '''
        try:
            del self._private_by_ghid[ghid]
            
        except KeyError as exc:
            raise ValueError(
                'Obj w/ passed ghid is unknown or already public: ' + str(ghid)
            ) from exc
            
        else:
            await self._account.flush()
            
    @make_public.fixture
    async def make_public(self, ghid):
        # Bypass all validation
        del self._private_by_ghid[ghid]
        
    @public_api
    async def add_api(self, connection, api_id):
        ''' Register the connection as currently tracking the api_id.
        '''
        self._conns_from_api.add(api_id, connection)
        await self._account.flush()
        
    @add_api.fixture
    async def add_api(self, connection, api_id):
        # Bypass account flushing
        self._conns_from_api.add(api_id, connection)
        
    @public_api
    async def remove_api(self, connection, api_id):
        ''' Remove a connection's registration for the api_id. Happens
        automatically when connections are GC'd.
        '''
        self._conns_from_api.discard(api_id, connection)
        await self._account.flush()
        
    @remove_api.fixture
    async def remove_api(self, connection, api_id):
        # Bypass account flushing
        self._conns_from_api.discard(api_id, connection)
        
    def _make_new_token(self):
        token = None
        # Birthday paradox be damned; we can actually *enforce* uniqueness
        while token is None or token in self._all_known_tokens:
            token = os.urandom(4)
        return token
        
    @public_api
    async def start_application(self, connection, token=None):
        ''' Ensures that an application is known to the dispatcher.
        
        Currently just checks within all known tokens. In the future,
        this be responsible for starting the application (unless we move
        that responsibility elsewhere), and then sending all of the
        startup objects to the application.
        '''
        if connection in self._token_from_conn:
            raise ExistantAppError('Token already registered for connection.')
            
        elif token in self._conn_from_token:
            raise ExistantAppError(
                'Token already started at different connection.'
            )
            
        elif token is None:
            # Token lock is currently a NoContext, but eventually will need to
            # be a distributed lock -- or, the token lookup needs to be made
            # into a no-contention system. Which I'm not sure it possibly can
            # be, since we're best off actually ENFORCING the "no birthday
            # collisions" for tokens.
            with self._token_lock:
                token = self._make_new_token()
                # Do this right away to prevent race condition
                self._all_known_tokens.add(token)
                await self._account.flush()
            
        elif token not in self._all_known_tokens:
            raise UnknownToken('App token unknown to dispatcher.')
            
        # TODO: should these be enclosed within an operations lock?
        self._conn_from_token[token] = connection
        self._token_from_conn[connection] = token
        
        return token
        
    @start_application.fixture
    async def start_application(self, connection, token=None):
        if token is None:
            token = os.urandom(4)
        
        # Don't emulate normal dispatcher behavior here; let everything start,
        # regardless of status re: "exists in tokens"
        self._all_known_tokens.add(token)
        self._conn_from_token[token] = connection
        self._token_from_conn[connection] = token
        
        return token
    
    @fixture_noop
    @public_api
    async def track_object(self, connection, ghid):
        ''' Registers a connection as tracking a ghid.
        
        This is necessary so that upstream updates can be properly
        dispatched to any applications with copies of the object.
        '''
        logger.debug(
            'CONN ' + str(connection) + ' tracking ' + str(ghid) + '...'
        )
        self._update_listeners.add(ghid, connection)
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            api_id = None,  # Let _pull() apply this.
            state = None,   # Let _pull() apply this.
            dispatch = self,
            ipc_protocol = self._ipc_protocol,
            account = self._account
        )
        # This keeps the object in memory, allowing it to receive subscriptions
        # When all apps untrack the object, it gets unsubbed.
        self._obj_binding.add(obj, connection)
    
    @fixture_noop
    @public_api
    async def untrack_object(self, connection, ghid):
        ''' Remove a connection as tracking a ghid.
        
        This indicates an application no longer has a copy of the
        object, therefore silencing any updates it would otherwise have
        received.
        '''
        logger.debug(
            'CONN ' + str(connection) + ' untracking ' + str(ghid) + '...'
        )
        self._update_listeners.discard(ghid, connection)
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            api_id = None,  # Let _pull() apply this.
            state = None,   # Let _pull() apply this.
            dispatch = self,
            ipc_protocol = self._ipc_protocol,
            account = self._account
        )
        # This keeps the object in memory, allowing it to receive subscriptions
        # When all apps untrack the object, it gets unsubbed.
        self._obj_binding.discard(obj, connection)
        
    @public_api
    async def register_object(self, connection, ghid, private):
        ''' Call this every time a new object is created to register it
        with the dispatcher, recording it as private or distributing it
        to other applications as needed.
        '''
        # If the object is private, register it as such.
        if private:
            try:
                token = self._token_from_conn[connection]
            
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
            make_background_future(
                self.distribute_share(
                    ghid,
                    origin = None,
                    skip_conn = connection
                )
            )
            
    @register_object.fixture
    async def register_object(self, connection, ghid, private):
        ''' Just don't distribute the object.
        '''
        if private:
            try:
                token = self._token_from_conn[connection]
            
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
            
    @public_api
    async def register_startup(self, connection, ghid):
        ''' Registers a ghid to be used as a startup object for token.
        '''
        try:
            token = self._token_from_conn[connection]
            
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
                await self._account.flush()
                
    @public_api
    async def deregister_startup(self, connection):
        ''' Deregisters a ghid to be used as a startup object for token.
        '''
        try:
            token = self._token_from_conn[connection]
            
        except KeyError as exc:
            raise UnknownToken(
                'Must register app token before deregistering startup objects.'
            ) from exc
            
        else:
            if token not in self._startup_by_token:
                raise ValueError(
                    'Startup object has not been defined for that application.'
                )
            else:
                del self._startup_by_token[token]
                await self._account.flush()
    
    @public_api
    async def get_startup_obj(self, token):
        ''' Returns the ghid of the declared startup object for that
        token, or None if none has been declared.
        '''
        if token not in self._all_known_tokens:
            raise UnknownToken()
        
        elif token not in self._startup_by_token:
            return None
        
        else:
            return self._startup_by_token[token]
    
    @fixture_noop
    @public_api
    async def distribute_share(self, ghid, origin, skip_conn=None):
        ''' Perform an actual share distribution.
        
        Pass an explicit None as the origin to indicate self as origin.
        '''
        callsheet = set()
        
        # Note that this is called in exactly two situations: when creating a
        # new object locally, through register_object, or when receiving a new
        # share, through rolodex. As such, this will never be called when the
        # object is private. So, don't check for a private owner.
        
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            api_id = None,  # Let _pull() apply this.
            state = None,   # Let _pull() apply this.
            dispatch = self,
            ipc_protocol = self._ipc_protocol,
            account = self._account
        )
        
        callsheet.update(
            # Get any connections that have registered the api_id
            self._conns_from_api.get_any(obj.api_id)
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
            str_origin = origin or 'self'
            logger.debug(
                'Distributing ' + str(ghid) + ' shared object from ' +
                str(str_origin) + ' to ' + repr(callsheet)
            )
            
            # If we still have a callsheet, distribute it.
            if callsheet:
                await self._distribute(
                    self._ipc_protocol.share_obj,    # distr_coro
                    callsheet,
                    ghid,
                    origin,
                    obj.api_id
                )
        
        # Only make an orphan_shares entry if we didn't create the object.
        elif origin is not None:
            sharelog = _ShareLog(ghid, origin)
            self._orphan_incoming_shares.add(sharelog)
            await self._account.flush()
    
    @fixture_noop
    @public_api
    async def distribute_update(self, ghid, deleted=False, skip_conn=None):
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
                self._ipc_protocol.delete_obj,   # distr_coro
                callsheet,
                ghid
            )
            
        else:
            await self._distribute(
                self._ipc_protocol.update_obj,   # distr_coro
                callsheet,
                ghid
            )
            
    @fixture_noop
    @public_api
    async def distribute_share_success(self, ghid, recipient, tokens):
        ''' Notify any available connections for all passed <tokens> of
        a share failure.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._conn_from_token[token])
            except KeyError:
                logger.info('No connection for token ' + str(token))
        
        # Distribute the share failure to all apps that requested its delivery
        await self._distribute(
            self._ipc_protocol.notify_share_success,   # distr_coro
            callsheet,
            ghid,
            recipient
        )
    
    @fixture_noop
    @public_api
    async def distribute_share_failure(self, ghid, recipient, tokens):
        ''' Notify any available connections for all passed <tokens> of
        a share failure.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._conn_from_token[token])
            except KeyError:
                logger.info('No connection for token ' + str(token))
        
        # Distribute the share failure to all apps that requested its delivery
        await self._distribute(
            self._ipc_protocol.notify_share_failure,   # distr_coro
            callsheet,
            ghid,
            recipient
        )
                
    async def _distribute(self, distr_coro, callsheet, *args, **kwargs):
        ''' Call distr_coro with *args and **kwargs at each of the
        connections in the callsheet. Log (but don't raise) any errors.
        '''
        logger.info(
            'Dispatching ' + str(len(callsheet)) + ' calls to ' +
            str(distr_coro)
        )
        distributions = set()
        for connection in callsheet:
            # For each connection...
            distributions.add(
                # ...in parallel, schedule a single execution of the
                # distribution coroutine.
                make_background_future(distr_coro(connection, *args, **kwargs))
            )
            
        # And gather the results, logging (but not raising) any exceptions.
        # BUT, only do it if we have some to do.
        if distributions:
            try:
                await asyncio.wait(
                    fs = distributions,
                    return_when = asyncio.ALL_COMPLETED
                )
            
            except asyncio.CancelledError:
                logger.debug('Cancelling pending distributions.')
                for distr in distributions:
                    distr.cancel()
                raise
            
            
class _Dispatchable(GAOCore, metaclass=API):
    ''' A dispatchable object. Note that privacy is store within the
    dispatcher itself, and not within the individual dispatchables.
    '''
    _dispatch = weak_property('__dispatch')
    _account = weak_property('__account')
    _ipc_protocol = weak_property('__ipc_protocol')
    api_id = immutable_property('_api_id')
    
    def __init__(self, *args, api_id, state, dispatch, ipc_protocol, account,
                 **kwargs):
        super().__init__(*args, **kwargs)
        
        self._dispatch = dispatch
        self._ipc_protocol = ipc_protocol
        self._account = account
        
        # These may be explicitly set to None to allow pull() to apply them.
        if state is not None:
            self.state = state
            
        if api_id is not None:
            self.api_id = api_id
            
    @public_api
    async def _push(self):
        ''' Extend GAOCore._push() to flush the account upon completion.
        '''
        await super()._push()
        await self._account.flush()
        
    @_push.fixture
    async def _push(self):
        ''' Call super() to FIXTURE super instead of dispatchable.
        '''
        await super(_Dispatchable, self)._push()
    
    @public_api
    async def _pull(self):
        ''' Extend GAOCore._pull() to notify IPC about updates.
        '''
        await super()._pull()
        make_background_future(
            self._dispatch.distribute_update(self.ghid)
        )
        
    @_pull.fixture
    async def _pull(self):
        ''' Call super() to FIXTURE super instead of dispatchable.
        '''
        await super(_Dispatchable, self)._pull()
        
    async def pack_gao(self):
        ''' Packs state into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        '''
        version = b'\x00'
        return b'hgxd' + version + bytes(self.api_id) + self.state
        
    async def unpack_gao(self, packed):
        ''' Unpacks state from a bytes object. May be overwritten in
        subs to unpack more complex objects. Should always be a
        staticmethod or classmethod.
        '''
        magic = packed[0:4]
        version = packed[4:5]
        api_id = ApiID.from_bytes(packed[5:70])
        
        if magic != b'hgxd':
            raise DispatchError('Object does not appear to be dispatchable.')
        elif version != b'\x00':
            raise DispatchError('Incompatible dispatchable version number.')
        elif getattr(self, 'api_id', None) is None:
            self.api_id = api_id
        else:
            # YES, this does need to be _api_id (with underscore!) because
            # we may need to override something existing.
            self._api_id = api_id
        # Checking for this from other people isn't really that useful. We'll
        # already error out if you try to change it locally, and if someone
        # goes to the effort of changing it elsewhere, well, more power to them
        # I guess.
        # elif api_id != self.api_id:
        #     raise DispatchError('Cannot change API ID.')
            
        self.state = packed[70:]
    
    @fixture_noop
    @public_api
    async def apply_delete(self, debinding):
        ''' Extend GAOCore.apply_delete to notify consumers of deletion.
        '''
        await super().apply_delete(debinding)
        make_background_future(
            self._dispatch.distribute_update(self.ghid, deleted=True)
        )
        
    def __eq__(self, other):
        ''' Ensure the other object is the same gao with the same state.
        Intended pretty much exclusively for use in testing.
        '''
        equal = True
        
        try:
            equal &= (self.dynamic == other.dynamic)
            equal &= (self.ghid == other.ghid)
            equal &= (self.author == other.author)
            equal &= (self.state == other.state)
            equal &= (self.api_id == other.api_id)
            
        except AttributeError:
            equal = False
        
        return equal
        
    def __hash__(self):
        ''' Dispatchable hashes should mix the hashes of the api_id, the
        author, and the ghid together, but only if all of the above are
        defined.
        '''
        defined = self.ghid is not None
        defined &= self.author is not None
        defined &= self.api_id is not None
        
        if not defined:
            raise TypeError(
                'Dispatchable objects may only be hashed after they are ' +
                'fully defined (with a ghid, an author, and an api_id.'
            )
        
        return hash(self.ghid) ^ hash(self.author) ^ hash(self.api_id)
        
    def __del__(self, *args, **kwargs):
        ''' Log the removal of the dispatchable.
        '''
        logger.info(
            'GAO ' + str(self.ghid) + ' passed out of memory and is being ' +
            'locally garbage collected by Python.'
        )
