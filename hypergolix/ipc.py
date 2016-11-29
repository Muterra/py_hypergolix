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


Some thoughts:

Misc extras:
    + More likely than not, all persistence remotes should also use a
        single autoresponder, through the salmonator. Salmonator should
        then be moved into hypergolix.remotes instead of .persistence.
    + At least for now, applications must ephemerally declare themselves
        capable of supporting a given API. Note, once again, that these
        api_id registrations ONLY APPLY TO UNSOLICITED OBJECT SHARES!
    
It'd be nice to remove the msgpack dependency in utils.IPCPackerMixIn.
    + Could use very simple serialization instead.
    + Very heavyweight for such a silly thing.
    + It would take very little time to remove.
    + This should wait until we have a different serialization for all
        of the core bootstrapping _GAOs. This, in turn, should wait
        until after SmartyParse is converted to be async.
        
IPC Apps should not have access to objects that are not _Dispatchable.
    + Yes, this introduces some overhead. Currently, it isn't the most
        efficient abstraction.
    + Non-dispatchable objects are inherently un-sharable. That's the
        most fundamental issue here.
    + Note that private objects are also un-sharable, so they should be
        able to bypass some overhead in the future (see below)
    + Future effort will focus on making the "dispatchable" wrapper as
        efficient an abstraction as possible.
    + This basically makes a judgement call that everything should be
        sharable.
'''

# External dependencies
import collections
import traceback
# These are just used for fixturing.
import random

from golix import Ghid

from loopa.utils import make_background_future

# Intrapackage dependencies
from .hypothetical import public_api
from .hypothetical import fixture_api

from .exceptions import HandshakeError
from .exceptions import HandshakeWarning
from .exceptions import IPCError

from .exceptions import HGXLinkError
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
from .exceptions import CatOuttaBagError

from .utils import WeakSetMap
from .utils import SetMap
from .utils import ApiID
from .utils import weak_property

from .comms import RequestResponseAPI
from .comms import RequestResponseProtocol
from .comms import request

from .dispatch import _Dispatchable
from .dispatch import _DispatchableState
from .dispatch import _AppDef

# from .objproxy import ObjBase


# ###############################################
# Boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Inquisitor',
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
    b'\x00\x0A': RemoteNak,
    b'\xFF\xFF': IPCError
}


# Identity here can be either a sender or recipient dependent upon context
_ShareLog = collections.namedtuple(
    typename = '_ShareLog',
    field_names = ('ghid', 'identity'),
)


class _IPCSerializer:
    ''' This helper class defines the IPC serialization process.
    '''
        
    def _pack_object_def(self, address, author, state, is_link, api_id,
                         private, dynamic, _legroom):
        ''' Serializes an object definition.
        
        This is crude, but it's getting the job done for now. Also, for
        the record, I was previously using msgpack, but good lord is it
        slow.
        
        General format:
        version     1B      int16 unsigned
        address     65B     ghid
        author      65B     ghid
        private     1B      bool
        dynamic     1B      bool
        _legroom    1B      int8 unsigned
        api_id      65B     bytes
        is_link     1B      bool
        state       ?B      bytes (implicit length)
        '''
        version = b'\x00'
            
        if address is None:
            address = bytes(65)
        else:
            address = bytes(address)
        
        if author is None:
            author = bytes(65)
        else:
            author = bytes(author)
            
        private = bool(private).to_bytes(length=1, byteorder='big')
        dynamic = bool(dynamic).to_bytes(length=1, byteorder='big')
        if _legroom is None:
            _legroom = b'\x00'
        else:
            _legroom = int(_legroom).to_bytes(length=1, byteorder='big')
        if api_id is None:
            api_id = bytes(65)
        is_link = bool(is_link).to_bytes(length=1, byteorder='big')
        # State need not be modified
        
        return (version +
                address +
                author +
                private +
                dynamic +
                _legroom +
                bytes(api_id) +
                is_link +
                state)
        
    def _unpack_object_def(self, data):
        ''' Deserializes an object from bytes.
        
        General format:
        version     1B      int16 unsigned
        address     65B     ghid
        author      65B     ghid
        private     1B      bool
        dynamic     1B      bool
        _legroom    1B      int8 unsigned
        api_id      65B     bytes
        is_link     1B      bool
        state       ?B      bytes (implicit length)
        '''
        try:
            # version = data[0:1]
            address = data[1:66]
            author = data[66:131]
            private = data[131:132]
            dynamic = data[132:133]
            _legroom = data[133:134]
            api_id = ApiID.from_bytes(data[134:199])
            is_link = data[199:200]
            state = data[200:]
            
        except Exception:
            logger.error(
                'Unable to unpack IPC object definition w/ traceback:\n'
                ''.join(traceback.format_exc())
            )
            raise
            
        # Version stays unmodified (unused)
        if address == bytes(65):
            address = None
        else:
            address = Ghid.from_bytes(address)
        if author == bytes(65):
            author = None
        else:
            author = Ghid.from_bytes(author)
        private = bool(int.from_bytes(private, 'big'))
        dynamic = bool(int.from_bytes(dynamic, 'big'))
        _legroom = int.from_bytes(_legroom, 'big')
        if _legroom == 0:
            _legroom = None
        is_link = bool(int.from_bytes(is_link, 'big'))
        # state also stays unmodified
        
        return (address,
                author,
                state,
                is_link,
                api_id,
                private,
                dynamic,
                _legroom)


class IPCServerProtocol(_IPCSerializer, metaclass=RequestResponseAPI,
                        error_codes=ERROR_CODES, default_version=b'\x00\x00'):
    ''' Defines the protocol for IPC, with handlers specific to servers.
    '''
    _dispatch = weak_property('__dispatch')
    _oracle = weak_property('__oracle')
    _golcore = weak_property('__golcore')
    _rolodex = weak_property('__rolodex')
    _salmonator = weak_property('__salmonator')
        
    @fixture_api
    def __init__(self, whoami, *args, **kwargs):
        super(IPCServerProtocol.__fixture__, self).__init__(*args, **kwargs)
        self._whoami = whoami
        
    def assemble(self, golcore, oracle, dispatch, rolodex, salmonator):
        # Chicken, egg, etc.
        self._golcore = golcore
        self._oracle = oracle
        self._dispatch = dispatch
        self._rolodex = rolodex
        self._salmonator = salmonator
        
    @request(b'+T')
    async def set_token(self, connection, token=None):
        ''' Register an existing token or get a new token, or notify an
        app of its existing token.
        '''
        # On the server side, this will only be implemented once actual
        # application launching is available.
        raise NotImplementedError()
        
    @set_token.request_handler
    async def set_token(self, connection, body):
        ''' Handles token-setting requests.
        '''
        token = body[:4]
        
        # Getting a new token.
        if token == b'':
            token = await self._dispatch.start_application(connection)
            logger.info(''.join((
                'CONN ', str(connection), ' generating new token: ', str(token)
            )))
        
        # Setting an existing token, but the connection already exists.
        elif self._dispatch.token_lookup(connection):
            raise IPCError(
                'Attempt to reregister a new concurrent token for an ' +
                'existing connection. Each app may use only one token.'
            )
            
        # Setting an existing token, but the token already exists.
        elif self._dispatch.connection_lookup(token) is not None:
            raise IPCError(
                'Attempt to reregister a new concurrent connection for ' +
                'an existing token. Each app may only use one connection.'
            )
        
        # Setting an existing token, with valid state.
        else:
            logger.info(''.join((
                'CONN ', str(connection), ' registering existing token: ',
                str(token)
            )))
            await self._dispatch.start_application(connection, token)
        
        return token
        
    @request(b'+A')
    async def register_api(self, connection):
        ''' Registers the application as supporting an API. Client only.
        '''
        raise NotImplementedError()
        
    @register_api.request_handler
    async def register_api(self, connection, body):
        ''' Handles API registration requests. Server only.
        '''
        api_id = ApiID.from_bytes(body)
        self._dispatch.add_api(connection, api_id)
        
        return b'\x01'
        
    @request(b'-A')
    async def deregister_api(self, connection):
        ''' Removes any existing registration for the app supporting an
        API. Client only.
        '''
        raise NotImplementedError()
        
    @deregister_api.request_handler
    async def deregister_api(self, connection, body):
        ''' Handles API deregistration requests. Server only.
        '''
        api_id = ApiID.from_bytes(body)
        self._dispatch.remove_api(connection, api_id)
        
        return b'\x01'
        
    @public_api
    @request(b'?I')
    async def whoami(self, connection):
        ''' Get the current hypergolix fingerprint, or notify an app of
        the current hypergolix fingerprint.
        '''
        # On the server side, this will only be implemented once actual
        # application launching is available.
        raise NotImplementedError()
        
    @whoami.request_handler
    async def whoami(self, connection, body):
        ''' Handles whoami requests.
        '''
        ghid = self._golcore.whoami
        return bytes(ghid)
        
    @request(b'>$')
    async def get_startup_obj(self, connection):
        ''' Request a startup object, or notify an app of its declared
        startup object.
        '''
        token = self._dispatch.token_lookup(connection)
        ghid = await self._dispatch.get_startup_obj(token)
        
        if ghid is not None:
            return bytes(ghid)
        else:
            return b''
        
    @get_startup_obj.request_handler
    async def get_startup_obj(self, connection, body):
        ''' Handles requests for startup objects.
        '''
        token = self._dispatch.token_lookup(connection)
        ghid = await self._dispatch.get_startup_obj(token)
        
        if ghid is not None:
            return bytes(ghid)
        else:
            return b''
        
    @request(b'+$')
    async def register_startup_obj(self, connection, ghid):
        ''' Register a startup object. Client only.
        '''
        raise NotImplementedError()
        
    @register_startup_obj.request_handler
    async def register_startup_obj(self, connection, body):
        ''' Handles startup object registration. Server only.
        '''
        ghid = Ghid.from_bytes(body)
        await self._dispatch.register_startup(connection, ghid)
        return b'\x01'
        
    @request(b'-$')
    async def deregister_startup_obj(self, connection):
        ''' Register a startup object. Client only.
        '''
        raise NotImplementedError()
        
    @deregister_startup_obj.request_handler
    async def deregister_startup_obj(self, connection, body):
        ''' Handles startup object registration. Server only.
        '''
        await self._dispatch.deregister_startup(connection)
        return b'\x01'
        
    @request(b'>O')
    async def get_obj(self, connection):
        ''' Get an object with the specified address. Client only.
        '''
        raise NotImplementedError()
        
    @get_obj.request_handler
    async def get_obj(self, connection, body):
        ''' Handles requests for an object. Server only.
        '''
        ghid = Ghid.from_bytes(body)
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        
        self._dispatch.track_object(connection, ghid)
            
        if isinstance(obj.state, Ghid):
            is_link = True
            state = bytes(obj.state)
        else:
            is_link = False
            state = obj.state
            
        # For now, anyways.
        # Note: need to add some kind of handling for legroom.
        _legroom = None
        
        # Not a big fan of how this works, seems inelegant to me
        private = bool(self._dispatch.private_parent_lookup(obj.ghid))
        
        return self._pack_object_def(
            obj.ghid,
            obj.author,
            state,
            is_link,
            obj.api_id,
            private,
            obj.dynamic,
            _legroom
        )
        
    @request(b'+O')
    async def new_obj(self, connection):
        ''' Create a new object, or notify an app of a new object
        created by a concurrent instance of the app on a different
        hypergolix session.
        '''
        # Not currently supported.
        raise NotImplementedError()
        
    @new_obj.request_handler
    async def new_obj(self, connection, body):
        ''' Handles requests for new objects.
        '''
        (address,    # Unused and set to None.
         author,     # Unused and set to None.
         state,
         is_link,
         api_id,
         private,
         dynamic,
         legroom) = self._unpack_object_def(body)
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
        
        obj = await self._oracle.new_object(
            gaoclass = _Dispatchable,
            dispatch = self._dispatch,
            ipc_protocol = self,
            state = state,
            dynamic = dynamic,
            legroom = legroom,
            api_id = api_id,
        )
            
        # Add the endpoint as a listener.
        await self._dispatch.register_object(connection, obj.ghid, private)
        self._dispatch.track_object(connection, obj.ghid)
        
        return bytes(obj.ghid)
        
    @request(b'!O')
    async def update_obj(self, connection, ghid):
        ''' Update an object or notify an app of an incoming update.
        '''
        try:
            obj = await self._oracle.get_object(
                gaoclass = _Dispatchable,
                ghid = ghid,
                dispatch = self._dispatch,
                ipc_protocol = self
            )
            
        # No CancelledError catch necessary because we're re-raising any exc
            
        except Exception:
            # At some point we'll need some kind of proper handling for this.
            logger.error(
                'Failed to retrieve object at ' + str(ghid) + '\n' +
                ''.join(traceback.format_exc())
            )
            raise
            
        else:
            return self._pack_object_def(
                obj.ghid,
                obj.author,
                obj.state,
                False,      # is_link (currently unsupported)
                obj.api_id,
                None,       # private
                obj.dynamic,
                None        # legroom
            )
        
    @update_obj.request_handler
    async def update_obj(self, connection, body):
        ''' Handles update object requests.
        '''
        logger.debug('Handling update request from ' + str(connection))
        (ghid,
         author,    # Unused and set to None.
         state,
         is_link,
         api_id,    # Unused and set to None.
         private,   # TODO: use this.
         dynamic,   # Unused and set to None.
         legroom   # TODO: use this.
         ) = self._unpack_object_def(body)
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
            
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_protocol = self
        )
        obj.state = state
        
        # Converting a private object to a public one
        if self._dispatch.private_parent_lookup(ghid):
            if not private:
                self._dispatch.make_public(ghid)
        
        else:
            if private:
                raise CatOuttaBagError(
                    'Once made public, objects cannot be made private. They ' +
                    'have already been distributed to other applications.'
                )
            
        await obj.push()
        
        # Schedule an update in the background.
        make_background_future(
            self._dispatch.distribute_update(
                obj.ghid,
                skip_conn = connection
            )
        )
        
        return b'\x01'
        
    @request(b'~O')
    async def sync_obj(self, connection):
        ''' Manually force Hypergolix to check an object for updates.
        Client only.
        '''
        raise NotImplementedError()
        
    @sync_obj.request_handler
    async def sync_obj(self, connection, body):
        ''' Handles manual syncing requests. Server only.
        '''
        ghid = Ghid.from_bytes(body)
        await self._salmonator.attempt_pull(ghid)
        return b'\x01'
        
    @request(b'@O')
    async def share_obj(self, connection, ghid, origin, api_id):
        ''' Request an object share or notify an app of an incoming
        share.
        '''
        return bytes(ghid) + bytes(origin) + bytes(api_id)
        
    @share_obj.request_handler
    async def share_obj(self, connection, body):
        ''' Handles object share requests.
        '''
        ghid = Ghid.from_bytes(body[0:65])
        recipient = Ghid.from_bytes(body[65:130])
        
        # Instead of forbidding unregistered apps from sharing objects,
        # go for it, but document that you will never be notified of a
        # share success or failure without an app token.
        requesting_token = self._dispatch.token_lookup(connection)
        if requesting_token is None:
            logger.info(
                'CONN ' + str(connection) + ' is sharing ' + str(ghid) +
                ' with ' + str(recipient) + ' without defining an app ' +
                'token, and therefore cannot be notified of share success ' +
                'or failure.'
            )
            
        await self._rolodex.share_object(ghid, recipient, requesting_token)
        return b'\x01'
        
    @request(b'^S')
    async def notify_share_success(self, connection, ghid, recipient):
        ''' Notify app of successful share. Server only.
        '''
        return bytes(ghid) + bytes(recipient)
        
    @notify_share_success.request_handler
    async def notify_share_success(self, connection, body):
        ''' Handles app notifications for successful shares. Client
        only.
        '''
        raise NotImplementedError()
        
    @request(b'^F')
    async def notify_share_failure(self, connection, ghid, recipient):
        ''' Notify app of unsuccessful share. Server only.
        '''
        return bytes(ghid) + bytes(recipient)
        
    @notify_share_failure.request_handler
    async def notify_share_failure(self, connection, body):
        ''' Handles app notifications for unsuccessful shares. Client
        only.
        '''
        raise NotImplementedError()
        
    @request(b'*O')
    async def freeze_obj(self, connection):
        ''' Creates a new static copy of the object, or notifies an app
        of a frozen copy of an existing object created by a concurrent
        instance of the app.
        '''
        # Not currently supported.
        raise NotImplementedError()
        
    @freeze_obj.request_handler
    async def freeze_obj(self, connection, body):
        ''' Handles object freezing requests.
        '''
        ghid = Ghid.from_bytes(body)
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_protocol = self
        )
        frozen_address = await obj.freeze()
        
        return bytes(frozen_address)
        
    @request(b'#O')
    async def hold_obj(self, connection):
        ''' Creates a new static binding for the object, or notifies an
        app of a static binding created by a concurrent instance of the
        app.
        '''
        # Not currently supported.
        raise NotImplementedError()
        
    @hold_obj.request_handler
    async def hold_obj(self, connection, body):
        ''' Handles object holding requests.
        '''
        ghid = Ghid.from_bytes(body)
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_protocol = self
        )
        await obj.hold()
        return b'\x01'
        
    @request(b'-O')
    async def discard_obj(self, connection):
        ''' Stop listening to object updates. Client only.
        '''
        raise NotImplementedError()
        
    @discard_obj.request_handler
    async def discard_obj(self, connection, body):
        ''' Handles object discarding requests. Server only.
        '''
        ghid = Ghid.from_bytes(body)
        self._dispatch.untrack_object(connection, ghid)
        return b'\x01'
        
    @request(b'XO')
    async def delete_obj(self, connection, ghid):
        ''' Request an object deletion or notify an app of an incoming
        deletion.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('ghid must be type Ghid or similar.')
        
        return bytes(ghid)
        
    @delete_obj.request_handler
    async def delete_obj(self, connection, body):
        ''' Handles object deletion requests.
        '''
        ghid = Ghid.from_bytes(body)
        obj = await self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_protocol = self
        )
        self._dispatch.untrack_object(connection, ghid)
        # TODO: shift to a dispatch.delete_object method that can ignore this
        # connection when the intevitable "deleted that object!" warning comes
        # back
        await obj.delete()
        return b'\x01'


class IPCClientProtocol(_IPCSerializer, metaclass=RequestResponseAPI,
                        error_codes=ERROR_CODES, default_version=b'\x00\x00'):
    ''' Defines the protocol for IPC, with handlers specific to clients.
    '''
    _hgxlink = weak_property('__hgxlink')
        
    @fixture_api
    def __init__(self, whoami, *args, **kwargs):
        ''' Create the fixture internals.
        '''
        # This is necessary because of the fixture_bases bit.
        super(IPCClientProtocol.__fixture__, self).__init__(*args, **kwargs)
        self.whoami = whoami
        self.apis = set()
        self.token = None
        self.startup = None
        self.pending_obj = None
        self.pending_ghid = None
        self.discarded = set()
        self.updates = []
        self.syncs = []
        self.shares = SetMap()
        self.frozen = set()
        self.held = set()
        self.deleted = set()
        
    @fixture_api
    def RESET(self):
        ''' Nothing beyond just re-running __init__, reusing whoami.
        '''
        self.apis = set()
        self.token = None
        self.startup = None
        self.pending_obj = None
        self.pending_ghid = None
        self.discarded = set()
        self.updates = []
        self.syncs = []
        self.shares = SetMap()
        self.frozen = set()
        self.held = set()
        self.deleted = set()
        
    @fixture_api
    def prep_obj(self, obj):
        ''' Define the next object to be returned in any obj-based
        operations.
        '''
        self.pending_obj = (
            obj._hgx_ghid,
            obj._hgx_binder,
            obj._hgx_state,
            obj._hgx_linked,
            obj._hgx_api_id,
            obj._hgx_private,
            obj._hgx_dynamic,
            obj._hgx_legroom
        )
        
    def assemble(self, hgxlink):
        # Chicken, egg, etc.
        self._hgxlink = hgxlink
        
    @public_api
    @request(b'+T')
    async def set_token(self, connection, token):
        ''' Register an existing token or get a new token, or notify an
        app of its existing token.
        '''
        if token is None:
            return b''
        else:
            return token
        
    @set_token.request_handler
    async def set_token(self, connection, body):
        ''' Handles token-setting requests.
        '''
        self._hgxlink.token = body
        
    @set_token.fixture
    async def set_token(self, token):
        ''' Fixture for setting a token (or getting a new one).
        '''
        if token is None:
            token = bytes([random.randint(0, 255) for i in range(0, 4)])
            
        self.token = token
        return token
    
    @public_api
    @request(b'+A')
    async def register_api(self, connection, api_id):
        ''' Registers the application as supporting an API. Client only.
        '''
        if not isinstance(api_id, ApiID):
            raise TypeError('api_id must be ApiID.')
            
        return bytes(api_id)
        
    @register_api.request_handler
    async def register_api(self, connection, body):
        ''' Handles API registration requests. Server only.
        '''
        raise NotImplementedError()
        
    @register_api.response_handler
    async def register_api(self, connection, response, exc):
        ''' Handles responses to API registration requests. Client only.
        '''
        if exc is not None:
            raise exc
        elif response == b'\x01':
            return True
        else:
            raise IPCError('Unknown error while registering API.')
            
    @register_api.fixture
    async def register_api(self, api_id):
        ''' Fixture for registering an api.
        '''
        self.apis.add(api_id)
        
    @public_api
    @request(b'-A')
    async def deregister_api(self, connection, api_id):
        ''' Removes any existing registration for the app supporting an
        API. Client only.
        '''
        if not isinstance(api_id, ApiID):
            raise TypeError('api_id must be ApiID.')
            
        return bytes(api_id)
        
    @deregister_api.request_handler
    async def deregister_api(self, connection, body):
        ''' Handles API deregistration requests. Server only.
        '''
        raise NotImplementedError()
        
    @deregister_api.response_handler
    async def deregister_api(self, connection, response, exc):
        ''' Handles responses to API deregistration requests. Client
        only.
        '''
        if exc is not None:
            raise exc
        elif response == b'\x01':
            return True
        else:
            raise IPCError('Unknown error while deregistering API.')
            
    @deregister_api.fixture
    async def deregister_api(self, api_id):
        ''' Fixture for api removal.
        '''
        self.apis.discard(api_id)
        
    @public_api
    @request(b'?I')
    async def get_whoami(self, connection):
        ''' Get the current hypergolix fingerprint, or notify an app of
        the current hypergolix fingerprint.
        '''
        return b''
        
    @get_whoami.request_handler
    async def get_whoami(self, connection, body):
        ''' Handles whoami requests.
        '''
        ghid = Ghid.from_bytes(body)
        self._hgxlink.whoami = ghid
        return b''
        
    @get_whoami.response_handler
    async def get_whoami(self, connection, response, exc):
        ''' Handles responses to whoami requests.
        '''
        if exc is not None:
            raise exc
        else:
            ghid = Ghid.from_bytes(response)
            return ghid
            
    @get_whoami.fixture
    async def get_whoami(self, connection):
        ''' Fixture for get_whoami.
        '''
        return self.whoami
        
    @public_api
    @request(b'>$')
    async def get_startup_obj(self, connection):
        ''' Request a startup object, or notify an app of its declared
        startup object.
        '''
        return b''
        
    @get_startup_obj.request_handler
    async def get_startup_obj(self, connection, body):
        ''' Handles requests for startup objects.
        '''
        ghid = Ghid.from_bytes(body)
        self._hgxlink._startup_obj = ghid
        return b'\x01'
        
    @get_startup_obj.response_handler
    async def get_startup_obj(self, connection, response, exc):
        ''' Handle the response to retrieving startup obj ghids.
        '''
        if exc is not None:
            raise exc
        elif response == b'':
            return None
        else:
            ghid = Ghid.from_bytes(response)
            return ghid
            
    @get_startup_obj.fixture
    async def get_startup_obj(self):
        ''' Yep, go ahead and fixture that, too.
        '''
        return self.startup
        
    @public_api
    @request(b'+$')
    async def register_startup_obj(self, connection, ghid):
        ''' Register a startup object. Client only.
        '''
        return bytes(ghid)
        
    @register_startup_obj.request_handler
    async def register_startup_obj(self, connection, body):
        ''' Handles startup object registration. Server only.
        '''
        raise NotImplementedError()
        
    @register_startup_obj.fixture
    async def register_startup_obj(self, ghid):
        ''' Fixture startup obj registration and stuff.
        '''
        self.startup = ghid
        
    @public_api
    @request(b'-$')
    async def deregister_startup_obj(self, connection):
        ''' Register a startup object. Client only.
        '''
        return b''
        
    @deregister_startup_obj.request_handler
    async def deregister_startup_obj(self, connection, body):
        ''' Handles startup object registration. Server only.
        '''
        raise NotImplementedError()
        
    @deregister_startup_obj.fixture
    async def deregister_startup_obj(self):
        ''' Still more fixtures.
        '''
        self.startup = None
        
    @public_api
    @request(b'>O')
    async def get_ghid(self, connection, ghid):
        ''' Get an object with the specified address. Client only.
        '''
        return bytes(ghid)
        
    @get_ghid.request_handler
    async def get_ghid(self, connection, body):
        ''' Handles requests for an object. Server only.
        '''
        raise NotImplementedError()
        
    @get_ghid.response_handler
    async def get_ghid(self, connection, response, exc):
        ''' Handles responses to get object requests. Client only.
        '''
        if exc is not None:
            raise exc
            
        return self._unpack_object_def(response)
        
    @get_ghid.fixture
    async def get_ghid(self, ghid):
        ''' Interact with pending_obj.
        '''
        return self.pending_obj
        
    @public_api
    @request(b'+O')
    async def new_ghid(self, connection, state, api_id, dynamic, private,
                       _legroom):
        ''' Create a new object, or notify an app of a new object
        created by a concurrent instance of the app on a different
        hypergolix session.
        '''
        # If state is Ghid, it's a link.
        if isinstance(state, Ghid):
            is_link = True
        else:
            is_link = False
        
        return self._pack_object_def(
            None,               # address
            None,               # author
            state,              # state
            is_link,            # is_link
            bytes(api_id),      # api_id
            private,            # private
            dynamic,            # dynamic
            _legroom            # legroom
        )
        
    @new_ghid.request_handler
    async def new_ghid(self, connection, body):
        ''' Handles requests for new objects.
        '''
        raise NotImplementedError()
        
    @new_ghid.response_handler
    async def new_ghid(self, connection, response, exc):
        ''' Handles responses to requests for new objects.
        '''
        if exc is not None:
            raise exc
        
        else:
            return Ghid.from_bytes(response)
            
    @new_ghid.fixture
    async def new_ghid(self, state, api_id, dynamic, private, _legroom):
        ''' We just need an address.
        '''
        self.pending_obj = (
            self.pending_ghid,
            self.whoami,
            state,
            False,  # is_link
            api_id,
            private,
            _legroom
        )
        
        return self.pending_ghid
        
    @public_api
    @request(b'!O')
    async def update_ghid(self, connection, ghid, state, private, _legroom):
        ''' Update an object or notify an app of an incoming update.
        '''
        # If state is Ghid, it's a link.
        if isinstance(state, Ghid):
            is_link = True
        else:
            is_link = False
            
        return self._pack_object_def(
            ghid,       # ghid
            None,       # Author
            state,      # state
            is_link,    # is_link
            None,       # api_id
            private,    # private
            None,       # dynamic
            _legroom    # legroom
        )
        
    @update_ghid.request_handler
    async def update_ghid(self, connection, body):
        ''' Handles update object requests.
        '''
        (address,
         author,    # Will be unused and set to None
         state,
         is_link,
         api_id,    # Will be unused and set to None
         private,   # Will be unused and set to None
         dynamic,   # Will be unused and set to None
         _legroom   # Will be unused and set to None
         ) = self._unpack_object_def(body)
        
        if is_link:
            state = Ghid.from_bytes(state)
            
        await self._hgxlink._pull_state(address, state)
            
        return b'\x01'
        
    @update_ghid.response_handler
    async def update_ghid(self, connection, response, exc):
        ''' Handles responses to update object requests.
        '''
        if exc is not None:
            raise exc
            
        elif response != b'\x01':
            raise HGXLinkError('Unknown error while updating object.')
            
        else:
            return True
            
    @update_ghid.fixture
    async def update_ghid(self, ghid, state, private, _legroom):
        ''' Yarp, fixture that.
        '''
        self.updates.append(
            {ghid: (state, private, _legroom)}
        )
        
    @public_api
    @request(b'~O')
    async def sync_ghid(self, connection, ghid):
        ''' Manually force Hypergolix to check an object for updates.
        Client only.
        '''
        return bytes(ghid)
        
    @sync_ghid.request_handler
    async def sync_ghid(self, connection, body):
        ''' Handles manual syncing requests. Server only.
        '''
        raise NotImplementedError()
        
    @sync_ghid.response_handler
    async def sync_ghid(self, connection, response, exc):
        ''' Handles responses to manual syncing requests. Client only.
        '''
        if exc is not None:
            raise exc
        elif response != b'\x01':
            raise IPCError('Unknown error while updating object.')
        else:
            return True
            
    @sync_ghid.fixture
    async def sync_ghid(self, ghid):
        # Moar fixturing.
        self.syncs.append(ghid)
        
    @public_api
    @request(b'@O')
    async def share_ghid(self, connection, ghid, recipient):
        ''' Request an object share or notify an app of an incoming
        share.
        '''
        return bytes(ghid) + bytes(recipient)
        
    @share_ghid.request_handler
    async def share_ghid(self, connection, body):
        ''' Handles object share requests.
        '''
        ghid = Ghid.from_bytes(body[0:65])
        origin = Ghid.from_bytes(body[65:130])
        api_id = ApiID.from_bytes(body[130:195])
        
        await self._hgxlink.handle_share(ghid, origin, api_id)
        return b'\x01'
        
    @share_ghid.response_handler
    async def share_ghid(self, connection, response, exc):
        ''' Handles responses to object share requests.
        '''
        if exc is not None:
            raise exc
        elif response == b'\x01':
            return True
        else:
            raise IPCError('Unknown error while sharing object.')
            
    @share_ghid.fixture
    async def share_ghid(self, ghid, recipient):
        # Blahblah
        self.shares.add(ghid, recipient)
        
    @request(b'^S')
    async def share_success(self, connection):
        ''' Notify app of successful share. Server only.
        '''
        raise NotImplementedError()
        
    @share_success.request_handler
    async def share_success(self, connection, body):
        ''' Handles app notifications for successful shares. Client
        only.
        '''
        # Don't raise NotImplementedError, just because this will be called for
        # every share, and we're perfectly well aware that it isn't implemented
        # TODO: implement this.
        return b''
        
    @request(b'^F')
    async def share_failure(self, connection):
        ''' Notify app of unsuccessful share. Server only.
        '''
        raise NotImplementedError()
        
    @share_failure.request_handler
    async def share_failure(self, connection, body):
        ''' Handles app notifications for unsuccessful shares. Client
        only.
        '''
        # Don't raise NotImplementedError, just because this will be called for
        # every share, and we're perfectly well aware that it isn't implemented
        # TODO: implement this.
        return b''
        
    @public_api
    @request(b'*O')
    async def freeze_ghid(self, connection, ghid):
        ''' Creates a new static copy of the object, or notifies an app
        of a frozen copy of an existing object created by a concurrent
        instance of the app.
        '''
        return bytes(ghid)
        
    @freeze_ghid.request_handler
    async def freeze_ghid(self, connection, body):
        ''' Handles object freezing requests.
        '''
        # Not currently supported.
        raise NotImplementedError()
        
    @freeze_ghid.response_handler
    async def freeze_ghid(self, connection, response, exc):
        ''' Handles responses to object freezing requests.
        '''
        if exc is not None:
            raise exc
        
        else:
            return Ghid.from_bytes(response)
            
    @freeze_ghid.fixture
    async def freeze_ghid(self, ghid):
        # Moar fixture.
        self.frozen.add(ghid)
        return ghid
        
    @public_api
    @request(b'#O')
    async def hold_ghid(self, connection, ghid):
        ''' Creates a new static binding for the object, or notifies an
        app of a static binding created by a concurrent instance of the
        app.
        '''
        return bytes(ghid)
        
    @hold_ghid.request_handler
    async def hold_ghid(self, connection, body):
        ''' Handles object holding requests.
        '''
        # Not currently supported.
        raise NotImplementedError()
        
    @hold_ghid.response_handler
    async def hold_ghid(self, connection, response, exc):
        ''' Handles responses to object holding requests.
        '''
        if exc is not None:
            raise exc
        elif response == b'\x01':
            return True
        else:
            raise IPCError('Unknown error while holding object.')
            
    @hold_ghid.fixture
    async def hold_ghid(self, ghid):
        # Yep yep yep
        self.held.add(ghid)
        
    @public_api
    @request(b'-O')
    async def discard_ghid(self, connection, ghid):
        ''' Stop listening to object updates. Client only.
        '''
        return bytes(ghid)
        
    @discard_ghid.request_handler
    async def discard_ghid(self, connection, body):
        ''' Handles object discarding requests. Server only.
        '''
        raise NotImplementedError()
        
    @discard_ghid.response_handler
    async def discard_ghid(self, connection, response, exc):
        ''' Handles responses to object discarding requests. Client
        only.
        '''
        if exc is not None:
            raise exc
        elif response == b'\x01':
            return True
        else:
            raise IPCError('Unknown error while discarding object.')
            
    @discard_ghid.fixture
    async def discard_ghid(self, ghid):
        # Nothing special.
        self.discarded.add(ghid)
        
    @public_api
    @request(b'XO')
    async def delete_ghid(self, connection, ghid):
        ''' Request an object deletion or notify an app of an incoming
        deletion.
        '''
        return bytes(ghid)
        
    @delete_ghid.request_handler
    async def delete_ghid(self, connection, body):
        ''' Handles object deletion requests.
        '''
        ghid = Ghid.from_bytes(body)
        await self._hgxlink.handle_delete(ghid)
        return b'\x01'
        
    @delete_ghid.response_handler
    async def delete_ghid(self, connection, response, exc):
        ''' Handles responses to object deletion requests.
        '''
        if exc is not None:
            raise exc
        elif response == b'\x01':
            return True
        else:
            raise IPCError('Unknown error while deleting object.')
            
    @delete_ghid.fixture
    async def delete_ghid(self, ghid):
        # mmmmhmmm
        self.deleted.add(ghid)
