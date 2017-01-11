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
import logging
import collections
import traceback

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
from .utils import AppToken
from .utils import weak_property

from .comms import RequestResponseAPI
from .comms import RequestResponseProtocol
from .comms import request

# from .objproxy import ObjBase


# ###############################################
# Boilerplate, etc
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports.
__all__ = [
    # 'Inquisitor',
]


_ShareLog = collections.namedtuple(
    typename = '_ShareLog',
    field_names = ('connection', 'ghid', 'origin', 'api_id')
)


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
            return AppToken.null()
        else:
            return token
        
    @set_token.request_handler
    async def set_token(self, connection, body):
        ''' Handles token-setting requests.
        '''
        self._hgxlink.token = AppToken(body)
        
    @set_token.fixture
    async def set_token(self, token):
        ''' Fixture for setting a token (or getting a new one).
        '''
        if token is None:
            # ONLY use pseudorandom for the fixture
            token = AppToken.pseudorandom()
            
        self.token = token
        return token
        
    @set_token.response_handler
    async def set_token(self, connection, response, exc):
        ''' Converts the response into an AppToken.
        '''
        if exc is not None:
            raise exc
        else:
            return AppToken(response)
    
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
