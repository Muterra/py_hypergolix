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
import weakref
import collections

from golix import Ghid

import concurrent
import asyncio
import traceback

# Intrapackage dependencies
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

from .utils import call_coroutine_threadsafe
from .utils import WeakSetMap
from .utils import ApiID

from .comms import WSConnection as _AutoresponderSession
from .comms import RequestResponseProtocol as Autoresponder
from .comms import MsgBuffer as AutoresponseConnector

from .comms import RequestResponseProtocol
from .comms import request

from .dispatch import _Dispatchable
from .dispatch import _DispatchableState
from .dispatch import _AppDef

from .objproxy import ObjBase


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
                api_id +
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
            api_id = data[134:199]
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
        if api_id == bytes(65):
            api_id = None
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


class IPCServerProtocol(_IPCSerializer, metaclass=RequestResponseProtocol,
                        error_codes=ERROR_CODES, default_version=b'\x00\x00'):
    ''' Defines the protocol for IPC, with handlers specific to servers.
    '''
    
    def __init__(self, *args, **kwargs):
        ''' Add intentionally invalid init to force assemblage.
        '''
        super().__init__(*args, **kwargs)
        
        self._dispatch = None
        self._oracle = None
        self._golcore = None
        self._rolodex = None
        self._salmonator = None
        
    def assemble(self, golix_core, oracle, dispatch, rolodex, salmonator):
        # Chicken, egg, etc.
        self._golcore = weakref.proxy(golix_core)
        self._oracle = weakref.proxy(oracle)
        self._dispatch = weakref.proxy(dispatch)
        self._rolodex = weakref.proxy(rolodex)
        self._salmonator = weakref.proxy(salmonator)
        
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
            token = self._dispatch.start_application(connection)
            logger.info(''.join((
                'CONN ', str(connection), ' generating new token: ', str(token)
            )))
        
        # Setting an existing token, but the connection already exists.
        elif self._dispatch.which_token(connection):
            raise IPCError(
                'Attempt to reregister a new concurrent token for an ' +
                'existing connection. Each app may use only one token.'
            )
            
        # Setting an existing token, but the token already exists.
        elif self._dispatch.which_connection(token):
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
            self._dispatch.start_application(connection, token)
        
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
        token = self._dispatch.which_token(connection)
        ghid = self._dispatch.get_startup_obj(token)
        
        if ghid is not None:
            return bytes(ghid)
        else:
            return b''
        
    @get_startup_obj.request_handler
    async def get_startup_obj(self, connection, body):
        ''' Handles requests for startup objects.
        '''
        token = self._dispatch.which_token(connection)
        ghid = self._dispatch.get_startup_obj(token)
        
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
        self._dispatch.register_startup(connection, ghid)
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
        obj = self._oracle.get_object(
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
        
        return self._pack_object_def(
            obj.ghid,
            obj.author,
            state,
            is_link,
            obj.api_id,
            obj.private,
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
         _legroom) = self._unpack_object_def(body)
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
        
        obj = self._oracle.new_object(
            gaoclass = _Dispatchable,
            dispatch = self._dispatch,
            ipc_core = self,
            state = _DispatchableState(api_id, state),
            dynamic = dynamic,
            _legroom = _legroom,
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
            obj = self._oracle.get_object(
                gaoclass = _Dispatchable,
                ghid = ghid,
                dispatch = self._dispatch,
                ipc_core = self
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
                False, # is_link is currently unsupported
                obj.api_id,
                None,
                obj.dynamic,
                None
            )
        
    @update_obj.request_handler
    async def update_obj(self, connection, body):
        ''' Handles update object requests.
        '''
        logger.debug('Handling update request from ' + str(connection))
        (address,
         author,    # Unused and set to None.
         state,
         is_link,
         api_id,    # Unused and set to None.
         private,   # TODO: use this.
         dynamic,   # Unused and set to None.
         _legroom   # TODO: use this.
         ) = self._unpack_object_def(body)
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
            
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = address,
            dispatch = self._dispatch,
            ipc_core = self
        )
        obj.update(state)
        
        await self._dispatch.schedule_update_distribution(
            obj.ghid,
            skip_conn = connection
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
    async def share_obj(self, connection, ghid, origin):
        ''' Request an object share or notify an app of an incoming
        share.
        '''
        return bytes(ghid) + bytes(origin)
        
    @share_obj.request_handler
    async def share_obj(self, connection, body):
        ''' Handles object share requests.
        '''
        ghid = Ghid.from_bytes(body[0:65])
        recipient = Ghid.from_bytes(body[65:130])
        
        # Instead of forbidding unregistered apps from sharing objects,
        # go for it, but document that you will never be notified of a
        # share success or failure without an app token.
        requesting_token = self._dispatch.which_token(connection)
        if requesting_token is None:
            logger.info(
                (
                    'CONN {!s} is sharing {!s} with {!s} without defining ' +
                    'an app token, and therefore cannot be notified of ' +
                    'share success or failure.'
                ).format(connection, ghid, recipient)
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
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        frozen_address = obj.freeze()
        
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
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        obj.hold()
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
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        self._dispatch.untrack_object(connection, ghid)
        obj.delete()
        return b'\x01'


class IPCClientProtocol(_IPCSerializer, metaclass=RequestResponseProtocol,
                        error_codes=ERROR_CODES, default_version=b'\x00\x00'):
    ''' Defines the protocol for IPC, with handlers specific to clients.
    '''
    
    def __init__(self, *args, **kwargs):
        ''' Add intentionally invalid init to force assemblage.
        '''
        super().__init__(*args, **kwargs)
        self._hgxlink = None
        
    def assemble(self, hgxlink):
        # Chicken, egg, etc.
        self._hgxlink = weakref.proxy(hgxlink)
        
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
        self._hgxlink.app_token = body
        
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
        
        await self._hgxlink.handle_share(ghid, origin)
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


class IPCCore:
    ''' The core IPC system, including the server autoresponder. Add the
    individual IPC servers to the IPC Core.
    
    NOTE: this class, with the exception of initialization, is wholly
    asynchronous. Outside entities should call into it using
    utils.call_coroutine_threadsafe. Any thread-wrapping that needs to
    happen to break in-loop chains should also be executed in the
    outside entity.
    '''
    REQUEST_CODES = {
        # Receive a declared startup obj.
        'send_startup': b':O',
        # Receive a new object from a remotely concurrent instance of self.
        'send_object': b'+O',
        # Receive an update for an existing object.
        'send_update': b'!O',
        # Receive an update that an object has been deleted.
        'send_delete': b'XO',
        # Receive an object that was just shared with us.
        'send_share': b'^O',
        # Receive an async notification of a sharing failure.
        'notify_share_failure': b'^F',
        # Receive an async notification of a sharing success.
        'notify_share_success': b'^S',
    }
    
    def __init__(self, *args, **kwargs):
        ''' Initialize the autoresponder and get it ready to go.
        '''
        self._dispatch = None
        self._oracle = None
        self._golcore = None
        self._rolodex = None
        self._salmonator = None
        
        # Some distributed objects to be bootstrapped
        # Set of incoming shared ghids that had no endpoint
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = None
        # Setmap-like lookup for share acks that had no endpoint
        # <app token>: set(<ghids>)
        self._orphan_share_acks = None
        # Setmap-like lookup for share naks that had no endpoint
        # <app token>: set(<ghids>)
        self._orphan_share_naks = None
        
        # Lookup <server_name>: <server>
        self._ipc_servers = {}
        
        # Lookup <app token>: <connection/session/endpoint>
        self._endpoint_from_token = weakref.WeakValueDictionary()
        # Reverse lookup <connection/session/endpoint>: <app token>
        self._token_from_endpoint = weakref.WeakKeyDictionary()
        
        # Lookup <api ID>: set(<connection/session/endpoint>)
        self._endpoints_from_api = WeakSetMap()
        
        # This lookup directly tracks who has a copy of the object
        # Lookup <object ghid>: set(<connection/session/endpoint>)
        self._update_listeners = WeakSetMap()
        
        req_handlers = {
            # Get new app token
            b'+T': self.new_token_wrapper,
            # Register existing app token
            b'$T': self.set_token_wrapper,
            # Register an API
            b'$A': self.add_api_wrapper,
            # Deegister an API
            b'XA': self.remove_api_wrapper,
            # Register a startup object
            b'$O': self.register_startup_wrapper,
            # Whoami?
            b'?I': self.whoami_wrapper,
            # Get object
            b'>O': self.get_object_wrapper,
            # New object
            b'+O': self.new_object_wrapper,
            # Sync object
            b'~O': self.sync_object_wrapper,
            # Update object
            b'!O': self.update_object_wrapper,
            # Share object
            b'@O': self.share_object_wrapper,
            # Freeze object
            b'*O': self.freeze_object_wrapper,
            # Hold object
            b'#O': self.hold_object_wrapper,
            # Discard object
            b'-O': self.discard_object_wrapper,
            # Delete object
            b'XO': self.delete_object_wrapper,
        }
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            *args, **kwargs
        )
        
    def assemble(self, golix_core, oracle, dispatch, rolodex, salmonator):
        # Chicken, egg, etc.
        self._golcore = weakref.proxy(golix_core)
        self._oracle = weakref.proxy(oracle)
        self._dispatch = weakref.proxy(dispatch)
        self._rolodex = weakref.proxy(rolodex)
        self._salmonator = weakref.proxy(salmonator)
        
    def bootstrap(self, incoming_shares, orphan_acks, orphan_naks):
        ''' Initializes distributed state.
        '''
        # Set of incoming shared ghids that had no endpoint
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = incoming_shares
        # Setmap-like lookup for share acks that had no endpoint
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_acks = orphan_acks
        # Setmap-like lookup for share naks that had no endpoint
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_naks = orphan_naks
        
    def add_ipc_server(self, server_name, server_class, *args, **kwargs):
        ''' Automatically sets up an IPC server connected to the IPCCore
        system. Just give it the server_class, eg WSBasicServer, and
        all of the *args and **kwargs will be passed to the server's
        __init__.
        '''
        if server_name in self._ipc_servers:
            raise ValueError(
                'Cannot overwrite an existing IPC server. Pop it first.'
            )
        
        # We could maybe do this elsewhere, but adding an IPC server isn't 
        # really performance-critical, especially not now.
        class LinkedServer(AutoresponseConnector, server_class):
            pass
            
        self._ipc_servers[server_name] = \
            LinkedServer(autoresponder=self, *args, **kwargs)

    def pop_ipc_server(self, server_name):
        ''' Removes and returns the IPC server. It may then be cleanly 
        shut down (manually).
        '''
        self._ipc_servers.pop(server_name)
        
    async def notify_update(self, ghid, deleted=False):
        ''' Updates all ipc endpoints with copies of the object.
        '''
        callsheet = self._update_listeners.get_any(ghid)
        
        # Go ahead and distribute it to the appropriate endpoints.
        if deleted:
            await self.distribute_to_endpoints(
                callsheet,
                self.send_delete,
                ghid
            )
        else:
            await self.distribute_to_endpoints(
                callsheet,
                self.send_update,
                ghid
            )
            
    async def process_share(self, target, sender):
        ''' Manage everything about processing incoming shares.
        '''
        # Build a callsheet for the target.
        callsheet = await self._make_callsheet(target)
        # Go ahead and distribute it to the appropriate endpoints.
        await self.distribute_to_endpoints(
            callsheet,
            self.send_share,
            target,
            sender
        )
        
    async def process_share_success(self, target, recipient, tokens):
        ''' Wrapper to notify all requestors of share success.
        
        Note that tokens will only include applications that have a 
        declared app token.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._endpoint_from_token[token])
            except KeyError:
                logger.debug('Missing endpoint for token ' + str(token))
        
        # Distribute the share success to all apps that requested its delivery
        await self._robodialer(
            self.notify_share_success,
            callsheet,
            target,
            recipient
        )
    
    async def process_share_failure(self, target, recipient, tokens):
        ''' Wrapper to notify all requestors of share failure.
        
        Note that tokens will only include applications that have a
        declared app token.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._endpoint_from_token[token])
            except KeyError:
                logger.debug('Missing endpoint for token ' + str(token))
        
        # Distribute the share success to all apps that requested its delivery
        await self._robodialer(
            self.notify_share_failure,
            callsheet,
            target,
            recipient
        )
    
    async def _make_callsheet(self, ghid, skip_endpoint=None):
        ''' Generates a callsheet (set of tokens) for the dispatchable
        obj.
        
        The callsheet is generated from app_tokens, so that the actual
        distributor can kick missing tokens back for safekeeping.
        
        TODO: make this a "private" method -- aka, remove this from the
        rolodex share handling.
        TODO: make this exclusively apply to object sharing, NOT to obj
        updates, which uses _update_listeners directly and exclusively.
        '''
        try:
            obj = self._oracle.get_object(
                gaoclass = _Dispatchable,
                ghid = ghid,
                dispatch = self._dispatch,
                ipc_core = self
            )
        
        except Exception:
            # At some point we'll need some kind of proper handling for this.
            logger.error(
                'Failed to retrieve object at ' + str(ghid) + '\n' +
                ''.join(traceback.format_exc())
            )
            return set()
        
        # Create a temporary set for relevant endpoints
        callsheet = set()
        
        private_owner = self._dispatch.get_parent_token(ghid)
        if private_owner:
            try:
                private_endpoint = self._endpoint_from_token[private_owner]
            except KeyError:
                logger.warning(
                    'Could not retrieve the object\'s private owner, with '
                    'traceback: \n' + ''.join(traceback.format_exc())
                )
            else:
                callsheet.add(private_endpoint)
            
        else:
            # Add any endpoints based on their tracking of the api.
            logger.debug(
                'Object has no private owner; generating list of approx. ' + 
                str(len(self._endpoints_from_api.get_any(obj.api_id))) + 
                ' interested API endpoints.'
            )
            callsheet.update(self._endpoints_from_api.get_any(obj.api_id))
            
            # Add any endpoints based on their existing listening status.
            logger.debug(
                'Adding an additional approx ' + 
                str(len(self._update_listeners.get_any(obj.ghid))) + 
                ' explicit object listeners.'
            )
            callsheet.update(self._update_listeners.get_any(obj.ghid))
            
        # And discard any skip_endpoint, if it's there.
        callsheet.discard(skip_endpoint)
        
        logger.debug('Callsheet generated: ' + repr(callsheet))
        
        return callsheet
        
    async def distribute_to_endpoints(self, callsheet, distributor, *args):
        ''' For each app token in the callsheet, awaits the distributor,
        passing it the endpoint and *args.
        '''
        if len(callsheet) == 0:
            logger.info('No applications are available to handle the request.')
            await self._handle_orphan_distr(distributor, *args)
            
        else:
            await self._robodialer(
                self._distr_single, 
                callsheet, 
                distributor, 
                *args
            )
            
    async def _robodialer(self, caller, callsheet, *args):
        tasks = []
        for endpoint in callsheet:
            # For each endpoint...
            tasks.append(
                # ...in parallel, schedule a single execution
                asyncio.ensure_future(
                    # Of a _distribute_single call to the distributor.
                    caller(endpoint, *args)
                )
            )
        await asyncio.gather(*tasks)
                    
    async def _distr_single(self, endpoint, distributor, *args):
        ''' Distributes a single request to a single token.
        '''
        try:
            await distributor(endpoint, *args)
            
        except Exception:
            logger.error(
                'Error while contacting endpoint: \n' +
                ''.join(traceback.format_exc())
            )
            
    async def _handle_orphan_distr(self, distributor, *args):
        ''' This is what happens when our callsheet has zero length.
        Also, this is how we get ants.
        '''
        # Save incoming object shares.
        if distributor is self.send_share:
            sharelog = _ShareLog(*args)
            self._orphan_incoming_shares.add(sharelog)
    
        # But ignore everything else.
    
    async def _obj_sender(self, endpoint, ghid, request_code):
        ''' Generic flow control for sending an object.
        '''
        try:
            obj = self._oracle.get_object(
                gaoclass = _Dispatchable,
                ghid = ghid,
                dispatch = self._dispatch,
                ipc_core = self
            )
            
        except Exception:
            # At some point we'll need some kind of proper handling for this.
            logger.error(
                'Failed to retrieve object at ' + str(ghid) + '\n' +
                ''.join(traceback.format_exc())
            )
            
        else:
            try:
                response = await self.send(
                    session = endpoint,
                    msg = self._pack_object_def(
                        obj.ghid,
                        obj.author,
                        obj.state,
                        False, # is_link is currently unsupported
                        obj.api_id,
                        None,
                        obj.dynamic,
                        None
                    ),
                    request_code = self.REQUEST_CODES[request_code],
                )
                
            except Exception:
                logger.error(
                    'Application client failed to receive object at ' +
                    str(ghid) + ' w/ the following traceback: \n' +
                    ''.join(traceback.format_exc())
                )
                
            else:
                # Don't forget to track who has the object
                self._update_listeners.add(ghid, endpoint)
        
    async def set_token_wrapper(self, endpoint, request_body):
        ''' With the current paradigm of independent app starting, this
        is the "official" start of the application. We set our lookups
        for endpoint <--> token, and then send all startup objects.
        '''
        app_token = request_body[0:4]
        
        if app_token in self._endpoint_from_token:
            raise RuntimeError(
                'Attempt to reregister a new endpoint for the same token. '
                'Each app token must have exactly one endpoint.'
            )
        
        appdef = _AppDef(app_token)
        # Check our app token
        self._dispatch.start_application(appdef)
        
        # TODO: should these be enclosed within an operations lock?
        self._endpoint_from_token[app_token] = endpoint
        self._token_from_endpoint[endpoint] = app_token
        
        startup_ghid = self._dispatch.get_startup_obj(app_token)
        if startup_ghid is not None:
            await self.send_startup(endpoint, startup_ghid)
        
        return b'\x01'
    
    async def new_token_wrapper(self, endpoint, request_body):
        ''' Ignore body, get new token from dispatch, and proceed.
        
        Obviously doesn't require an existing app token.
        '''
        appdef = self._dispatch.register_application()
        app_token = appdef.app_token
        
        # TODO: should these be enclosed within an operations lock?
        self._endpoint_from_token[app_token] = endpoint
        self._token_from_endpoint[endpoint] = app_token
        
        return app_token
    
    async def send_startup(self, endpoint, ghid):
        ''' Sends the endpoint a startup object.
        '''
        await self._obj_sender(endpoint, ghid, 'send_startup')
    
    async def send_share(self, endpoint, ghid, sender):
        ''' Notifies the endpoint of a shared object, for which it is 
        interested. This will never be called when the object was 
        created concurrently by another remote instance of the agent
        themselves, just when someone else shares the object with the
        agent.
        '''
        # Note: currently we're actually sending the whole object update, not
        # just a notification of update address.
        # Note also that we're not currently doing anything about who send the
        # share itself.
        await self._obj_sender(endpoint, ghid, 'send_share')
        
    async def send_object(self, endpoint, ghid):
        ''' Sends a new object to the emedded client. This is called
        when another (concurrent and remote) instance of the logged-in 
        agent has created an object that local applications might be
        interested in.
        
        NOTE: This is not currently invoked anywhere, because we don't
        currently have a mechanism to push these things between multiple
        concurrent Hypergolix instances. Put simply, we're lacking a 
        notification mechanism. See note in Dispatcher.
        '''
        # Note: currently we're actually sending the whole object update, not
        # just a notification of update address.
        await self._obj_sender(endpoint, ghid, 'send_object')
    
    async def send_update(self, endpoint, ghid):
        ''' Sends an updated object to the emedded client.
        '''
        # Note: currently we're actually sending the whole object update, not
        # just a notification of update address.
        await self._obj_sender(endpoint, ghid, 'send_update')
        
    async def send_delete(self, endpoint, ghid):
        ''' Notifies the endpoint that the object has been deleted
        upstream.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('ghid must be type Ghid or similar.')
        
        try:
            response = await self.send(
                session = self,
                msg = bytes(ghid),
                request_code = self.REQUEST_CODES['send_delete'],
                # Note: for now, just don't worry about failures.
                # await_reply = False
            )
                
        except Exception:
            logger.error(
                'Application client failed to receive delete at ' +
                str(ghid) + ' w/ the following traceback: \n' +
                ''.join(traceback.format_exc())
            )
        
    async def notify_share_success(self, endpoint, ghid, recipient):
        ''' Notifies the embedded client of a successful share.
        '''
        try:
            response = await self.send(
                session = endpoint,
                msg = bytes(ghid) + bytes(recipient),
                request_code = self.REQUEST_CODES['notify_share_success'],
                # Note: for now, just don't worry about failures.
                # await_reply = False
            )
            
        except Exception:
            logger.error(
                'Application client failed to receive share success at ' +
                str(ghid) + ' w/ the following traceback: \n' +
                ''.join(traceback.format_exc())
            )
        
    async def notify_share_failure(self, endpoint, ghid, recipient):
        ''' Notifies the embedded client of an unsuccessful share.
        '''
        try:
            response = await self.send(
                session = endpoint,
                msg = bytes(ghid) + bytes(recipient),
                request_code = self.REQUEST_CODES['notify_share_failure'],
                # Note: for now, just don't worry about failures.
                # await_reply = False
            )
        except Exception:
            logger.error(
                'Application client failed to receive share failure at ' +
                str(ghid) + ' w/ the following traceback: \n' +
                ''.join(traceback.format_exc())
            )
        
    async def add_api_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        if len(request_body) != 65:
            raise ValueError('Invalid API ID format.')
            
        self._endpoints_from_api.add(request_body, endpoint)
        
        return b'\x01'
        
    async def remove_api_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        
        Requires existing app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token prior to removing APIs.')
            
        if len(request_body) != 65:
            raise ValueError('Invalid API ID format.')
            
        self._endpoints_from_api.discard(request_body, endpoint)
        
        return b'\x01'
        
    async def whoami_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.whoami into a bytes return.
        
        Does not require an existing app token.
        '''
        ghid = self._golcore.whoami
        return bytes(ghid)
        
    async def register_startup_wrapper(self, endpoint, request_body):
        ''' Wraps object sharing. Requires existing app token. Note that
        it will return successfully immediately, regardless of whether
        or not the share was eventually accepted by the recipient.
        '''
        ghid = Ghid.from_bytes(request_body)
        try:
            requesting_token = self._token_from_endpoint[endpoint]
            
        except KeyError as exc:
            raise IPCError(
                'Must register app token before registering startup objects.'
            ) from exc
            
        self._dispatch.register_startup(requesting_token, ghid)
        return b'\x01'
        
    async def get_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.get_object into a bytes return.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        self._update_listeners.add(ghid, endpoint)
            
        if isinstance(obj.state, Ghid):
            is_link = True
            state = bytes(obj.state)
        else:
            is_link = False
            state = obj.state
            
        # For now, anyways.
        # Note: need to add some kind of handling for legroom.
        _legroom = None
        
        return self._pack_object_def(
            obj.ghid,
            obj.author,
            state,
            is_link,
            obj.api_id,
            obj.private,
            obj.dynamic,
            _legroom
        )
        
    async def new_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_object into a bytes return.
        
        Does not require an existing app token.
        '''
        (
            address, # Unused and set to None.
            author, # Unused and set to None.
            state, 
            is_link, 
            api_id, 
            private, 
            dynamic, 
            _legroom
        ) = self._unpack_object_def(request_body)
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
        
        obj = self._oracle.new_object(
            gaoclass = _Dispatchable,
            dispatch = self._dispatch,
            ipc_core = self,
            state = _DispatchableState(api_id, state),
            dynamic = dynamic,
            _legroom = _legroom,
            api_id = api_id,
        )
            
        # Add the endpoint as a listener.
        self._update_listeners.add(obj.ghid, endpoint)
        
        # If the object is private, register it as such.
        if private:
            try:
                app_token = self._token_from_endpoint[endpoint]
            
            except KeyError as exc:
                raise IPCError(
                    'Must register app token before creating private objects.'
                ) from exc
                
            else:
                logger.debug(
                    'Creating private object for ' + str(endpoint) + 
                    '; bypassing distribution.'
                )
                self._dispatch.register_private(app_token, obj.ghid)
            
        # Otherwise, make sure to notify any other interested parties.
        else:
            # TODO: change send_object to just send the ghid, not the object
            # itself, so that the app doesn't have to be constantly discarding
            # stuff it didn't create?
            callsheet = await self._make_callsheet(
                obj.ghid, 
                skip_endpoint = endpoint
            )
             
            # Note that self._obj_sender handles adding update listeners
            await self.distribute_to_endpoints(
                callsheet,
                self.send_share,
                obj.ghid,
                self._golcore.whoami
            )
        
        return bytes(obj.ghid)
        
    async def update_object_wrapper(self, endpoint, request_body):
        ''' Called to handle downstream application update requests.
        '''
        logger.debug('Handling update request from ' + str(endpoint))
        (
            address,
            author, # Unused and set to None.
            state, 
            is_link, 
            api_id, # Unused and set to None.
            private, # Unused and set to None.
            dynamic, # Unused and set to None.
            _legroom # Unused and set to None.
        ) = self._unpack_object_def(request_body)
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
            
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = address,
            dispatch = self._dispatch,
            ipc_core = self
        )
        obj.update(state)
        
        if not obj.private:
            logger.debug('Object is NOT private; distributing.')
            callsheet = await self._make_callsheet(
                obj.ghid, 
                skip_endpoint = endpoint
            )
                
            await self.distribute_to_endpoints(
                callsheet,
                self.send_update,
                obj.ghid
            )
        else:
            logger.debug('Object IS private; skipping distribution.')
        
        return b'\x01'
        
    async def sync_object_wrapper(self, endpoint, request_body):
        ''' Requires existing app token. Will not return the update; if
        a new copy of the object was available, it will be sent 
        independently.
        '''
        ghid = Ghid.from_bytes(request_body)
        self._salmonator.attempt_pull(ghid)
        return b'\x01'
        
    async def share_object_wrapper(self, endpoint, request_body):
        ''' Wraps object sharing. Requires existing app token. Note that
        it will return successfully immediately, regardless of whether
        or not the share was eventually accepted by the recipient.
        '''
        ghid = Ghid.from_bytes(request_body[0:65])
        recipient = Ghid.from_bytes(request_body[65:130])
        
        try:
            requesting_token = self._token_from_endpoint[endpoint]
            
        except KeyError as exc:
            # Instead of forbidding unregistered apps from sharing objects,
            # go for it, but document that you will never be notified of a
            # share success or failure without an app token.
            requesting_token = None
            
            # raise IPCError(
            #     'Must register app token before sharing objects.'
            # ) from exc
            
        self._rolodex.share_object(ghid, recipient, requesting_token)
        return b'\x01'
        
    async def freeze_object_wrapper(self, endpoint, request_body):
        ''' Wraps object freezing.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        frozen_address = obj.freeze()
        
        return bytes(frozen_address)
        
    async def hold_object_wrapper(self, endpoint, request_body):
        ''' Wraps object holding.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        obj.hold()
        return b'\x01'
        
    async def discard_object_wrapper(self, endpoint, request_body):
        ''' Wraps object discarding. 
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        self._update_listeners.discard(ghid, endpoint)
        return b'\x01'
        
    async def delete_object_wrapper(self, endpoint, request_body):
        ''' Wraps object deletion with a packable format.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        self._update_listeners.discard(ghid, endpoint)
        obj.delete()
        return b'\x01'
        
        
class IPCEmbed:
    ''' The thing you actually put in your app. 
    '''
    REQUEST_CODES = {
        # # Get new app token
        'new_token': b'+T',
        # # Register existing app token
        'set_token': b'$T',
        # Register an API
        'register_api': b'$A',
        # Register an API
        'deregister_api': b'XA',
        # Register a startup object
        'register_startup': b'$O',
        # Whoami?
        'whoami': b'?I',
        # Get object
        'get_object': b'>O',
        # New object
        'new_object': b'+O',
        # Sync object
        'sync_object': b'~O',
        # Update object
        'update_object': b'!O',
        # Share object
        'share_object': b'@O',
        # Freeze object
        'freeze_object': b'*O',
        # Hold object
        'hold_object': b'#O',
        # Discard an object
        'discard_object': b'-O',
        # Delete object
        'delete_object': b'XO',
    }
    
    def __init__(self, *args, **kwargs):
        ''' Initializes self.
        '''  
        self._token = None
        self._whoami = None
        self._ipc = None
        self._startup_obj = None
        self._legroom = 7
        
        # Lookup for ghid -> object
        self._objs_by_ghid = weakref.WeakValueDictionary()
        
        # All of the various object handlers
        # Lookup api_id: async awaitable share handler
        self._share_handlers = {}
        # Lookup api_id: object class
        self._share_typecast = {}
        
        # Currently unused
        self._nonlocal_handlers = {}
        
        # Create an executor for awaiting threadsafe callbacks and handlers
        self._executor = concurrent.futures.ThreadPoolExecutor()
        
        # Note that these are only for unsolicited contact from the server.
        req_handlers = {
            # Receive a startup object.
            b':O': self.deliver_startup_wrapper,
            # Receive a new object from a remotely concurrent instance of self.
            b'+O': self.deliver_object_wrapper,
            # Receive a new object from a share.
            b'^O': self.deliver_share_wrapper,
            # Receive an update for an existing object.
            b'!O': self.update_object_wrapper,
            # Receive a delete command.
            b'XO': self.delete_object_wrapper,
            # Receive an async notification of a sharing failure.
            b'^F': self.notify_share_failure_wrapper,
            # Receive an async notification of a sharing success.
            b'^S': self.notify_share_success_wrapper,
        }
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            # Note: can also add error_lookup = {b'er': RuntimeError}
            *args, **kwargs
        )
        
    @property
    def whoami(self):
        ''' Read-only access to self._whoami with a raising wrapper if
        it is undefined.
        '''
        if self._whoami is not None:
            return self._whoami
        else:
            raise RuntimeError(
                'Whoami has not been defined. Most likely, no IPC client is '
                'currently available.'
            )
            
    def subscribe_to_updates(self, obj):
        ''' Called (primarily internally) to automatically subscribe the
        object to updates from upstream. Except really, right now, this
        just makes sure that we're tracking it in our local object
        lookup so that we can actually **apply** the updates we're 
        already receiving.
        '''
        self._objs_by_ghid[obj.hgx_ghid] = obj
        
    async def _get_whoami(self):
        ''' Pulls identity fingerprint from hypergolix IPC.
        '''
        raw_ghid = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['whoami']
        )
        return Ghid.from_bytes(raw_ghid)
        
    async def _add_ipc(self, client_class, *args, **kwargs):
        ''' Automatically sets up an IPC client connected to hypergolix.
        Just give it the client_class, eg WSBasicClient, and all of the 
        *args and **kwargs will be passed to the client's __init__.
        '''
        if self._ipc is not None:
            raise RuntimeError(
                'Must clear existing ipc before establishing a new one.'
            )
        
        # We could maybe do this elsewhere, but adding an IPC client isn't 
        # really performance-critical, especially not now.
        class LinkedClient(AutoresponseConnector, client_class):
            async def loop_stop(client, *args, **kwargs):
                ''' Clear both the app token and whoami in the embedded
                link when the loop stops. Ideally, this would also be
                called when a connection drops. TODO: that. Or, perhaps
                something similar, but after we've assimilated multiple
                loopertroopers into a single event loop.
                '''
                # This is a closure around parent self.
                self._startup_obj = None
                self._whoami = None
                await super().loop_stop(*args, **kwargs)
            
        self._ipc = LinkedClient(autoresponder=self, *args, **kwargs)
        await self.await_session_async()
        self._whoami = await self._get_whoami()
        
    async def add_ipc_loopsafe(self, *args, **kwargs):
        await run_coroutine_loopsafe(
            coro = self._add_ipc(*args, **kwargs),
            target_loop = self._loop
        )
        
    def add_ipc_threadsafe(self, *args, **kwargs):
        call_coroutine_threadsafe(
            coro = self._add_ipc(*args, **kwargs),
            loop = self._loop
        )
            
    async def _clear_ipc(self):
        ''' Disconnects and removes the current IPC.
        '''
        # NOTE THAT THIS WILL NEED TO CHANGE if the _ipc client is ever brought
        # into the same event loop as the IPCEmbed autoresponder.
        if self._ipc is None:
            raise RuntimeError('No existing IPC to clear.')
            
        self._ipc.stop_threadsafe_nowait()
        self._ipc = None
        self._whoami = None
        
    async def clear_ipc_loopsafe(self, *args, **kwargs):
        await run_coroutine_loopsafe(
            coro = self._clear_ipc(*args, **kwargs),
            target_loop = self._loop
        )
        
    def clear_ipc_threadsafe(self, *args, **kwargs):
        call_coroutine_threadsafe(
            coro = self._clear_ipc(*args, **kwargs),
            loop = self._loop
        )
    
    @property
    def app_token(self):
        ''' Read-only access to the current app token.
        '''
        if self._token is None:
            return RuntimeError(
                'You must get a new token (or set an existing one) first!'
            )
        else:
            return self._token
        
    async def _get_new_token(self):
        ''' Registers a new token with Hypergolix. Call this once per
        application, and then reuse each time the application restarts.
        
        Returns the token, and also caches it with self.app_token.
        '''
        app_token = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['new_token']
        )
        self._token = app_token
        return app_token
    
    def get_new_token_threadsafe(self):
        ''' Threadsafe wrapper for new_token.
        '''
        return call_coroutine_threadsafe(
            coro = self._get_new_token(),
            loop = self._loop,
        )
    
    async def get_new_token_loopsafe(self):
        ''' Loopsafe wrapper for new_token.
        '''
        return (await run_coroutine_loopsafe(
            coro = self._get_new_token(),
            target_loop = self._loop,
        ))
        
    async def _set_existing_token(self, app_token):
        ''' Sets the app token for an existing application. Should be
        called every time the application restarts.
        '''
        response = await self.send(
            session = self.any_session,
            msg = app_token,
            request_code = self.REQUEST_CODES['set_token']
        )
        
        # If we haven't errored out...
        self._token = app_token
        
        # Note that, due to:
        #   1. the way the request/response system works
        #   2. the ipc host sending any startup obj during token registration
        #   3. the ipc host awaiting OUR ack from the startup-object-sending
        #       before acking the original token setting
        #   4. us awaiting that last ack
        # we are guaranteed to already have any declared startup object.
        if self._startup_obj is not None:
            return self._startup_obj
        else:
            return None
    
    def set_existing_token_threadsafe(self, *args, **kwargs):
        ''' Threadsafe wrapper for self._set_existing_token.
        '''
        return call_coroutine_threadsafe(
            self._set_existing_token(*args, **kwargs),
            loop = self._loop,
        )
        
    async def set_existing_token_loopsafe(self, *args, **kwargs):
        ''' Loopsafe wrapper for self._set_existing_token.
        '''
        return (await run_coroutine_loopsafe(
            coro = self._set_existing_token(*args, **kwargs),
            target_loop = self._loop,
        ))
            
    def _normalize_api_id(self, api_id):
        ''' Wraps the api_id appropriately, making sure the first byte
        is '\x00' and that it is an appropriate length.
        '''
        if len(api_id) == 65:
            if api_id[0:1] != b'\x00':
                raise ValueError(
                    'Improper api_id. First byte of full 65-byte field must '
                    'be x00.'
                )
        elif len(api_id) == 64:
            api_id = b'\x00' + api_id
            
        else:
            raise ValueError('Improper length of api_id.')
            
        return api_id
    
    async def _register_api(self, api_id):
        ''' Registers the api_id with the hypergolix service, allowing
        this application to receive shares from it.
        '''
        # Don't need to call this twice...
        # api_id = self._normalize_api_id(api_id)
            
        response = await self.send(
            session = self.any_session,
            msg = api_id,
            request_code = self.REQUEST_CODES['register_api']
        )
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while registering API.')
            
    async def _deregister_api(self, api_id):
        ''' Stops updates for the api_id from the hypergolix service.
        '''
        response = await self.send(
            session = self.any_session,
            msg = api_id,
            request_code = self.REQUEST_CODES['deregister_api']
        )
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while deregistering API.')
    
    async def _register_share_handler(self, api_id, cls, handler):
        ''' Call this to register a handler for an object shared by a
        different hypergolix identity, or the same hypergolix identity
        but a different application. Any api_id can have at most one
        share handler, across ALL forms of callback (internal,
        threadsafe, loopsafe).
        
        typecast determines what kind of ObjProxy class the object will
        be cast into before being passed to the handler.
        
        This HANDLER will be called from within the IPC embed's internal
        event loop.
        
        This METHOD must be called from within the IPC embed's internal
        event loop.
        '''
        api_id = self._normalize_api_id(api_id)
        await self._register_api(api_id)
        
        # Any handlers passed to us this way can already be called natively
        # from withinour own event loop, so they just need to be wrapped such
        # that they never raise.
        async def wrap_handler(*args, handler=handler, **kwargs):
            try:
                await handler(*args, **kwargs)
                
            except Exception:
                logger.error(
                    'Error while running share handler. Traceback: \n' +
                    ''.join(traceback.format_exc())
                )
        
        # Hey, look at this! Because we're running a single-threaded event loop
        # and not ceding flow control to the loop, we don't need to worry about
        # synchro primitives here!
        self._share_handlers[api_id] = wrap_handler
        self._share_typecast[api_id] = cls
    
    def register_share_handler_threadsafe(self, api_id, cls, handler):
        ''' Call this to register a handler for an object shared by a
        different hypergolix identity, or the same hypergolix identity
        but a different application. Any api_id can have at most one 
        share handler, across ALL forms of callback (internal, 
        threadsafe, loopsafe).
        
        typecast determines what kind of ObjProxy class the object will
        be cast into before being passed to the handler.
        
        This HANDLER will be called from within a single-use, dedicated
        thread.
        
        This METHOD must be called from a different thread than the IPC 
        embed's internal event loop.
        '''
        # For simplicity, wrap the handler, so that any shares can be called
        # normally from our own event loop.
        async def wrapped_handler(*args, func=handler):
            ''' Wrap the handler in run_in_executor.
            '''
            await self._loop.run_in_executor(
                self._executor,
                func,
                *args
            )
            
        call_coroutine_threadsafe(
            coro = self._register_share_handler(
                api_id, 
                cls, 
                wrapped_handler
            ),
            loop = self._loop
        )
    
    async def register_share_handler_loopsafe(self, api_id, cls, handler, 
            target_loop):
        ''' Call this to register a handler for an object shared by a
        different hypergolix identity, or the same hypergolix identity
        but a different application. Any api_id can have at most one 
        share handler, across ALL forms of callback (internal, 
        threadsafe, loopsafe).
        
        typecast determines what kind of ObjProxy class the object will
        be cast into before being passed to the handler.
        
        This HANDLER will be called within the specified event loop, 
        also implying the specified event loop context (ie thread).
        
        This METHOD must be called from a different event loop than the 
        IPC embed's internal event loop. It is internally loopsafe, and
        need not be wrapped by run_coroutine_loopsafe.
        '''
        # For simplicity, wrap the handler, so that any shares can be called
        # normally from our own event loop.
        async def wrapped_handler(*args, loop=target_loop, coro=handler):
            ''' Wrap the handler in run_in_executor.
            '''
            await run_coroutine_loopsafe(
                coro = coro(*args),
                target_loop = loop
            )
            
        await run_coroutine_loopsafe(
            coro = self._register_share_handler(
                api_id, 
                cls, 
                wrapped_handler
            ),
            loop = self._loop
        )
    
    async def _register_nonlocal_handler(self, api_id, handler):
        ''' Call this to register a handler for any private objects 
        created by the same hypergolix identity and the same hypergolix 
        application, but at a separate, concurrent session.
        
        This HANDLER will be called from within the IPC embed's internal
        event loop.
        
        This METHOD must be called from within the IPC embed's internal
        event loop.
        '''
        raise NotImplementedError()
        api_id = self._normalize_api_id(api_id)
        
        # self._nonlocal_handlers = {}
    
    def register_nonlocal_handler_threadsafe(self, api_id, handler):
        ''' Call this to register a handler for any private objects 
        created by the same hypergolix identity and the same hypergolix 
        application, but at a separate, concurrent session.
        
        This HANDLER will be called from within a single-use, dedicated
        thread.
        
        This METHOD must be called from a different thread than the IPC 
        embed's internal event loop.
        '''
        raise NotImplementedError()
        api_id = self._normalize_api_id(api_id)
        
        # self._nonlocal_handlers = {}
    
    async def register_nonlocal_handler_loopsafe(self, api_id, handler, loop):
        ''' Call this to register a handler for any private objects 
        created by the same hypergolix identity and the same hypergolix 
        application, but at a separate, concurrent session.
        
        This HANDLER will be called within the specified event loop, 
        also implying the specified event loop context (ie thread).
        
        This METHOD must be called from a different event loop than the 
        IPC embed's internal event loop. It is internally loopsafe, and
        need not be wrapped by run_coroutine_loopsafe.
        '''
        raise NotImplementedError()
        api_id = self._normalize_api_id(api_id)
        
        # self._nonlocal_handlers = {}
        
    async def deliver_startup_wrapper(self, session, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        '''
        (
            address,
            author,
            state, 
            is_link, 
            api_id,
            private, # Will be unused and set to None 
            dynamic,
            _legroom # Will be unused and set to None
        ) = self._unpack_object_def(request_body)
        
        # Resolve any links
        if is_link:
            link = Ghid.from_bytes(state)
            # Note: this may cause things to freeze, because async
            state = self.get_object(link)
            
        # Okay, now let's create an object for it
        obj = ObjBase(
            hgxlink = self, 
            state = state, 
            api_id = api_id, 
            dynamic = dynamic,
            private = False,
            ghid = address, 
            binder = author, 
            # _legroom = None,
        )
            
        # Don't forget to add it to local lookup, since we're not rerouting
        # the update through get_object.
        self.subscribe_to_updates(obj)
        
        # Set the startup obj internally so that _set_existing_token has access
        # to it.
        self._startup_obj = obj
        
        # Successful delivery. Return true
        return b'\x01'
        
    async def _get(self, cls, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        
        TODO: support implicit typecast based on api_id.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['get_object']
        )
        
        (
            address,
            author,
            state, 
            is_link, 
            api_id, 
            private, 
            dynamic, 
            _legroom
        ) = self._unpack_object_def(response)
            
        if is_link:
            # First discard the object, since we can't support it.
            response = await self.send(
                session = self.any_session,
                msg = bytes(address),
                request_code = self.REQUEST_CODES['discard_object']
            )
            
            # Now raise.
            raise NotImplementedError(
                'Hypergolix does not yet support nested links to other '
                'dynamic objects.'
            )
            # link = Ghid.from_bytes(state)
            # state = await self._get(link)
        
        state = await cls._hgx_unpack(state)
        obj = cls(
            hgxlink = self,
            state = state, 
            api_id = api_id, 
            dynamic = dynamic,
            private = private, 
            ghid = address, 
            binder = author, 
            # _legroom = _legroom,
        )
            
        # Don't forget to add it to local lookup so we can apply updates.
        self.subscribe_to_updates(obj)
        
        return obj
        
    def get_threadsafe(self, cls, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        return call_coroutine_threadsafe(
            coro = self._get(cls, ghid),
            loop = self._loop,
        )
        
    async def get_loopsafe(self, cls, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        return (await run_coroutine_loopsafe(
            coro = self._get(cls, ghid),
            target_loop = self._loop,
        ))
    
    async def _new(self, cls, state, api_id=None, dynamic=True, private=False,
                    *args, **kwargs):
        ''' Create the object, yo.
        '''
        if api_id is None:
            api_id = cls._hgx_DEFAULT_API_ID
        
        obj = cls(
            hgxlink = self, 
            state = state, 
            api_id = api_id,
            dynamic = dynamic,
            private = private,
            *args, **kwargs
        )
        await obj._hgx_push()
        self.subscribe_to_updates(obj)
        return obj
        
    def new_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            coro = self._new(*args, **kwargs),
            loop = self._loop,
        )
        
    async def new_loopsafe(self, *args, **kwargs):
        return (await run_coroutine_loopsafe(
            coro = self._new(*args, **kwargs),
            target_loop = self._loop,
        ))
        
    async def _make_new(self, obj):
        ''' Submits a request for making a new object, and returns the
        resulting (address, binder).
        '''
        state = await obj._hgx_pack(
            obj._proxy_3141592
        )
        
        payload = self._pack_object_def(
            None,
            None,
            state,
            False, # is_link
            self._normalize_api_id(obj._api_id_3141592), # api_id
            obj.hgx_private,
            obj.hgx_dynamic,
            self._legroom
        )
        # Do this before making the request in case we disconnect immediately
        # after making it.
        binder = self.whoami
        # Now actually make the object.
        response = await self.send(
            session = self.any_session,
            msg = payload,
            request_code = self.REQUEST_CODES['new_object']
        )
        
        address = Ghid.from_bytes(response)
        return address, binder
        
    async def _make_update(self, obj):
        ''' Submits a request for updating an object. Does no LBYL 
        checking if dynamic, etc; just goes for it.
        '''
        state = await obj._hgx_pack(
            obj._proxy_3141592
        )
        msg = self._pack_object_def(
            obj.hgx_ghid,
            None, # Author
            state,
            False, # is_link
            None, # api_id
            None, # private
            None, # dynamic
            None # legroom
        )
        
        response = await self.send(
            session = self.any_session,
            msg = msg,
            request_code = self.REQUEST_CODES['update_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
            
        # Let the object worry about callbacks.
            
        return True
        
    async def _make_sync(self, obj):
        ''' Initiates a forceful upstream sync.
        ''' 
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['sync_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
    async def _make_share(self, obj, recipient):
        ''' Handles only the sharing of an object via the hypergolix
        service. Does not manage anything to do with the proxy itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid) + bytes(recipient),
            request_code = self.REQUEST_CODES['share_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while updating object.')
    
    async def _make_freeze(self, obj):
        ''' Handles only the freezing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['freeze_object']
        )
        
        frozen = await self._get(
            cls = type(obj), 
            ghid = Ghid.from_bytes(response)
        )
        
        return frozen
        
    async def _make_hold(self, obj):
        ''' Handles only the holding of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['hold_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while holding object.')
            
    async def _make_discard(self, obj):
        ''' Handles only the discarding of an object via the hypergolix
        service.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['discard_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        # It's a weakvaluedict. Doing this doesn't make the object any freer,
        # but it prevents us from fixing any future problems with it.
        
        # try:
        #     del self._objs_by_ghid[obj.hgx_ghid]
        # except KeyError:
        #     pass
            
        return True
        
    async def _make_delete(self, obj):
        ''' Handles only the deleting of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['delete_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        # It's a weakvaluedict. Doing this doesn't make the object any freer,
        # but it prevents us from fixing any future problems with it.
        
        # try:
        #     del self._objs_by_ghid[obj.address]
        # except KeyError:
        #     pass
        
        return True
        
    async def deliver_share_wrapper(self, session, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        '''
        (
            address,
            author,
            state, 
            is_link, 
            api_id,
            private, # Will be unused and set to None 
            dynamic,
            _legroom # Will be unused and set to None
        ) = self._unpack_object_def(request_body)
        
        # Resolve any links
        if is_link:
            raise NotImplementedError()
            
        # This is async, which is single-threaded, so there's no race condition
        try:
            handler = self._share_handlers[api_id]
            cls = self._share_typecast[api_id]
            
        except KeyError:
            logger.warning(
                'Received a share for an API_ID that was lacking a handler or '
                'typecast. Deregistering the API_ID.'
            )
            await self._deregister_api(api_id)
            
        else:
            state = await cls._hgx_unpack(state)
            obj = cls(
                hgxlink = self,
                state = state,
                api_id = api_id,
                dynamic = dynamic,
                private = False,
                ghid = address,
                binder = author
            )
            
            # Don't forget to add it to local lookup, since we're not rerouting
            # the update through get_object.
            self.subscribe_to_updates(obj)
            
            # Run this concurrently, so that we can release the req/res session
            asyncio.ensure_future(handler(obj))
        
        # Successful delivery. Return true
        return b'\x01'
        
    async def deliver_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        
        Note that (despite the terrible name) this is only called when a
        concurrent instance of the same application with the same 
        hypergolix agent creates a (private) object.
        '''
        raise NotImplementedError()

    async def update_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object update, updates the AppObj
        instance(s) accordingly, and serializes a response to the IPC 
        host.
        '''
        (
            address,
            author, # Will be unused and set to None
            state, 
            is_link, 
            api_id, # Will be unused and set to None 
            private, # Will be unused and set to None 
            dynamic, # Will be unused and set to None 
            _legroom # Will be unused and set to None
        ) = self._unpack_object_def(request_body)
        
        try:
            obj = self._objs_by_ghid[address]
            
        except KeyError:
            # Just discard the object, since we don't actually have a copy of
            # it locally.
            logger.warning(
                'Received an object update, but the object was no longer '
                'contained in memory. Discarding its subscription: ' +
                str(address) + '.'
            )
            response = await self.send(
                session = self.any_session,
                msg = bytes(address),
                request_code = self.REQUEST_CODES['discard_object']
            )
            
        else:
            if is_link:
                # Uhhhhhh... Raise? It's not really appropriate to discard...
                raise NotImplementedError(
                    'Cannot yet support objects with nested dynamic links.'
                )
                
            else:
                logger.debug(
                    'Received update for ' + str(address) + '; forcing pull.'
                )
                await obj._force_pull_3141592(state)
            
        return b'\x01'
        
    async def delete_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object deletion, and applies it to
        the object.
        '''
        ghid = Ghid.from_bytes(request_body)
        
        try:
            obj = self._objs_by_ghid[ghid]
        except KeyError:
            logger.debug(str(ghid) + ' not known to IPCEmbed.')
        else:
            await obj._force_delete_3141592()
            
        return b'\x01'

    async def notify_share_failure_wrapper(self, session, request_body):
        ''' Deserializes an incoming async share failure notification, 
        dispatches that to the app, and serializes a response to the IPC 
        host.
        '''
        return b''

    async def notify_share_success_wrapper(self, session, request_body):
        ''' Deserializes an incoming async share failure notification, 
        dispatches that to the app, and serializes a response to the IPC 
        host.
        '''
        return b''
