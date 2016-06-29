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

# Control * imports.
__all__ = [
    'IPCHost',
    'IPCEmbed',
    'AppObj'
]

# External dependencies
import abc
import msgpack
import os
import warnings
import weakref
import threading
import collections

from golix import Ghid






# import collections
# import warnings
# import functools
# import struct

import asyncio
import websockets
from websockets.exceptions import ConnectionClosed

import time
# import string
import traceback






# Intrapackage dependencies
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning
from .exceptions import IPCError

from .utils import IPCPackerMixIn
from .utils import RawObj
from .utils import call_coroutine_threadsafe

from .comms import _AutoresponderSession
from .comms import Autoresponder


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


class IPCHostEndpoint(_AutoresponderSession):
    ''' An application endpoints, as used by IPC hosts. Must only be 
    created from within an event loop!
    
    ENDPOINTS HAVE A 1:1 CORRELATION WITH APPLICATION TOKENS. A token
    denotes a singular application, and one endpoint is used for one
    application.
    
    TODO: move object methods into the IPC host. Also a note about them:
    since they're only ever called by the dispatch, which doesn't know 
    about the endpoint until it has been successfully registered, we 
    don't need to check for the app token.
    
    TODO: in the process of above, will need to modify dispatchers to 
    accommodate new behavior. That will also standardize the call 
    signatures for anything involving sessions, connections, etc.
    '''
    def __init__(self, ipc, dispatch, app_token=None, apis=None, *args, **kwargs):
        ''' Creates an endpoint for the specified agent that handles the
        associated apis. Apis is an iterable of api_ids. If token is not
        specified, generates a new one from dispatch.
        '''
        self._ctx = asyncio.Event()
        
        self._dispatch = weakref.proxy(dispatch)
        self._ipc = weakref.proxy(ipc)
        self._expecting_exchange = threading.Lock()
        self._known_ghids = set()
            
        self._token = app_token
        self._apis = set()
        
        if apis is not None:
            for api in apis:
                self.add_api(api)
                
        super().__init__(*args, **kwargs)
            
    def add_api(self, api_id):
        ''' This adds an api_id to the endpoint. Probably not strictly
        necessary, but helps keep track of things.
        '''
        # Need to add a type check.
        self._apis.add(api_id)
        # Don't forget to update the dispatch. For now, just reregister self.
        self.dispatch.register_endpoint(self)
        
    @property
    def dispatch(self):
        ''' Access the agent.
        '''
        return self._dispatch
        
    @property
    def app_token(self):
        ''' Access the app token.
        '''
        return self._token
        
    @property
    def apis(self):
        ''' Access a frozen set of the apis supported by the endpoint.
        '''
        return frozenset(self._apis)
        
    def notify_object_threadsafe(self, ghid, state):
        ''' Do that whole threadsafe thing.
        '''
        return call_coroutine_threadsafe(
            coro = self.notify_object(ghid, state),
            loop = self._ipc._loop
        )
        
    async def notify_object(self, ghid, state):
        ''' Notifies the endpoint that the object is available. May be
        either a new object, or an updated one.
        
        Checks to make sure we're not currently expecting and update to
        suppress.
        '''
        # if not self._expecting_exchange.locked():
        if ghid in self._known_ghids:
            await self.send_update(ghid, state)
        else:
            self.register_ghid(ghid)
            await self.send_object(ghid, state)
            
    def register_ghid(self, ghid):
        ''' Pretty simple wrapper to make sure we know about the ghid.
        '''
        self._known_ghids.add(ghid)
        
    def send_object_threadsafe(self, ghid, state):
        return call_coroutine_threadsafe(
            coro = self.send_object(ghid, state),
            loop = self._ipc._loop
        )
        
    async def send_object(self, ghid, state):
        ''' Sends a new object to the emedded client.
        '''
        if isinstance(state, Ghid):
            is_link = True
            state = bytes(state)
        else:
            is_link = False
            
        author = self.dispatch._author_by_ghid[ghid]
        dynamic = self.dispatch._dynamic_by_ghid[ghid]
        api_id = self.dispatch._api_by_ghid[ghid]
        
        response = await self._ipc.send(
            session = self,
            msg = self._ipc._pack_object_def(
                ghid,
                author,
                state,
                is_link,
                api_id,
                None,
                None,
                dynamic,
                None
            ),
            request_code = self._ipc.REQUEST_CODES['send_object'],
            # Note: for now, just don't worry about failures.
            # Todo: expect reply, and enclose within try/catch to prevent apps
            # from crashing the service.
            await_reply = False
        )
    
    def send_update_threadsafe(self, ghid, state):
        return call_coroutine_threadsafe(
            coro = self.send_update(ghid, state),
            loop = self._ipc._loop
        )
    
    async def send_update(self, ghid, state):
        ''' Sends an updated object to the emedded client.
        '''
        # Note: currently we're actually sending the whole object update, not
        # just a notification of update address.
        # print('Endpoint got send update request.')
        # print(ghid)
        
        if isinstance(state, Ghid):
            is_link = True
            state = bytes(state)
        else:
            is_link = False
        
        response = await self._ipc.send(
            session = self,
            msg = self._ipc._pack_object_def(
                ghid,
                None,
                state,
                is_link,
                None,
                None,
                None,
                None,
                None
            ),
            request_code = self._ipc.REQUEST_CODES['send_update'],
            # Note: for now, just don't worry about failures. See previous note
            await_reply = False
        )
        # print('Update sent and resuming life.')
        # if response == b'\x01':
        #     return True
        # else:
        #     raise RuntimeError('Unknown error while delivering object update.')
    
    def send_delete_threadsafe(self, ghid):
        return call_coroutine_threadsafe(
            coro = self.send_delete(ghid),
            loop = self._ipc._loop
        )
        
    async def send_delete(self, ghid):
        ''' Notifies the endpoint that the object has been deleted 
        upstream.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('ghid must be type Ghid or similar.')
        
        response = await self._ipc.send(
            session = self,
            msg = bytes(ghid),
            request_code = self._ipc.REQUEST_CODES['send_delete'],
            # Note: for now, just don't worry about failures.
            await_reply = False
        )
        # print('Update sent and resuming life.')
        # if response == b'\x01':
        #     return True
        # else:
        #     raise RuntimeError('Unknown error while delivering object update.')
    
    def notify_share_failure_threadsafe(self, ghid, recipient):
        return call_coroutine_threadsafe(
            coro = self.notify_share_failure(ghid, recipient),
            loop = self._ipc._loop
        )
        
    async def notify_share_failure(self, ghid, recipient):
        ''' Notifies the embedded client of an unsuccessful share.
        '''
        pass
    
    def notify_share_success_threadsafe(self, ghid, recipient):
        return call_coroutine_threadsafe(
            coro = self.notify_share_success(ghid, recipient),
            loop = self._ipc._loop
        )
        
    async def notify_share_success(self, ghid, recipient):
        ''' Notifies the embedded client of a successful share.
        '''
        pass


class IPCHost(Autoresponder, IPCPackerMixIn):
    ''' Reusable base class for Hypergolix IPC hosts. Subclasses 
    Autoresponder. Combine with a server in an AutoComms to establish 
    IPC over that transport.
    '''
    REQUEST_CODES = {
        # Receive/dispatch a new object.
        'send_object': b'+O',
        # Receive an update for an existing object.
        'send_update': b'!O',
        # Receive an update that an object has been deleted.
        'send_delete': b'XO',
        # Receive an async notification of a sharing failure.
        'notify_share_failure': b'^F',
        # Receive an async notification of a sharing success.
        'notify_share_success': b'^S',
    }
    
    def __init__(self, dispatch, *args, **kwargs):
        # Don't weakref.proxy, since dispatchers never contact the ipcbase,
        # just the endpoints.
        self._dispatch = dispatch
        
        req_handlers = {
            # New app tokens are handled during endpoint creation.
            # # Get new app token
            b'+T': self.new_token_wrapper,
            # # Register existing app token
            b'$T': self.set_token_wrapper,
            # Register an API
            b'$A': self.add_api_wrapper,
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
            
    def session_factory(self):
        ''' Added for easier subclassing. Returns the session class.
        '''
        logger.debug('Session endpoint created, but not yet initialized.')
        return IPCHostEndpoint(
            dispatch = self.dispatch,
            ipc = self,
            # app_token = app_token,
            # apis = apis,
        )
        
    async def init_endpoint(self, endpoint):
        ''' Waits for the endpoint to be ready for use, then registers
        it with dispatch.
        '''
        await endpoint._ctx.wait()
        self.dispatch.register_endpoint(endpoint)
        logger.debug('Session completely registered.')
        
    @property
    def dispatch(self):
        ''' Sorta superfluous right now.
        '''
        return self._dispatch
    
    async def set_token_wrapper(self, endpoint, request_body):
        ''' Ignore body, get new token from dispatch, and proceed.
        
        Obviously doesn't require an existing app token.
        '''
        app_token = request_body[0:4]
        endpoint.app_token = app_token
        endpoint._ctx.set()
        return b''
    
    async def new_token_wrapper(self, endpoint, request_body):
        ''' Ignore body, get new token from dispatch, and proceed.
        
        Obviously doesn't require an existing app token.
        '''
        app_token = self.dispatch.new_token()
        endpoint._token = app_token
        endpoint._ctx.set()
        return app_token
        
    async def add_api_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        
        Requires existing app token.
        '''
        if not endpoint.app_token:
            raise IPCError('Must register app token prior to adding APIs.')
        if len(request_body) != 65:
            raise ValueError('Invalid API ID format.')
        endpoint.add_api(request_body)
        return b'\x01'
        
    async def whoami_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.whoami into a bytes return.
        
        Does not require an existing app token.
        '''
        ghid = self.dispatch.whoami
        return bytes(ghid)
        
    async def get_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.get_object into a bytes return.
        
        Requires an existing app token.
        '''
        if not endpoint.app_token:
            raise IPCError('Must register app token prior to getting objects.')
            
        ghid = Ghid.from_bytes(request_body)
        
        author, state, api_id, app_token, is_dynamic = \
            self.dispatch.get_object(
                asking_token = endpoint.app_token,
                ghid = ghid
            )
            
        if app_token != bytes(4):
            private = True
        else:
            private = False
            
        if isinstance(state, Ghid):
            is_link = True
            state = bytes(state)
        else:
            is_link = False
            
        # For now, anyways.
        # Note: need to add some kind of handling for legroom.
        _legroom = None
        
        # Let the endpoint know to remember it
        endpoint.register_ghid(ghid)
        
        return self._pack_object_def(
            ghid,
            author,
            state,
            is_link,
            api_id,
            app_token,
            private,
            is_dynamic,
            _legroom
        )
        
    async def new_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_object into a bytes return.
        
        Requires an existing app token.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to making new objects.'
            )
        
        (
            address, # Unused and set to None.
            author, # Unused and set to None.
            state, 
            is_link, 
            api_id, 
            app_token, 
            private, 
            dynamic, 
            _legroom
        ) = self._unpack_object_def(request_body)
        
        if is_link:
            state = Ghid.from_bytes(state)
        
        address = self.dispatch.new_object(
            asking_token = endpoint.app_token,
            state = state, 
            api_id = api_id, 
            app_token = app_token, 
            dynamic = dynamic,
            _legroom = _legroom
        )
        
        # Let the endpoint know to remember it
        endpoint.register_ghid(address)
        
        return bytes(address)
        
    async def update_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        
        Requires existing app token.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to updating objects.'
            )
            
        (
            address,
            author, # Unused and set to None.
            state, 
            is_link, 
            api_id, # Unused and set to None.
            app_token, # Unused and set to None.
            private, # Unused and set to None.
            dynamic, # Unused and set to None.
            _legroom # Unused and set to None.
        ) = self._unpack_object_def(request_body)
        
        if is_link:
            state = Ghid.from_bytes(state)
        
        self.dispatch.update_object(
            asking_token = endpoint.app_token,
            ghid = address,
            state = state
        )
        
        return b'\x01'
        
    async def sync_object_wrapper(self, endpoint, request_body):
        ''' Requires existing app token. Currently unimplimented.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to syncing objects.'
            )
            
        # return b''
        raise NotImplementedError('Manual syncing not yet supported.')
        
    async def share_object_wrapper(self, endpoint, request_body):
        ''' Wraps object sharing.
        
        Requires existing app token.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to sharing objects.'
            )
            
        ghid = Ghid.from_bytes(request_body[0:65])
        recipient = Ghid.from_bytes(request_body[65:130])
        self.dispatch.share_object(
            asking_token = endpoint.app_token,
            ghid = ghid,
            recipient = recipient
        )
        return b'\x01'
        
    async def freeze_object_wrapper(self, endpoint, request_body):
        ''' Wraps object freezing into a packed format.
        
        Requires an app token.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to freezing objects.'
            )
            
        ghid = Ghid.from_bytes(request_body)
        address = self.dispatch.freeze_object(
            asking_token = endpoint.app_token,
            ghid = ghid,
        )
        return bytes(address)
        
    async def hold_object_wrapper(self, endpoint, request_body):
        ''' Wraps object holding into a packed format.
        
        Requires an app token.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to holding objects.'
            )
            
        ghid = Ghid.from_bytes(request_body)
        self.dispatch.hold_object(
            asking_token = endpoint.app_token,
            ghid = ghid,
        )
        return b'\x01'
        
    async def discard_object_wrapper(self, endpoint, request_body):
        ''' Wraps object discarding into a packable format.
        
        Requires an app token.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to discarding objects.'
            )
            
        ghid = Ghid.from_bytes(request_body)
        self.dispatch.discard_object(
            asking_token = endpoint.app_token,
            ghid = ghid,
        )
        return b'\x01'
        
    async def delete_object_wrapper(self, endpoint, request_body):
        ''' Wraps object deletion with a packable format.
        
        Requires an app token.
        '''
        if not endpoint.app_token:
            raise IPCError(
                'Must register app token prior to updating objects.'
            )
            
        ghid = Ghid.from_bytes(request_body)
        self.dispatch.delete_object(
            asking_token = endpoint.app_token,
            ghid = ghid,
        )
        return b'\x01'
        
        
class IPCEmbed(Autoresponder, IPCPackerMixIn):
    ''' The thing you actually put in your app. Pair with a client in an
    Autocomms to establish IPC over that transport.
    
    Note that each embed has exactly one endpoint. I guess there's not
    really any reason an application couldn't use multiple embeds, but
    there really should never be a reason to do that?
    
    Todo: asyncify.
    '''
    REQUEST_CODES = {
        # # Get new app token
        'new_token': b'+T',
        # # Register existing app token
        'set_token': b'$T',
        # Register an API
        'register_api': b'$A',
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
        try:    
            self._token = None
            self._legroom = 3
        
        # Something strange using _TestEmbed is causing this, suppress it for
        # now until technical debt can be reduced
        except AttributeError:
            pass
        
        # Lookup for ghid -> object
        self._objs_by_ghid = weakref.WeakValueDictionary()
        
        # Track registered callbacks for new objects
        self._object_handlers = {}
        
        # Note that these are only for unsolicited contact from the server.
        req_handlers = {
            # Receive/dispatch a new object.
            b'+O': self.deliver_object_wrapper,
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
    def app_token(self):
        ''' Get your app token, or if you have none, register a new one.
        '''
        if self._token is None:
            return RuntimeError(
                'You must get a new token (or set an existing one) first!'
            )
        else:
            return self._token
    
    def whoami_threadsafe(self):
        ''' Threadsafe wrapper for whoami.
        '''
        return call_coroutine_threadsafe(
            self.whoami(),
            loop = self._loop,
        )
        
    async def whoami(self):
        ''' Gets our identity GHID from the hypergolix service.
        '''
        raw_ghid = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['whoami']
        )
        return Ghid.from_bytes(raw_ghid)
    
    def new_token_threadsafe(self):
        ''' Threadsafe wrapper for new_token.
        '''
        return call_coroutine_threadsafe(
            self.new_token(),
            loop = self._loop,
        )
        
    async def new_token(self):
        ''' Gets a new app_token.
        '''
        app_token = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['new_token']
        )
        self._token = app_token
        return app_token
    
    def set_token_threadsafe(self, app_token):
        ''' Threadsafe wrapper for set_token.
        '''
        return call_coroutine_threadsafe(
            self.set_token(app_token),
            loop = self._loop,
        )
        
    async def set_token(self, app_token):
        ''' Sets an existing token.
        '''
        response = await self.send(
            session = self.any_session,
            msg = app_token,
            request_code = self.REQUEST_CODES['set_token']
        )
        # If we haven't errored out...
        self._token = app_token
        return response
        
    def register_api_threadsafe(self, api_id, object_handler=None):
        return call_coroutine_threadsafe(
            self.register_api(api_id, object_handler),
            loop = self._loop,
        )
        
    async def register_api(self, api_id, object_handler=None):
        ''' Registers an API ID with the hypergolix service for this 
        application.
        
        Note that object handlers must be threadsafe.
        '''
        if len(api_id) != 65:
            raise ValueError('Invalid API ID.')
        
        if object_handler is None:
            object_handler = lambda *args, **kwargs: None
            
        if not callable(object_handler):
            raise TypeError('object_handler must be callable')
        
        response = await self.send(
            session = self.any_session,
            msg = api_id,
            request_code = self.REQUEST_CODES['register_api']
        )
        if response == b'\x01':
            self._object_handlers[api_id] = object_handler
            return True
        else:
            raise RuntimeError('Unknown error while registering API.')
        
    def get_object(self, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        response = self._get_object(ghid)
        (
            address,
            author,
            state, 
            is_link, 
            api_id, 
            app_token, 
            private, 
            dynamic, 
            _legroom
        ) = self._unpack_object_def(response)
        
        if app_token == bytes(4):
            app_token = None
            
        if is_link:
            link = Ghid.from_bytes(state)
            state = self.get_object(link)
            
        state = (state, api_id, private, dynamic, _legroom)
        
        return AppObj(
            embed = self,
            state = state,
            _preexisting = (address, author),
        )
        
    def _get_object(self, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        # Simple enough. super().get_object() will handle converting this to an
        # actual object.
        return self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['get_object']
        )
        
    def new_object(self, *args, **kwargs):
        ''' Alternative constructor for AppObj that does not require 
        passing the embed explicitly.
        '''
        # We don't need to do anything special here, since AppObj will call
        # _new_object for us.
        return AppObj(embed=self, *args, **kwargs)
        
    def _new_object(self, obj):
        ''' Handles only the creation of a new object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        
        return address, author
        '''
        
        if obj.api_id is None and not obj.private:
            raise TypeError('api_id must be defined for a non-private object.')
            
        if obj.private:
            app_token = self.app_token
        else:
            app_token = bytes(4)
            
        if obj.is_link:
            state = obj.link_address
        else:
            state = obj.state
            
        if obj.is_dynamic:
            _legroom = obj._legroom
        else:
            _legroom = None
            
        payload = self._pack_object_def(
            None,
            None,
            state,
            obj.is_link,
            obj.api_id,
            app_token,
            obj.private,
            obj.is_dynamic,
            _legroom
        )
        
        # Note that currently, we're not re-packing the api_id or app_token.
        response = self.send_threadsafe(
            session = self.any_session,
            msg = payload,
            request_code = self.REQUEST_CODES['new_object']
        )
        
        address = Ghid.from_bytes(response)
            
        # Note that the upstream ipc_host will automatically send us updates.
            
        return address, self.whoami_threadsafe()
        
    def update_object(self, obj, state):
        ''' Wrapper for obj.update.
        '''
        if not obj.is_owned:
            raise TypeError(
                'Cannot update an object that was not created by the '
                'attached Agent.'
            )
            
        # First operate on the object, since it's easier to undo local changes
        obj._update(state)
        self._update_object(obj, state)
        # Todo: add try-catch to revert update after failed upstream push
        
        return True
        
    def _update_object(self, obj, state):
        ''' Handles only the updating of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        if isinstance(state, AppObj):
            state = bytes(state.address)
        
        msg = self._pack_object_def(
            obj.address,
            None,
            state,
            obj.is_link,
            None,
            None,
            None,
            None,
            None
        )
        
        response = self.send_threadsafe(
            session = self.any_session,
            msg = msg,
            request_code = self.REQUEST_CODES['update_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while updating object.')
        
    def sync_object(self, obj):
        ''' Checks for an update to a dynamic object, and if one is 
        available, performs an update.
        '''
        obj._sync_object()
        self._sync_object(obj)

    def _sync_object(self, obj):
        ''' Handles only the syncing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        raise NotImplementedError(
            'Manual object syncing is not yet supported. Approximate it with '
            'get_object?'
        )
        if obj.is_dynamic:
            pass
        else:
            other = self._get_object(obj.address)
            if other.state != obj.state:
                raise RuntimeError(
                    'Local object state appears to be corrupted.'
                )
        
    def share_object(self, obj, recipient):
        ''' Shares an object with someone else.
        '''
        # Todo: add try-catch to undo local changes after failed upstream share
        # First operate on the object, since it's easier to undo local changes
        obj._share(recipient)
        self._share_object(obj, recipient)

    def _share_object(self, obj, recipient):
        ''' Handles only the sharing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(obj.address) + bytes(recipient),
            request_code = self.REQUEST_CODES['share_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while updating object.')
        
    def freeze_object(self, obj):
        ''' Converts a dynamic object to a static object.
        '''
        obj._freeze()
        return self._freeze_object(obj)

    def _freeze_object(self, obj):
        ''' Handles only the freezing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['freeze_object']
        )
        address = Ghid.from_bytes(response)
        return AppObj(
            embed = self,
            _preexisting = (address, obj.author),
            state = (obj.state, obj.api_id, obj.private, False, None)
        )
        
    def hold_object(self, obj):
        ''' Binds an object, preventing its deletion.
        '''
        obj._hold()
        self._hold_object(obj)

    def _hold_object(self, obj):
        ''' Handles only the holding of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['hold_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while updating object.')
        
    def discard_object(self, obj):
        ''' Attempts to delete an object. May not succeed, if another 
        Agent has bound to it.
        '''
        obj._discard()
        self._discard_object(obj)
        
    def _discard_object(self, obj):
        ''' Handles only the discarding of an object via the hypergolix
        service.
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['discard_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        del self._objs_by_ghid[obj.address]
        return True
        
    def delete_object(self, obj):
        ''' Attempts to delete an object. May not succeed, if another 
        Agent has bound to it.
        '''
        obj._delete()
        self._delete_object(obj)

    def _delete_object(self, obj):
        ''' Handles only the deleting of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['delete_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        try:
            del self._objs_by_ghid[obj.address]
        except KeyError:
            pass
        
        return True
        
    async def deliver_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        '''
        (
            address,
            author,
            state, 
            is_link, 
            api_id,
            app_token, # Will be unused and set to None 
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
        obj = AppObj(
            embed = self,
            state = (state, api_id, False, dynamic, None),
            _preexisting = (address, author)
        )
            
        # Well this is a huge hack. But something about the event loop 
        # itself is fucking up control flow and causing the world to hang
        # here.
        # TODO: fix this.
        worker = threading.Thread(
            target = self._object_handlers[api_id],
            daemon = True,
            args = (obj,),
        )
        worker.start()
        
        # Successful delivery. Return true
        return b'\x01'

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
            app_token, # Will be unused and set to None 
            private, # Will be unused and set to None 
            dynamic, # Will be unused and set to None 
            _legroom # Will be unused and set to None
        ) = self._unpack_object_def(request_body)
                
        if address in self._objs_by_ghid:
            if is_link:
                link = Ghid.from_bytes(state)
                # Note: this may cause things to freeze, because async
                state = self.get_object(link)
            
            # Still a hack.
            # TODO: fix this.
            worker = threading.Thread(
                target = self._objs_by_ghid[address]._update,
                daemon = True,
                args = (state,),
            )
            worker.start()
            
            # self._ws_loop.call_soon_threadsafe(, state)
            # self._objs_by_ghid[address]._update(state)
            
            # python tests/trashtest/trashtest_ipc_hosts.py
            
        return b'\x01'
        
    async def delete_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object deletion, and applies it to
        the object.
        '''
        ghid = Ghid.from_bytes(request_body)
        self._objs_by_ghid[ghid]._delete()
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


class AppObj:
    ''' A class for objects to be used by apps. Can be updated (if the 
    object was created by the connected Agent and is mutable) and have
    a state.
    
    AppObj instances will wrap their state in a dispatch structure 
    before updating golix containers.
    
    Can be initiated directly using a reference to an embed. May also be
    constructed from _EmbedBase.new_object.
    
    Everything here is wrapped from the messagepack dispatch format, so
    state may be more than just bytes.
    
    Unlike RawObj instances, AppObj instances are meant to be used with
    a specific API definition, and/or a specific token. Tokens can be 
    used to provide a consistent PRIVATE, freeform application state,
    whilst any objects that are being communicated to a different agent
    must use api ids.
    
    Basically, token-based AppObj instances without an api_id represent
    data that will not be shared. It can, however, be any unstructured
    data.
    
    _private isinstance bool
        if True, dispatch by token.
        if False, dispatch by api id.
        
    Note: AppObj should be set up such that there is exactly one AppObj
    per application. As such, they should probably be aware of their 
    endpoints.
    
    Note: Todo: Add some kind of automatic garbage collection mechanism
    such that, when gc'd because, for example, it is only defined within
    the scope of an AppObj state (as when linking between dynamic 
    objects), we don't continue to get updates for an object that we no
    longer retain.
    '''
    # This should define *only* ADDITIONAL slots.
    __slots__ = [
        '__weakref__',
        '_embed',
        '_is_dynamic',
        '_callbacks',
        '_inoperable',
        '_author',
        '_address',
        '_state',
        '_api_id',
        '_private',
        '_legroom',
    ]
    
    # Restore the original behavior of hash
    __hash__ = type.__hash__
    
    def __init__(self, embed, state, api_id=None, private=False, dynamic=True, 
    callbacks=None, _preexisting=None, _legroom=None, *args, **kwargs):
        ''' Create a new AppObj with:
        
        state isinstance bytes(like)
        dynamic isinstance bool(like) (optional)
        callbacks isinstance iterable of callables (optional)
        
        _preexisting isinstance tuple(like):
            _preexisting[0] = address
            _preexisting[1] = author
            
        NOTE: entirely replaces RawObj.__init__.
        '''
        # This needs to be done first so we have access to object creation
        self._init_embed(embed)
        self._inoperable = False
        
        # _preexisting was set, so we're loading an existing object.
        # "Trust" anything using _preexisting to have passed a correct value
        # for state and dynamic.
        if _preexisting is not None:
            address = _preexisting[0]
            author = _preexisting[1]
            state, api_id, private, dynamic, _legroom = state
            
        # Now do all of the common init stuff.
        self._init_dynamic(dynamic)
        self._init_legroom(_legroom)
        self._init_state(state)
        self._api_id = api_id
        self._private = private
            
        # Finally, set the callbacks. Will error if inappropriate def (ex: 
        # attempt to register callbacks on static object)
        self._init_callbacks(callbacks)
            
        # Now that we have successfully created a new AppObj, if this is a new
        # object, let's update upstream. Split this from above so we can do the
        # middle (dangerous) stuff without ever calling this.
        if _preexisting is None:
            # This creates an actual Golix object via hypergolix service.
            address, author = self._embed._new_object(obj=self)
                
        # These bits should always work, so we don't need to worry about it.
        self._author = author
        self._address = address
        
        # Todo: add (python) gc logic such that this is removed when the object
        # is removed as well. Note that we currently DO do this in delete, but
        # not on actual python gc.
        self._embed._objs_by_ghid[address] = self
        
    @property
    def author(self):
        ''' The ghid address of the agent that created the object.
        '''
        return self._author
        
    @property
    def address(self):
        ''' The ghid address of the object itself.
        '''
        return self._address
            
    @property
    def private(self):
        ''' Return the (immutable) property describing whether this is
        a private application object, or a sharable api-id-dispatched
        object.
        '''
        return self._private
        
    @property
    def api_id(self):
        ''' The api_id (if one exists) of the object. Private objects
        may or may not omit this.
        '''
        return self._api_id
        
    @property
    def callbacks(self):
        if self.is_dynamic:
            return self._callbacks
        else:
            raise TypeError('Static objects cannot have callbacks.')
            
    @property
    def is_dynamic(self):
        ''' Indicates whether this object is dynamic.
        returns True/False.
        '''
        return self._is_dynamic
        
    @property
    def is_owned(self):
        ''' Indicates whether this object is owned by the associated 
        Agent.
        
        returns True/False.
        '''
        return self._embed.whoami_threadsafe() == self.author
            
    @property
    def mutable(self):
        ''' Returns true if and only if self is a dynamic object and is
        owned by the current agent.
        '''
        return self.is_dynamic and self.is_owned
        
    @property
    def is_link(self):
        if self.is_dynamic:
            if isinstance(self._state[0], AppObj):
                return True
            else:
                return False
        else:
            return None
            
    @property
    def link_address(self):
        ''' Only available when is_link is True. Otherwise, will return
        None.
        '''
        if self.is_dynamic and self.is_link:
            return self._state[0].address
        else:
            return None
        
    @property
    def state(self):
        if self._inoperable:
            raise ValueError('Object has already been deleted.')
        elif self.is_dynamic:
            if self.is_link:
                # Recursively resolve any nested/linked objects
                return self._state[0].state
            else:
                return self._state[0]
        else:
            return self._state
            
    @state.setter
    def state(self, value):
        ''' Wraps update() for dynamic objects. Attempts to directly set
        self._state for static objects, but will return AttributeError
        if state is already set.
        '''
        if self.is_dynamic:
            self.update(value)
            
        else:
            # Note that we don't need a LBYL for existing state, since our
            # modified __setattr__ will prevent updates
            try:
                # Typecheck AppObj specifically, since it is not allowed for
                # static objects, but declaring it will create problems for the
                # packing and unpacking.
                if isinstance(value, AppObj):
                    raise ValueError(
                        'Static objects cannot link to other objects.'
                    )
                self._state = value
            except AttributeError as e:
                raise AttributeError(
                    'Cannot update the state of a static object.'
                ) from e
        
    def add_callback(self, callback):
        ''' Registers a callback to be called when the object receives
        an update.
        
        callback must be hashable and callable. Function definitions and
        lambdas are natively hashable; callable classes may not be.
        
        On update, callbacks are passed the object.
        '''
        if not self.is_dynamic:
            raise TypeError('Static objects cannot register callbacks.')
        if not callable(callback):
            raise TypeError('Callback must be callable.')
        self._callbacks.add(callback)
        
    def remove_callback(self, callback):
        ''' Removes a callback.
        
        Raises KeyError if the callback has not been registered.
        '''
        if self.is_dynamic:
            if callback in self._callbacks:
                self._callbacks.remove(callback)
            else:
                raise KeyError(
                    'Callback not found in dynamic obj callback set.'
                )
        else:
            raise TypeError('Static objects cannot have callbacks.')
        
    def clear_callbacks(self):
        ''' Resets all callbacks.
        '''
        if self.is_dynamic:
            self._callbacks.clear()
        # It's meaningless to call this on a static object, but there's also 
        # no need to error out
            
    def update(self, state):
        ''' Updates a mutable object to a new state.
        
        May only be called on a dynamic object that was created by the
        attached Agent.
        
        If _preexisting is True, this is an update coming down from a
        persister, and we will NOT push it upstream.
        '''
        if not self.is_owned:
            raise TypeError(
                'Cannot update an object that was not created by the '
                'attached Agent.'
            )
            
        # First operate on the object, since it's easier to undo local changes
        self._update(state)
        self._embed._update_object(self, state)
        # Todo: add try-catch to revert update after failed upstream push
            
    def _update(self, state):
        ''' Handles the actual updating **for the object only.** Does 
        not update or involve the embed.
        '''
        if self._inoperable:
            raise ValueError('Object has already been deleted.')
            
        if not self.is_dynamic:
            raise TypeError('Cannot update a static object.')
            
        # Update local state.
        self._state.appendleft(state)
        
        # Todo: figure out some mechanism to delay this until confirmed upsream
        # successful push
        # INFORM THE OTHERS
        for callback in self.callbacks:
            callback(self)
            
    def sync(self, *args):
        ''' Checks the current state matches the state at the connected
        Agent. If this is a dynamic and an update is available, do that.
        If it's a static and the state mismatches, raise error.
        '''
        self._embed._sync_object(self)
        self._sync()
            
    def _sync(self, *args):
        ''' Handles the actual syncing **for the object only.** Does not
        update or involve the embed.
        '''
        pass
            
    def share(self, recipient):
        ''' Public accessor for sharing via object.
        '''
        # Todo: add try-catch to undo local changes after failed upstream share
        # First operate on the object, since it's easier to undo local changes
        self._share(recipient)
        self._embed._share_object(self, recipient)
            
    def _share(self, recipient):
        ''' Handles the actual sharing **for the object only.** Does not
        update or involve the embed.
        
        This prevents the sharing of private objects.
        
        Overriding this without calling super() may result in security
        risks for applications.
        '''
        # Note: should this be moved into the Embed? That might be a little bit
        # safer. Or, should the hypergolix service check to make sure nothing
        # with an app_token is shared?
        if self.private:
            raise TypeError('Private application objects cannot be shared.')
        else:
            return True
        
    def freeze(self):
        ''' Creates a static snapshot of the dynamic object. Returns a 
        new static RawObj instance. Does NOT modify the existing object.
        May only be called on dynamic objects. 
        
        Note: should really be reimplemented as a recursive resolution
        of the current container object, and then a hold on that plus a
        return of a static RawObj version of that. This is pretty buggy.
        
        Note: does not currently traverse nested dynamic bindings, and
        will probably error out if you attempt to freeze one.
        '''
        self._freeze()
        return self._embed._freeze_object(self)
            
    def _freeze(self):
        ''' Handles the actual freezing **for the object only.** Does 
        not update or involve the embed.
        '''
        if not self.is_dynamic:
            raise TypeError(
                'Static objects cannot be frozen. If attempting to save them, '
                'call hold instead.'
            )
        
    def hold(self):
        ''' Binds to the object, preventing its deletion.
        '''
        self._hold()
        self._embed.hold_object(self)
            
    def _hold(self):
        ''' Handles the actual holding **for the object only.** Does not
        update or involve the embed.
        '''
        pass
        
    def discard(self):
        ''' Tells the hypergolix service that the application is done 
        with the object, but does not directly delete it. No more 
        updates will be received.
        '''
        self._discard()
        self._embed._discard_object(self)
        
    def _discard(self):
        ''' Performs AppObj actions necessary to discard.
        '''
        self.clear_callbacks()
        super().__setattr__('_inoperable', True)
        super().__setattr__('_is_dynamic', None)
        super().__setattr__('_author', None)
            
    def delete(self):
        ''' Tells any persisters to delete. Clears local state. Future
        attempts to access will raise ValueError, but does not (and 
        cannot) remove the object from memory.
        '''
        self._delete()
        self._embed._delete_object(self)
            
    def _delete(self):
        ''' Handles the actual deleting **for the object only.** Does 
        not update or involve the embed.
        '''
        self._discard()
        # This creates big problems down the road.
        # super().__setattr__('_embed', None)
    
    # This might be a little excessive, but I guess it's nice to have a
    # little extra protection against updates?
    def __setattr__(self, name, value):
        ''' Prevent rewriting declared attributes in slots. Does not
        prevent assignment using @property.
        
        Note: if this gets removed, or re-assingment is otherwise
        implemented, you will need to add a check for overwriting an 
        existing state in static objects.
        '''
        if name in self.__slots__:
            try:
                __ = getattr(self, name)
            except AttributeError:
                pass
            else:
                raise AttributeError(
                    'RawObj internals cannot be changed once they have been '
                    'declared. They must be mutated instead.'
                )
                
        super().__setattr__(name, value)
            
    def __delattr__(self, name):
        ''' Prevent deleting declared attributes.
        '''
        raise AttributeError(
            'RawObj internals cannot be changed once they have been '
            'declared. They must be mutated instead.'
        )
        
    def __eq__(self, other):
        if not isinstance(other, AppObj):
            raise TypeError(
                'Cannot compare RawObj instances to incompatible types.'
            )
            
        # Short-circuit if dynamic mismatches
        if self.is_dynamic != other.is_dynamic:
            return False
            
        meta_comparison = (
            # Don't compare is_owned, because we want shared objects to be ==
            # Don't compare app_token, because we want shared objects to be ==
            self.api_id == other.api_id and
            self.address == other.address and
            self.author == other.author
        )
        
        # If dynamic, state comparison looks at as many state shots as we share
        if self.is_dynamic:
            state_comparison = True
            comp = zip(self._state, other._state)
            for a, b in comp:
                state_comparison &= (a == b)
                
        # If static, state comparison simply looks at both states directly
        else:
            state_comparison = (self.state == other.state)
            
        # Return the result of the whole comparison
        return meta_comparison and state_comparison
            
    def _init_embed(self, embed):
        ''' Typechecks embed and them creates a weakref to it.
        '''
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(embed, weakref.ProxyTypes):
            self._embed = embed
        else:
            self._embed = weakref.proxy(embed)
            
    def _init_dynamic(self, dynamic):
        ''' Sets whether or not we're dynamic based on dynamic.
        '''
        if dynamic:
            self._is_dynamic = True
        else:
            self._is_dynamic = False
            
    def _init_legroom(self, legroom):
        ''' Sets up our legroom. If not defined, then default to the 
        embed's preference.
        '''
        # Only proceed to define legroom if this is dynamic.
        if self.is_dynamic:
            # Legroom is None. Infer it from the dispatch.
            if legroom is None:
                legroom = self._embed._legroom
                
            self._legroom = legroom
        
    def _init_state(self, state):
        ''' Makes the first state commit for the object, regardless of
        whether or not the object is new or loaded. Even dynamic objects
        are initially loaded with a single frame of history.
        '''
        if self.is_dynamic:
            self._state = collections.deque(
                maxlen = self._legroom
            )
            self._state.appendleft(state)
        else:
            self._state = state
            
    def _init_callbacks(self, callbacks):
        ''' Initializes callbacks.
        '''
        # Only proceed if dynamic, and if callbacks is defined.
        if self.is_dynamic:
            self._callbacks = set()
            
            if callbacks is not None:
                for callback in callbacks:
                    self.add_callback(callback)
        
    # def collate(self):
        # ''' Converts state (and any ancillary information) into a format
        # that can be packed by the embed. Mostly here to allow subclasses
        # to intelligently extend the AppObj class.
        # '''
        # return self.state
        
    # def decollate(self, collated):
        # ''' Handles any ancillary information required by subclasses of
        # AppObj. MUST return the state of the object. Has access to all
        # of the AppObj except the author and address.
        # '''
        # return collated
        
        
        



# ###############################################
# Deprecated, but not yet excised
# ###############################################
        
        
        
class _TestEmbed:
    def register_api(self, api_id, object_handler=None):
        ''' Just here to silence errors from ABC.
        '''
        if object_handler is None:
            object_handler = lambda *args, **kwargs: None
            
        self._object_handlers[api_id] = object_handler
    
    def get_object(self, ghid):
        ''' Wraps RawObj.__init__  and get_ghid for preexisting objects.
        '''
        author, is_dynamic, state = self.get_ghid(ghid)
            
        return RawObj(
            # Todo: make the dispatch more intelligent
            dispatch = self,
            state = state,
            dynamic = is_dynamic,
            _preexisting = (ghid, author)
        )
        
    def new_object(self, state, dynamic=True, _legroom=None):
        ''' Creates a new object. Wrapper for RawObj.__init__.
        '''
        return RawObj(
            # Todo: update dispatch intelligently
            dispatch = self,
            state = state,
            dynamic = dynamic,
            _legroom = _legroom
        )
        
    def update_object(self, obj, state):
        ''' Updates a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        Wraps RawObj.update and modifies the dynamic object in place.
        
        Could add a way to update the legroom parameter while we're at
        it. That would need to update the maxlen of both the obj._buffer
        and the self._historian.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Obj must be an RawObj.'
            )
            
        obj.update(state)
        
    def sync_object(self, obj):
        ''' Wraps RawObj.sync.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError('Must pass RawObj or subclass to sync_object.')
            
        return obj.sync()
        
    def hand_object(self, obj, recipient):
        ''' DEPRECATED.
        
        Initiates a handshake request with the recipient to share 
        the object.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Obj must be a RawObj or similar.'
            )
    
        # This is, shall we say, suboptimal, for dynamic objects.
        # frame_ghid = self._historian[obj.address][0]
        # target = self._dynamic_targets[obj.address]
        target = obj.address
        self.hand_ghid(target, recipient)
        
    def share_object(self, obj, recipient):
        ''' Currently, this is just calling hand_object. In the future,
        this will have a devoted key exchange subprotocol.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Only RawObj may be shared.'
            )
        return self.hand_ghid(obj.address, recipient)
        
    def freeze_object(self, obj):
        ''' Wraps RawObj.freeze. Note: does not currently traverse 
        nested dynamic bindings.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Only RawObj may be frozen.'
            )
        return obj.freeze()
        
    def hold_object(self, obj):
        ''' Wraps RawObj.hold.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError('Only RawObj may be held by hold_object.')
        obj.hold()
        
    def delete_object(self, obj):
        ''' Wraps RawObj.delete. 
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Obj must be RawObj or similar.'
            )
            
        obj.delete()
    
    @property
    @abc.abstractmethod
    def whoami(self):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def new_static(self, state):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def new_dynamic(self, state):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def update_dynamic(self, obj, state):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def freeze_dynamic(self, obj):
        ''' Inherited from Agent.
        '''
        pass
        
    def _get_object(self, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        pass
        
    def _new_object(self, obj):
        ''' Handles only the creation of a new object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        
        return address, author
        '''
        pass
        
    def _update_object(self, obj, state):
        ''' Handles only the updating of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _sync_object(self, obj):
        ''' Handles only the syncing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _share_object(self, obj, recipient):
        ''' Handles only the sharing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _freeze_object(self, obj):
        ''' Handles only the freezing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _hold_object(self, obj):
        ''' Handles only the holding of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _delete_object(self, obj):
        ''' Handles only the deleting of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass