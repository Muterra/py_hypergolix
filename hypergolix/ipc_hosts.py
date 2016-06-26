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
    'LocalhostIPC'
]

# External dependencies
import abc
import msgpack
import os
import warnings
import weakref
import threading

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
from .utils import call_coroutine_threadafe

from .comms import _WSConnection
from .comms import _AutoresponderSession
from .comms import WSBasicServer
from .comms import Autoresponder
from .comms import Autocomms


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


class IPCHostEndpoint(metaclass=abc.ABCMeta):
    ''' An application endpoints, as used by IPC hosts. Must only be 
    created from within an event loop!
    
    ENDPOINTS HAVE A 1:1 CORRELATION WITH APPLICATION TOKENS. A token
    denotes a singular application, and one endpoint is used for one
    application.
    
    TODO: move object methods into the IPC host. Also a note about them:
    since they're only ever called by the dispatch, which doesn't know 
    about the endpoint until it has been successfully registered, we 
    don't need to check for the app token.
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
        
    def notify_object(self, ghid, state):
        ''' Notifies the endpoint that the object is available. May be
        either a new object, or an updated one.
        
        Checks to make sure we're not currently expecting and update to
        suppress.
        '''
        # if not self._expecting_exchange.locked():
        if ghid in self._known_ghids:
            self.send_update(ghid, state)
        else:
            self.register_ghid(ghid)
            self.send_object(ghid, state)
            
    def register_ghid(self, ghid):
        ''' Pretty simple wrapper to make sure we know about the ghid.
        '''
        self._known_ghids.add(ghid)
        
    def send_object_threadsafe(self, ghid, state):
        return call_coroutine_threadsafe(
            coro = self.send_object(ghid, state),
            loop = self.ipc._loop
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
        
        response = await self.ipc.send(
            session = self,
            msg = self.ipc._pack_object_def(
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
            request_code = self.ipc.REQUEST_CODES['send_object'],
            # Note: for now, just don't worry about failures.
            # Todo: expect reply, and enclose within try/catch to prevent apps
            # from crashing the service.
            expect_reply = False
        )
    
    def send_update_threadsafe(self, ghid, state):
        return call_coroutine_threadsafe(
            coro = self.send_update(ghid, state),
            loop = self.ipc._loop
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
        
        response = await self.ipc.send(
            session = self,
            msg = self.ipc._pack_object_def(
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
            request_code = self.ipc.REQUEST_CODES['send_update'],
            # Note: for now, just don't worry about failures. See previous note
            expect_reply = False
        )
        # print('Update sent and resuming life.')
        # if response == b'\x01':
        #     return True
        # else:
        #     raise RuntimeError('Unknown error while delivering object update.')
    
    def send_delete_threadsafe(self, ghid, state):
        return call_coroutine_threadsafe(
            coro = self.send_delete(ghid),
            loop = self.ipc._loop
        )
        
    async def send_delete(self, ghid):
        ''' Notifies the endpoint that the object has been deleted 
        upstream.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('ghid must be type Ghid or similar.')
        
        response = await self.ipc.send(
            session = self,
            msg = bytes(ghid),
            request_code = self.ipc.REQUEST_CODES['send_delete'],
            # Note: for now, just don't worry about failures.
            expect_reply = False
        )
        # print('Update sent and resuming life.')
        # if response == b'\x01':
        #     return True
        # else:
        #     raise RuntimeError('Unknown error while delivering object update.')
    
    def notify_share_failure_threadsafe(self, ghid, recipient):
        return call_coroutine_threadsafe(
            coro = self.notify_share_failure(ghid, state),
            loop = self.ipc._loop
        )
        
    async def notify_share_failure(self, ghid, recipient):
        ''' Notifies the embedded client of an unsuccessful share.
        '''
        pass
    
    def notify_share_success_threadsafe(self, ghid, recipient):
        return call_coroutine_threadsafe(
            coro = self.notify_share_success(ghid, state),
            loop = self.ipc._loop
        )
        
    async def notify_share_success(self, ghid, recipient):
        ''' Notifies the embedded client of a successful share.
        '''
        pass


class IPCHost(IPCPackerMixIn, metaclass=abc.ABCMeta):
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
        endpoint.app_token = app_token
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
        
        
class _EmbeddedIPC(_IPCBase):
    ''' EmbeddedIPC wraps _EmbedBase from embeds. It also 
    is its own endpoint (or has its own endpoint). It therefore fulfills
    all of the requirements for _EmbedBase.
    '''
    def new_endpoint(self):
        ''' Creates a new endpoint for the IPC system. Endpoints must
        be unique. Uniqueness must be enforced by subclasses of the
        _IPCBase class.
        
        Returns an Endpoint object.
        '''
        pass
    
    
class PipeIPC(_IPCBase):
    pass
           
           

        












class WSAutoServer:
    def __init__(self, host, port, req_handlers, success_code, failure_code, 
    error_lookup=None, birthday_bits=40, debug=False, aengel=True, 
    connection_class=None, *args, **kwargs):
        pass
class WSAutoClient:
    def __init__(self, host, port, req_handlers, success_code, failure_code, 
    error_lookup=None, debug=False, aengel=True, connection_class=None, 
    *args, **kwargs):
        pass
    
    
class WebsocketsIPC(_IPCBase, WSAutoServer):
    ''' Websockets IPC via localhost. Sets up a server.
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
    
    def __init__(self, *args, **kwargs):
        req_handlers = {
            # New app tokens are handled during endpoint creation.
            # # Get new app token
            # b'+T': self.new_token_wrapper,
            # # Register existing app token
            # b'$T': self,
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
            connection_class = connection_class,
            # Note: can also add error_lookup = {b'er': RuntimeError}
            *args, **kwargs
        )
        
    @property
    def connection_factory(self):
        ''' Proxy for connection factory to allow saner subclassing.
        '''
        return WSEndpoint
        
    async def new_connection(self, websocket, path, *args, **kwargs):
        ''' Initializes the connection with the client, creating an 
        endpoint/connection object, and registering it with dispatch.
        '''
        # First command on the wire MUST be registering the application.
        msg = await websocket.recv()
        # This insulates us from unpacking problems during the except bit
        req_token = 0
        try:
            version, req_token, req_code, body = self._unpack_request(msg)
            
            # New app requesting a new token.
            if req_code == b'+T':
                app_token = self.dispatch.new_token()
            # Existing app registering existing token.
            elif req_code == b'$T':
                app_token = body[0:4]
            else:
                raise ValueError('Improper handshake command.')
            
            connection = await super().new_connection(
                websocket = websocket, 
                path = path, 
                app_token = app_token,
                *args, **kwargs
            )
            self.dispatch.register_endpoint(connection)
            
        # If anything there went wrong, notify the app and then terminate.
        except Exception as e:
            # Send a failure nak and reraise.
            reply = self._pack_request(
                version = self._version,
                token = 0,
                req_code = self._failure_code,
                body = self.pack_failure(
                    their_token = req_token,
                    exc = e
                )
            )
            await websocket.send(reply)
            raise
            
        # Nothing went wrong, so notify the app and continue.
        else:
            # Send a success message and continue.
            reply = self._pack_request(
                version = self._version,
                token = 0,
                req_code = self._success_code,
                body = self.pack_success(
                    their_token = req_token,
                    data = app_token
                )
            )
            await websocket.send(reply)
            
        print('Connection established with embedded client', str(app_token))
        return connection
        
    @asyncio.coroutine
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_autoresponder_exc(self, exc, token):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
        return repr(exc)