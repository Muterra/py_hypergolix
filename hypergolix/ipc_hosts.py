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

from golix import Guid






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

from .comms import WSReqResServer
from .comms import _ReqResWSConnection


class _EndpointBase(metaclass=abc.ABCMeta):
    ''' Base class for an endpoint. Defines everything needed by the 
    Integration to communicate with an individual application.
    
    ENDPOINTS HAVE A 1:1 CORRELATION WITH APPLICATION TOKENS. A token
    denotes a singular application, and one endpoint is used for one
    application.
    
    Note: endpoints have all the necessary information to wrap objects
    in the messagepack definition of their api_id, etc.
    
    Note on messagepack optimization: in the future, can change pack and
    unpack to look specifically (and only) at the relevant keys. Check
    out read_map_header.
    
    Alternatively, might just treat the whole appdata portion as a 
    nested binary file, and unpack that separately.
    '''
    def __init__(self, dispatch, app_token=None, apis=None, *args, **kwargs):
        ''' Creates an endpoint for the specified agent that handles the
        associated apis. Apis is an iterable of api_ids. If token is not
        specified, generates a new one from dispatch.
        '''
        super().__init__(*args, **kwargs)
        
        self._dispatch = weakref.proxy(dispatch)
        self._expecting_exchange = threading.Lock()
        self._known_guids = set()
        
        if app_token is None:
            app_token = self.dispatch.new_token()
            
        self._token = app_token
        self._apis = set()
        
        if apis is not None:
            for api in apis:
                self.add_api(api)
            
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
        
    def notify_object(self, obj):
        ''' Notifies the endpoint that the object is available. May be
        either a new object, or an updated one.
        
        Checks to make sure we're not currently expecting and update to
        suppress.
        '''
        # if not self._expecting_exchange.locked():
        if obj.address in self._known_guids:
            self.send_update(obj)
        else:
            self._known_guids.add(obj.address)
            self.send_object(obj)
    
    @abc.abstractmethod
    def send_object(self, obj):
        ''' Sends a new object to the emedded client. This originates 
        upstream and is not solicited by the client.
        '''
        pass
    
    @abc.abstractmethod
    def send_update(self, obj):
        ''' Sends an updated object to the emedded client. Originates 
        upstream and is not solicited by the client.
        '''
        pass
        
    @abc.abstractmethod
    def notify_share_failure(self, obj, recipient):
        ''' Notifies the embedded client of an unsuccessful share.
        '''
        pass
        
    @abc.abstractmethod
    def notify_share_success(self, obj, recipient):
        ''' Notifies the embedded client of a successful share.
        '''
        pass
        
        
class _TestEndpoint(_EndpointBase):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__name = name
        self._assigned_objs = []
        self._failed_objs = []
        
    def send_object(self, obj):
        self._assigned_objs.append(obj)
        print('Endpoint ', self.__name, ' incoming: ', obj)
        
    def send_update(self, obj):
        self._assigned_objs.append(obj)
        print('Endpoint ', self.__name, ' updated: ', obj)
        
    def notify_share_failure(self, obj, recipient):
        self._failed_objs.append(obj)
        print('Endpoint ', self.__name, ' failed: ', obj)
        
    def notify_share_success(self, obj, recipient):
        self._assigned_objs.append(obj)
        print('Endpoint ', self.__name, ' success: ', obj)


class _IPCBase(IPCPackerMixIn, metaclass=abc.ABCMeta):
    ''' Base class for an IPC mechanism. Note that an _IPCBase cannot 
    exist without also being an agent. They are separated to allow 
    mixing-and-matching agent/persister/IPC configurations.
    
    Could subclass _EndpointBase to ensure that we can use self as an 
    endpoint for incoming messages. To prevent spoofing risks, anything
    we'd accept this way MUST be append-only with a very limited scope.
    Or, we could just handle all of our operations directly with the 
    agent bootstrap object. Yeah, let's do that instead.
    '''
    def __init__(self, dispatch, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Don't weakref.proxy, since dispatchers never contact the ipcbase,
        # just the endpoints.
        self._dispatch = dispatch
        
    @property
    def dispatch(self):
        ''' Sorta superfluous right now.
        '''
        return self._dispatch
        
    # @abc.abstractmethod
    # def new_endpoint(self):
    #     ''' Creates a new endpoint for the IPC system. Endpoints must
    #     be unique. Uniqueness must be enforced by subclasses of the
    #     _IPCBase class.
        
    #     Returns an Endpoint object.
    #     '''
    #     pass
        
    def add_api_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        if len(request_body) != 65:
            raise ValueError('Invalid API ID format.')
        endpoint.add_api(request_body)
        return b'\x01'
        
    def whoami_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        guid = self.dispatch.whoami
        return bytes(guid)
        
    def get_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        guid = Guid.from_bytes(request_body)
        # This will return a DispatchObj
        obj = self.dispatch.get_object(guid)
        
        # Todo: Note: need to add callback for subscriptions, right?
        
        if obj.app_token is None:
            app_token = bytes(4)
            private = False
        else:
            app_token = obj.app_token
            private = True
        
        if obj.is_link:
            state = obj.link_address
        else:
            state = obj.state
            
        if obj.is_dynamic:
            _legroom = obj._state.maxlen
        else:
            _legroom = None
        
        return self._pack_object_def(
            obj.address,
            obj.author,
            state,
            obj.is_link,
            obj.api_id,
            app_token,
            private,
            obj.is_dynamic,
            _legroom
        )
        
    def new_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        # Create the object upstream
            # If we're dynamic, subscribe to any updates.
            # if self.is_dynamic:
            #     # This feels gross. Also, it's not really un-sub-able
            #     # Also, it breaks if we use this in AppObj.
            #     self._embed.persister.subscribe(self.address, self.sync)
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
            # Note: this is a really silly workaround as a substitute to 
            # re-writing a bunch of other code. #technicaldebt
            # TODO: FIX THIS!
            link = Guid.from_bytes(state)
            author = link
            state = RawObj(
                dispatch = self.dispatch, 
                # State is never used.
                state = bytes(),
                # This prevents recursive state lookup to non-existing objects
                dynamic = False,
                # Really the only thing we need in this entire chain is link.
                _preexisting = (link, author),
            )
            
        if dynamic:
            callbacks = (endpoint.send_update,)
            
        else:
            callbacks = None
        
        obj = self.dispatch.new_object(
            # This is inefficient but, well, whatever
            state = state, 
            api_id = api_id, 
            app_token = app_token, 
            dynamic = dynamic,
            callbacks = callbacks,
            _legroom = _legroom
        )
        
        return bytes(obj.address)
        
    def sync_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        return b''
        
    def update_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        return b''
        
    def share_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        return b''
        
    def freeze_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        return b''
        
    def hold_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        return b''
        
    def delete_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        return b''
        
        
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
        
        
class WSEndpoint(_EndpointBase, _ReqResWSConnection):
    def send_object(self, obj):
        ''' Sends a new object to the emedded client.
        '''
        pass
    
    def send_update(self, obj):
        ''' Sends an updated object to the emedded client.
        '''
        pass
        
    def notify_share_failure(self, obj, recipient):
        ''' Notifies the embedded client of an unsuccessful share.
        '''
        pass
        
    def notify_share_success(self, obj, recipient):
        ''' Notifies the embedded client of a successful share.
        '''
        pass
    
    
class WebsocketsIPC(_IPCBase, WSReqResServer):
    ''' Websockets IPC via localhost. Sets up a server.
    '''
    def __init__(self, *args, **kwargs):
        req_handlers = {
            # New app tokens are handled during endpoint creation.
            # # Get new app token
            # b'+T': self.new_token_wrapper,
            # # Register existing app token
            # b'@T': self,
            # Register an API
            b'@A': self.add_api_wrapper,
            # Whoami?
            b'?I': self.whoami_wrapper,
            # Get object
            b'<O': self.get_object_wrapper,
            # New object
            b'>O': self.new_object_wrapper,
            # Sync object
            b'~O': self.sync_object_wrapper,
            # Update object
            b'!O': self.update_object_wrapper,
            # Share object
            b'^O': self.share_object_wrapper,
            # Freeze object
            b'*O': self.freeze_object_wrapper,
            # Hold object
            b'#O': self.hold_object_wrapper,
            # Delete object
            b'XO': self.delete_object_wrapper,
        }
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            # Note: can also add error_lookup = {b'er': RuntimeError}
            *args, **kwargs
        )
        
    def new_connection(self, app_token=None, apis=None, *args, **kwargs):
        ''' Merge the connection and endpoint to the same thing.
        '''
        return WSEndpoint(
            dispatch = self.dispatch, 
            app_token=None, 
            apis=None, 
            *args, **kwargs
        )
        
    @asyncio.coroutine
    def init_connection(self, websocket, path):
        ''' Initializes the connection with the client, creating an 
        endpoint/connection object, and registering it with dispatch.
        '''
        # First command on the wire MUST be registering the application.
        msg = yield from websocket.recv()
        # This insulates us from unpacking problems during the except bit
        req_token = 0
        try:
            version, req_token, req_code, body = self._unpack_request(msg)
            
            # New app requesting a new token.
            if req_code == b'+T':
                app_token = self.dispatch.new_token()
            # Existing app registering existing token.
            elif req_code == b'@T':
                app_token = body[0:4]
            else:
                raise ValueError('Improper handshake command.')
            
            connection = yield from super().init_connection(
                websocket = websocket, 
                path = path, 
                app_token = app_token
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
            yield from websocket.send(reply)
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
            yield from websocket.send(reply)
            
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
    
    
class PipeIPC(_IPCBase):
    pass