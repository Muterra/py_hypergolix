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

from .utils import AppObj

from .comms import WSReqResServer


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
            
        self._token = token
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
        ''' Sends a new object to the emedded client.
        '''
        pass
    
    @abc.abstractmethod
    def send_update(self, obj):
        ''' Sends an updated object to the emedded client.
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


class _MsgpackEndpointBase(_EndpointBase):
    ''' Base class for an endpoint. Defines everything needed by the 
    Integration to communicate with an individual application.
    
    Note: endpoints have all the necessary information to wrap objects
    in the messagepack definition of their api_id, etc.
    
    Note on messagepack optimization: in the future, can change pack and
    unpack to look specifically (and only) at the relevant keys. Check
    out read_map_header.
    
    Alternatively, might just treat the whole appdata portion as a 
    nested binary file, and unpack that separately.
    '''
    def pack_command(self, command, obj):
        ''' Packs an object/command pair for transit.
        '''
        if not isinstance(obj, AppObj):
            raise TypeError('obj must be appobj.')
            
        if command not in {
            'send_object', 
            'send_update',
            'notify_share_failure', 
            'notify_share_success', 
        }:
            raise ValueError('Invalid command.')
        
        # Should token not be used? Is leaking the app_token a security risk?
        # I mean, if it's going to someone who doesn't already have it, we 
        # majorly fucked up, but what about damage mitigation? On the other
        # hand, what if we have (in the future) a secure way to use a single
        # endpoint for multiple applications? (admittedly that seems unlikely).
        obj_pack = {
            # 'app_token': obj.app_token,
            'state': obj.state,
            'api_id': obj.api_id,
            'private': obj.private,
            'dynamic': obj.is_dynamic,
            'address': obj.address,
            'author': obj.author,
            # # Ehh, let's hold off on this for now
            # legroom = obj._state.maxlen
        }
        
        return msgpack.packb(
            {
                'command': command,
                'obj': obj_pack,
            },
            use_bin_type = True    
        )
        
    def unpack_command(self, packed):
        ''' Packs an object/command pair for transit.
        '''
        unpacked = msgpack.unpackb(packed, encoding='utf-8')

        # This bit cleverly selects the command.
        command = unpacked['command']
        try:
            command = {
                'register_api': self.register_api,
                'whoami': self.whoami,
                'get_object': self.get_object,
                'new_object': self.new_object,
                'update_object': self.update_object,
                'sync_object': self.sync_object,
                'share_object': self.share_object,
                'freeze_object': self.freeze_object,
                'hold_object': self.hold_object,
                'delete_object': self.delete_object
            }[command]
        except KeyError as e:
            raise IPCError('Invalid command.') from e
        
        args = unpacked['args']
        kwargs = unpacked['kwargs']
        
        
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


class _IPCBase(metaclass=abc.ABCMeta):
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
        ''' Sorta superluous right now.
        '''
        return self._dispatch
        
    @abc.abstractmethod
    def new_endpoint(self):
        ''' Creates a new endpoint for the IPC system. Endpoints must
        be unique. Uniqueness must be enforced by subclasses of the
        _IPCBase class.
        
        Returns an Endpoint object.
        '''
        pass
        
    def new_token_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def add_api_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def whoami_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def get_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def new_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def sync_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def update_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def share_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def freeze_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def hold_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
    def delete_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        '''
        pass
        
        
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
        
        
class WSEndpoint(_EndpointBase):
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
            # Get new app token
            b'+T': self.new_token_wrapper,
            # # Register existing app token
            # b'@T': self,
            # Register an API
            b'@A': self.add_api_wrapper,
            # Whoami?
            b'?I': self.whoami_wrapper,
            # Get object
            b'>O': self.get_object_wrapper,
            # New object
            b'<O': self.new_object_wrapper,
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
        
    def new_endpoint(self, connid, app_token=None, apis=None):
        ''' Creates a new endpoint for the IPC system. Endpoints must
        be unique. Uniqueness must be enforced by subclasses of the
        _IPCBase class.
        
        Returns an Endpoint object.
        '''
        return WSEndpoint(
            dispatch = self.dispatch,
            app_token = app_token,
            apis = apis,
        )
        
    def __trash_handle_new_app_registration(self):
        ''' Scratchpad for how to handle new connections.
        '''
        connection_id = get_connection_id()
        endpoint = self.new_endpoint(connection_id)
        self.dispatch.register_endpoint(endpoint)
        
    @asyncio.coroutine
    def init_connection(self, websocket, connid):
        ''' Does anything necessary to initialize a connection. Has 
        access to self.connections[connid], which will contain None.
        '''
        # First command on the wire MUST be registering the application.
        # msg = yield from websocket.recv()
        pass
        
    @asyncio.coroutine
    def producer(self, connid):
        ''' Produces anything needed to send to the connection indicated
        by connid. Must return bytes.
        '''
        pass
        
    @asyncio.coroutine
    def consumer(self, msg, connid):
        ''' Consumes the msg produced by the websockets receiver 
        listening at connid.
        '''
        pass
        
    @asyncio.coroutine
    def _request_handler(self, websocket, exchange_lock, dispatch_lookup):
        ''' Handles incoming requests from the websocket in parallel 
        with outgoing subscription updates.
        '''
        rcv = yield from websocket.recv()
            
        # Acquire the exchange lock so we can guarantee an orderly response
        # print('Getting exchange lock for request.')
        yield from exchange_lock
        try:
            framed = memoryview(rcv)
            header = bytes(framed[0:2])
            body = framed[2:]
            
            # This will automatically dispatch the request based on the
            # two-byte header
            response_preheader = dispatch_lookup[header](body)
            # print('-----------------------------------------------')
            print('Successful ', header, ' ', bytes(body[:4]))
            response = self._frame_response(response_preheader)
            # print(bytes(body))
                
        except Exception as e:
            # We should log the exception, but exceptions here should never
            # be allowed to take down a server.
            response_preheader = False
            response_body = (str(e)).encode('utf8')
            response = self._frame_response(
                preheader = response_preheader, 
                body = response_body
            )
            print('Failed to dispatch ', header, ' ', bytes(body[:4]))
            # print(rcv)
            # print(repr(e))
            # traceback.print_tb(e.__traceback__)
            
        finally:
            yield from websocket.send(response)
            exchange_lock.release()
        # print('Released exchange lock for request.')
            
    @asyncio.coroutine
    def _update_handler(self, websocket, exchange_lock, sub_queue):
        ''' Handles updates through a queue into sending out.
        '''
        # This will grab the notification guid from the queue.
        update = yield from sub_queue.get()
        
        # Immediately acquire the exchange lock when update yields.
        # print('Getting exchange lock for update.')
        yield from exchange_lock
        try:
            # print('Preparing to send sub update.')
            msg_preheader = b'!!'
            msg_body = bytes(update)
            msg = msg_preheader + msg_body
            yield from websocket.send(msg)
            # Note: should this expect a response?
            # print('Sub update sent.')
            
        finally:
            exchange_lock.release()
        # print('Released exchange lock for update.')
    
    
class PipeIPC(_IPCBase):
    pass