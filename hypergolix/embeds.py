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
# Embeds contains all of the application-side IPC integrations. These will only
# be used by applications written in python.

# Control * imports.
__all__ = [
    # 'TestIntegration', 
    # 'LocalhostIntegration'
]

# External dependencies
import abc
import msgpack
import weakref
import collections
import asyncio

from golix import Guid

# Inter-package dependencies
from .utils import RawObj
from .utils import AppObj

from .comms import WSReqResClient

from .exceptions import IPCError
        

class _EmbedBase(metaclass=abc.ABCMeta):
    ''' Embeds are what you put in the actual application to communicate
    with the Hypergolix service.
    
    Note that each embed will have exactly one endpoint. However, for 
    some types of IPC systems (ex: filesystems), it may make sense for
    a single application to use multiple endpoints. Therefore, an 
    application may want to use multiple embeds.
    
    Note that this API will be merged with AgentCore when DynamicObject
    and StaticObject are reconciled with AppObj.
    '''
    def __init__(self, app_token=None, *args, **kwargs):
        ''' Initializes self.
        '''
        self.app_token = app_token
        super().__init__(*args, **kwargs)
    
    @property
    def app_token(self):
        ''' Get your app token, or if you have none, register a new one.
        '''
        return self._token
        
    @app_token.setter
    def app_token(self, value):
        ''' Set your app token.
        '''
        self._token = value
    
    @abc.abstractmethod
    def register_api(self, api_id):
        ''' Registers the embed with the service as supporting the
        passed api_id.
        
        May be called multiple times to denote that a single application 
        endpoint is capable of supporting multiple api_ids.
        
        Returns True.
        '''
        pass
        
    @property
    @abc.abstractmethod
    def whoami(self):
        ''' Return the address of the currently active agent.
        '''
        pass
        
    @abc.abstractmethod
    def get_object(self, guid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        pass
        
    @abc.abstractmethod
    def new_object(self, state, api_id, app_token, private, dynamic=True, callbacks=None, _legroom=None):
        ''' Alternative constructor for AppObj that does not require 
        passing the embed explicitly.
        '''
        pass
        
    @abc.abstractmethod
    def update_object(self, obj, state):
        ''' Wrapper for obj.update.
        '''
        pass
        
    @abc.abstractmethod
    def sync_object(self, obj):
        ''' Checks for an update to a dynamic object, and if one is 
        available, performs an update.
        '''
        pass
        
    @abc.abstractmethod
    def share_object(self, obj, recipient):
        ''' Shares an object with someone else.
        '''
        pass
        
    @abc.abstractmethod
    def freeze_object(self, obj):
        ''' Converts a dynamic object to a static object.
        '''
        pass
        
    @abc.abstractmethod
    def hold_object(self, obj):
        ''' Binds an object, preventing its deletion.
        '''
        pass
        
    @abc.abstractmethod
    def delete_object(self, obj):
        ''' Attempts to delete an object. May not succeed, if another 
        Agent has bound to it.
        '''
        pass
        
    def deliver_object_wrapper(self, connection, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        '''
        return b''

    def update_object_wrapper(self, connection, request_body):
        ''' Deserializes an incoming object update, updates the AppObj
        instance(s) accordingly, and serializes a response to the IPC 
        host.
        '''
        return b''

    def notify_share_failure_wrapper(self, connection, request_body):
        ''' Deserializes an incoming async share failure notification, 
        dispatches that to the app, and serializes a response to the IPC 
        host.
        '''
        return b''

    def notify_share_success_wrapper(self, connection, request_body):
        ''' Deserializes an incoming async share failure notification, 
        dispatches that to the app, and serializes a response to the IPC 
        host.
        '''
        return b''
        
        
class _TestEmbed(_EmbedBase):
    def register_api(self, *args, **kwargs):
        ''' Just here to silence errors from ABC.
        '''
        pass
    
    def get_object(self, guid):
        ''' Wraps RawObj.__init__  and get_guid for preexisting objects.
        '''
        author, is_dynamic, state = self.get_guid(guid)
            
        return RawObj(
            # Todo: make the dispatch more intelligent
            dispatch = self,
            state = state,
            dynamic = is_dynamic,
            _preexisting = (guid, author)
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
        # frame_guid = self._historian[obj.address][0]
        # target = self._dynamic_targets[obj.address]
        target = obj.address
        self.hand_guid(target, recipient)
        
    def share_object(self, obj, recipient):
        ''' Currently, this is just calling hand_object. In the future,
        this will have a devoted key exchange subprotocol.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Only RawObj may be shared.'
            )
        return self.hand_guid(obj.address, recipient)
        
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
        
        
class WebsocketsEmbed(_EmbedBase, WSReqResClient):
    REQUEST_CODES = {
        # # Get new app token
        # b'+T': None,
        # # Register existing app token
        # b'@T': None,
        # Register an API
        'register_api': b'@A',
        # Whoami?
        'whoami': b'?I',
        # Get object
        'get_object': b'>O',
        # New object
        'new_object': b'<O',
        # Sync object
        'sync_object': b'~O',
        # Update object
        'update_object': b'!O',
        # Share object
        'share_object': b'^O',
        # Freeze object
        'freeze_object': b'*O',
        # Hold object
        'hold_object': b'#O',
        # Delete object
        'delete_object': b'XO',
    }
    
    def __init__(self, *args, **kwargs):
        # Note that these are only for unsolicited contact from the server.
        req_handlers = {
            # Receive/dispatch a new object.
            b'vO': self.deliver_object_wrapper,
            # Receive an update for an existing object.
            b'!O': self.update_object_wrapper,
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
        
    @asyncio.coroutine
    def init_connection(self, websocket, path):
        ''' Initializes the connection with the client, creating an 
        endpoint/connection object, and registering it with dispatch.
        '''
        connection = yield from super().init_connection(
            websocket = websocket, 
            path = path
        )
        
        # First command on the wire MUST be us registering the application.
        if self.app_token is None:
            app_token = b''
            request_code = b'+T'
        else:
            app_token = self.app_token
            request_code = b'@T'
        
        msg = self._pack_request(
            version = self._version, 
            token = 0, 
            req_code = request_code, 
            body = app_token
        )
        
        yield from websocket.send(msg)
        reply = yield from websocket.recv()
        
        version, resp_token, resp_code, resp_body = self._unpack_request(reply)
        
        if resp_code == self._success_code:
            # Note: somewhere, app_token consistency should be checked.
            my_token, app_token = self.unpack_success(resp_body)
            self.app_token = app_token
            
        elif resp_code == self._failure_code:
            my_token, exc = self.unpack_failure(resp_body)
            raise IPCError('IPC host denied app registration.') from exc
            
        else:
            raise IPCError(
                'IPC host did not respond appropriately during initial app '
                'handshake.'
            )
            
        print('Connection established with IPC server.')
        return connection
        
    def register_api(self, api_id):
        ''' Registers an API ID with the hypergolix service for this 
        application.
        '''
        if len(api_id) != 65:
            raise ValueError('Invalid API ID.')
        
        response = self.send_threadsafe(
            connection = self.connection,
            msg = api_id,
            request_code = self.REQUEST_CODES['register_api']
        )
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while registering API.')
    
    def get_object(self, guid):
        ''' Wraps RawObj.__init__  and get_guid for preexisting objects.
        '''
        author, is_dynamic, state = self.get_guid(guid)
            
        return RawObj(
            # Todo: make the dispatch more intelligent
            dispatch = self,
            state = state,
            dynamic = is_dynamic,
            _preexisting = (guid, author)
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
        # frame_guid = self._historian[obj.address][0]
        # target = self._dynamic_targets[obj.address]
        target = obj.address
        self.hand_guid(target, recipient)
        
    def share_object(self, obj, recipient):
        ''' Currently, this is just calling hand_object. In the future,
        this will have a devoted key exchange subprotocol.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Only RawObj may be shared.'
            )
        return self.hand_guid(obj.address, recipient)
        
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
    def whoami(self):
        ''' Inherited from Agent.
        '''
        raw_guid = self.send_threadsafe(
            connection = self.connection,
            msg = b'',
            request_code = self.REQUEST_CODES['whoami']
        )
        return Guid.from_bytes(raw_guid)
        
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