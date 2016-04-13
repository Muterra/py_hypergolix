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

# Intrapackage dependencies
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning

from .utils import AppDef
from .utils import AppObj


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
    def __init__(self, dispatch, token=None, apis=None, *args, **kwargs):
        ''' Creates an endpoint for the specified agent that handles the
        associated apis. Apis is an iterable of either AppDef objects, 
        in which case the existing tokens are used, or api_ids, in which 
        case, new tokens are generated. The iterable may mix those as 
        well.
        '''
        super().__init__(*args, **kwargs)
        
        self._dispatch = weakref.proxy(dispatch)
        
        if token is None:
            token = self.dispatch.new_token()
        if apis is None:
            apis = tuple()
            
        self._token = token
        # Set of available apis -> appdefs.
        self._apis = {}
        
        for api in apis:
            self.add_api(api)
            
    def add_api(self, api_id):
        ''' This adds an appdef to the endpoint. Probably not strictly
        necessary, but helps keep track of things.
        '''
        # Type check by creating an appdef.
        appdef = AppDef(api_id, self._token)
        self._apis[api_id] = appdef
        
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
        
    @property
    def appdefs(self):
        ''' Return tuple of all current appdefs.
        '''
        return tuple(self._apis.values())
    
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
            'send_new', 
            'send_failure', 
            'send_success', 
            'send_update'
        }:
            raise ValueError('Invalid command.')
        
        # Should token not be used? Is leaking the app_token a security risk?
        # I mean, if it's going to someone who doesn't already have it, we 
        # majorly fucked up, but what about damage mitigation? On the other
        # hand, what if we have (in the future) a secure way to use a single
        # endpoint for multiple applications? (admittedly that seems unlikely).
        obj_pack = {
            'app_token': obj.app_token,
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    @abc.abstractmethod
    def new_endpoint(self):
        ''' Creates a new endpoint for the IPC system. Endpoints must
        be unique. Uniqueness must be enforced by subclasses of the
        _IPCBase class.
        
        Returns an Endpoint object.
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
    
    
class WebsocketsIPC(_IPCBase):
    ''' Websockets IPC via localhost.
    '''
    
    pass
    
    
class PipeIPC(_IPCBase):
    pass
    
    
class FileIPC(_IPCBase):
    pass