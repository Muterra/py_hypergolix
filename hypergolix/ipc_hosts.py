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


class _EndpointBase(metaclass=abc.ABCMeta):
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
    def __init__(self, dispatch, apis, *args, **kwargs):
        ''' Creates an endpoint for the specified agent that handles the
        associated apis. Apis is an iterable of either AppDef objects, 
        in which case the existing tokens are used, or api_ids, in which 
        case, new tokens are generated. The iterable may mix those as 
        well.
        '''
        super().__init__(*args, **kwargs)
        
        self._dispatch = weakref.proxy(dispatch)
        self._appdefs = {}
        
        for api in apis:
            if isinstance(api, AppDef):
                api_id = api.api_id
                app_token = api.app_token
            else:
                api_id = api
                app_token = None
                
            appdef = self._dispatch.register_api(
                api_id = api_id, 
                endpoint = self,
                app_token = app_token
            )
            self._appdefs[api_id] = appdef
            
    def wrap_appobj(self, obj):
        ''' Wraps data from an appobj state update (or initial creation)
        ...
        shit, I'm confused.
        '''
        api_id = obj.api_id
        body = obj.state
        wrapped = {
            'api_id': api_id,
            'body': body
        }
        # Note: the object must define its api_id.
        
    
    @abc.abstractmethod
    def handle_incoming(self, obj):
        ''' Handles an object.
        '''
        pass
        
    @abc.abstractmethod
    def handle_outgoing_failure(self, obj):
        ''' Handles an object that failed to be accepted by the intended
        recipient.
        '''
        pass
        
    @abc.abstractmethod
    def handle_outgoing_success(self, obj):
        ''' Handles an object that failed to be accepted by the intended
        recipient.
        '''
        pass
        
    @property
    def dispatch(self):
        ''' Access the agent.
        '''
        return self._dispatch
        
        
class _TestEndpoint(_EndpointBase):
    pass


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
    
    
class LocalhostIPC(_IPCBase):
    pass
    
    
class PipeIPC(_IPCBase):
    pass
    
    
class FileIPC(_IPCBase):
    pass