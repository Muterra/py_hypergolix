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
    def __init__(self, dispatch, apis=None, *args, **kwargs):
        ''' Creates an endpoint for the specified agent that handles the
        associated apis. Apis is an iterable of either AppDef objects, 
        in which case the existing tokens are used, or api_ids, in which 
        case, new tokens are generated. The iterable may mix those as 
        well.
        '''
        super().__init__(*args, **kwargs)
        
        self._dispatch = weakref.proxy(dispatch)
        # Lookup api_id -> { appdefs }
        self._appdefs = {}
        # Lookup token -> appdef
        self._tokens = {}
        
        if apis is None:
            apis = tuple()
        
        for api in apis:
            self.add_api(api)
            
    def add_api(self, appdef):
        ''' This adds an appdef to the endpoint. Probably not strictly
        necessary, but helps keep track of things.
        '''
        if not isinstance(appdef, AppDef):
            raise TypeError('appdef must be AppDef.')
            
        if appdef.api_id not in self._appdefs:
            self._appdefs[appdef.api_id] = { appdef }
        else:
            # It's a set, so we don't need to worry about adding more copies
            self._appdefs[appdef.api_id].add(appdef)
            
        # This should probably be a little more intelligent in the future
        if appdef.app_token not in self._tokens:
            self._tokens[appdef.app_token] = appdef
        else:
            raise RuntimeError('Cannot reuse tokens for new applications.')
    
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._assigned_objs = []
        self._failed_objs = []
        
    def handle_incoming(self, obj):
        self._assigned_objs.append(obj)
        print('Incoming: ', obj)
        
    def handle_outgoing_failure(self, obj, recipient):
        self._failed_objs.append(obj)
        print('Failed: ', obj)
        
    def handle_outgoing_success(self, obj, recipient):
        self._assigned_objs.append(obj)
        print('Success: ', obj)


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