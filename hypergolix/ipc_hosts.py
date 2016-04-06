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

# Intrapackage dependencies
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning

from .utils import AppDef
from .utils import _EndpointBase

from .embeds import _EmbedBase


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
        
        
class _EmbeddedIPC(_IPCBase, _EmbedBase):
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