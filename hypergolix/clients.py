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
    'EmbeddedClient', 
    'LocalhostClient'
]

# External dependencies
import abc

# Intrapackage dependencies
from .utils import HandshakeError

class _ClientBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def dispatch_handshake(self, handshake):
        ''' Receives the target for a handshake (NOT the handshake
        object itself) and dispatches it to the appropriate application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        pass
        
    @abc.abstractmethod
    def dispatch_handshake_ack(self, ack):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        pass
        
    @abc.abstractmethod
    def dispatch_handshake_nak(self, nak):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        pass
    
    
class EmbeddedClient(_ClientBase):
    def __init__(self):
        self._handshakes = {}
        
    def dispatch_handshake(self, handshake):
        ''' Receives the target for a handshake (NOT the handshake
        object itself) and dispatches it to the appropriate application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        self._handshakes[handshake.address] = handshake
        
    def dispatch_handshake_ack(self, ack):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        pass
    
    def dispatch_handshake_nak(self, nak):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        pass
    
    
class LocalhostClient(_ClientBase):
    pass
    
    
class PipeClient(_ClientBase):
    pass
    
    
class FileClient(_ClientBase):
    pass