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
    'EmbeddedIntegration', 
    'LocalhostIntegration'
]

# External dependencies
import abc

# Intrapackage dependencies
from .utils import HandshakeError

class _IntegrationBase(metaclass=abc.ABCMeta):
    ''' Base class for a integration. Note that an integration cannot 
    exist without also being an agent. They are separated to allow 
    mixing-and-matching agent/persister/integration configurations.
    '''
    
    @property
    @abc.abstractmethod
    def address(self):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def new_static(self, data=None, link=None):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def new_dynamic(self, data):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def update_dynamic(self, obj, data=None, link=None):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def refresh_dynamic(self, obj):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def freeze_dynamic(self, obj):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def hold_object(self, obj):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def delete_object(self, obj):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def hand_object(self, obj, recipient_guid):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def get_object(self, secret, guid):
        ''' Inherited from Agent.
        '''
        pass
        
    @abc.abstractmethod
    def dispatch_handshake(self, handshake):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
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
    
    
class EmbeddedIntegration(_IntegrationBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._orphan_handshakes_incoming = []
        self._orphan_handshakes_outgoing = []
        
    def dispatch_handshake(self, handshake):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        self._orphan_handshakes_incoming.append(handshake)
        
    def dispatch_handshake_ack(self, ack):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        self._orphan_handshakes_outgoing.append(ack)
    
    def dispatch_handshake_nak(self, nak):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        pass
    
    
class LocalhostIntegration(_IntegrationBase):
    pass
    
    
class PipeIntegration(_IntegrationBase):
    pass
    
    
class FileIntegration(_IntegrationBase):
    pass