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
    'TestIntegration', 
    'LocalhostIntegration'
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


class _IntegrationBase(metaclass=abc.ABCMeta):
    ''' Base class for a integration. Note that an integration cannot 
    exist without also being an agent. They are separated to allow 
    mixing-and-matching agent/persister/integration configurations.
    
    Could subclass _EndpointBase to ensure that we can use self as an 
    endpoint for incoming messages. To prevent spoofing risks, anything
    we'd accept this way MUST be append-only with a very limited scope.
    Or, we could just handle all of our operations directly with the 
    agent bootstrap object. Yeah, let's do that instead.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Lookup for app_tokens -> endpoints
        self._app_tokens = {
            b'\x00\x00\x00\x00': False,
        }
        
        # Lookup for api_ids -> AppDef.
        self._api_ids = {}
        
        # Lookup for handshake guid -> handshake object
        self._outstanding_handshakes = {}
        # Lookup for handshake guid -> api_id
        self._outstanding_owners = {}
        
        # Lookup for guid -> object
        self._assigned_objects = {}
        # Lookup for guid -> api_id
        self._assigned_owners = {}
        
        self._orphan_handshakes_incoming = []
        self._orphan_handshakes_outgoing = []
        
    def initiate_handshake(self, recipient, msg):
        ''' Creates a handshake for the API_id with recipient.
        
        msg isinstance dict(like) and must contain a valid api_id
        recipient isinstance Guid
        '''
        # Check to make sure that we have a valid api_id in the msg
        try:
            api_id = msg['api_id']
            appdef = self._api_ids[api_id]
            
        except KeyError as e:
            raise ValueError(
                'Handshake msg must contain a valid api_id that the current '
                'agent integration is capable of understanding.'
            ) from e
            
        try:
            packed_msg = msgpack.packb(msg)
            
        except (
            msgpack.exceptions.BufferFull,
            msgpack.exceptions.ExtraData,
            msgpack.exceptions.OutOfData,
            msgpack.exceptions.PackException,
            msgpack.exceptions.PackValueError
        ) as e:
            raise ValueError(
                'Couldn\'t pack handshake. Handshake msg must be dict-like.'
            ) from e
        
        handshake = self.new_dynamic(msg)
        self.hand_object(
            obj = handshake, 
            recipient_guid = recipient
        )
        
        # This bit is still pretty tentative
        self._outstanding_handshakes[handshake.address] = handshake
        self._outstanding_owners[handshake.address] = api_id
        
    def dispatch_handshake(self, handshake):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        # First try unpacking the message.
        try:
            unpacked_msg = msgpack.unpackb(handshake.state)
            api_id = unpacked_msg['api_id']
            appdef = self._api_ids[api_id]
            
        # MsgPack errors mean that we don't have a properly formatted handshake
        except (
            msgpack.exceptions.BufferFull,
            msgpack.exceptions.ExtraData,
            msgpack.exceptions.OutOfData,
            msgpack.exceptions.UnpackException,
            msgpack.exceptions.UnpackValueError
        ) as e:
            raise HandshakeError(
                'Handshake does not appear to conform to the hypergolix '
                'integration handshake procedure.'
            ) from e
            
        # KeyError means we don't have an app that speaks that API
        except KeyError:
            warnings.warn(HandshakeWarning(
                'Agent lacks application to handle app id.'
            ))
            self._orphan_handshakes_incoming.append(handshake)
            
        # Okay, we've successfully acquired an appdef. Pass the message along
        # to the application.
        else:
            try:
                appdef.endpoint.handle_incoming(raw_msg)
                
            except Exception as e:
                raise HandshakeError(
                    'Agent application assigned to app id was unable to '
                    'process the handshake.'
                )
        
            # Lookup for guid -> object
            self._assigned_objects[handshake.address] = handshake
            self._assigned_owners[handshake.address] = api_id
        
    def dispatch_handshake_ack(self, ack, target):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        handshake = self._outstanding_handshakes[target]
        owner = self._outstanding_owners[target]
        
        del self._outstanding_handshakes[target]
        del self._outstanding_owners[target]
        
        self._assigned_objects[target] = handshake
        self._assigned_owners[target] = owner
        
        endpoint = self._api_ids[owner].endpoint
        endpoint.handle_outgoing_success(handshake)
    
    def dispatch_handshake_nak(self, nak, target):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        handshake = self._outstanding_handshakes[target]
        owner = self._outstanding_owners[target]
        
        del self._outstanding_handshakes[target]
        del self._outstanding_owners[target]
        
        endpoint = self._api_ids[owner].endpoint
        endpoint.handle_outgoing_failure(handshake)
    
    def register_application(self, api_id=None, appdef=None):
        ''' Registers an application with the integration. If appdef is
        None, will create an AppDef for the app. Must define api_id XOR
        appdef.
        
        Returns an AppDef object.
        '''
        if appdef is None and api_id is not None:
            app_token = self.new_token()
            endpoint = self.new_endpoint()
            appdef = AppDef(api_id, app_token, endpoint)
            
        elif appdef is not None and api_id is None:
            # Do nothing. We already have an appdef.
            pass
            
        else:
            raise ValueError('Must specify appdef XOR api_id.')
            
        self._app_tokens[appdef.app_token] = appdef.endpoint
        self._api_ids[appdef.api_id] = appdef
        
        return appdef
        
    def new_token(self):
        # Use a dummy api_id to force the while condition to be true initially
        token = b'\x00\x00\x00\x00'
        # Birthday paradox be damned; we can actually *enforce* uniqueness
        while token in self._app_tokens:
            token = os.urandom(4)
        return token
        
    @abc.abstractmethod
    def new_endpoint(self):
        ''' Creates a new endpoint for the integration. Endpoints must
        be unique. Uniqueness must be enforced by subclasses of the
        _IntegrationBase class.
        
        Returns an Endpoint object.
        '''
        pass
    
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
    def get_object(self, guid):
        ''' Inherited from Agent.
        '''
        pass
        
        
class _EmbeddedIntegration(_IntegrationBase, _EmbedBase):
    ''' EmbeddedIntegration wraps _EmbedBase from embeds. It also 
    is its own endpoint (or has its own endpoint).
    '''
    def new_endpoint(self):
        ''' Creates a new endpoint for the integration. Endpoints must
        be unique. Uniqueness must be enforced by subclasses of the
        _IntegrationBase class.
        
        Returns an Endpoint object.
        '''
        pass
        
        
class TestEndpoint(_EndpointBase):
    ''' An embedded application endpoint.
    '''
    def handle_incoming(self, obj):
        ''' Handles an object.
        '''
        pass
        
    def handle_outgoing_failure(self, obj):
        ''' Handles an object that failed to be accepted by the intended
        recipient.
        '''
        pass
        
    def handle_outgoing_success(self, obj):
        ''' Handles an object that failed to be accepted by the intended
        recipient.
        '''
        pass
    
    
class TestIntegration(_IntegrationBase):
    ''' An integration that ignores all dispatching for test purposes.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._orphan_handshakes_incoming = []
        self._orphan_handshakes_outgoing = []
        self._orphan_handshake_failures = []
        
    def dispatch_handshake(self, handshake):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        self._orphan_handshakes_incoming.append(handshake)
        
    def dispatch_handshake_ack(self, ack, target):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        self._orphan_handshakes_outgoing.append(ack)
    
    def dispatch_handshake_nak(self, nak, target):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        self._orphan_handshake_failures.append(nak)
        
    def new_endpoint(self):
        ''' Creates a new endpoint for the integration. Endpoints must
        be unique. Uniqueness must be enforced by subclasses of the
        _IntegrationBase class.
        
        Returns an Endpoint object.
        '''
        return TestEndpoint()
        
    def retrieve_recent_handshake(self):
        return self._orphan_handshakes_incoming.pop()
        
    def retrieve_recent_ack(self):
        return self._orphan_handshakes_outgoing.pop()
        
    def retrieve_recent_nak(self):
        return self._orphan_handshake_failures.pop()
    
    
class LocalhostIntegration(_IntegrationBase):
    pass
    
    
class PipeIntegration(_IntegrationBase):
    pass
    
    
class FileIntegration(_IntegrationBase):
    pass