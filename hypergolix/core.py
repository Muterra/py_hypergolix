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

Some notes:

There's an awkward balance between streams and dynamic bindings. Streams
are mutable plaintext objects and totally private, but only ever locally
updated by Agents (whether receiving or creating). However, dynamic 
bindings can be subscribed to at persistence providers, and are public 
objects there. So they need two objects; one for the dynamic binding, 
which is then resolved into GEOC objects, and one for its plaintext.


DO PERSISTENCE PROVIDERS FIRST.

'''

# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'Agent', 
    'StaticObject',
    'DynamicObject'
]

# Global dependencies
import collections

from golix import FirstParty
from golix import SecondParty
from golix import Guid
from golix import Secret

from golix._getlow import GIDC
from golix._getlow import GEOC
from golix._getlow import GOBS
from golix._getlow import GOBD
from golix._getlow import GDXX
from golix._getlow import GARQ

from Crypto.Protocol.KDF import scrypt

# # Inter-package dependencies that pass straight through to __all__
# from .utils import Guid
# from .utils import SecurityError
# from .utils import Secret

# Inter-package dependencies that are only used locally
from .utils import NakError
from .persisters import _PersisterBase
        
# ###############################################
# Utilities, etc
# ###############################################
        

class _ObjectBase:
    ''' Hypergolix objects cannot be directly updated. They must be 
    passed to Agents for modification (if applicable). They do not (and, 
    if you subclass, for security reasons they should not) reference a
    parent Agent.
    
    Objects provide a simple interface to the arbitrary binary data 
    contained within Golix containers. They track both the plaintext, 
    and the associated GUID. They do NOT expose the secret key material
    of the container.
    
    From the perspective of an external method, *all* Objects should be 
    treated as read-only. They should only ever be modified by Agents.
    '''
    __slots__ = [
        '_author',
        '_state',
        '_address'
    ]
    
    def __init__(self, author, address, state):
        ''' Creates a new object. Address is the dynamic guid. State is
        the initial state.
        '''
        self._author = author
        self._state = state
        self._address = address
        
    @property
    def author(self):
        return self._author
        
    @property
    def address(self):
        return self._address
        
    @property
    def state(self):
        return self._state

        
class StaticObject(_ObjectBase):
    ''' An immutable object. Can be produced directly, or by freezing a
    dynamic object.
    '''
    # This might be a little excessive, but I guess it's nice to have a
    # little extra protection against updates?
    def __setattr__(self, name, value):
        ''' Prevent rewriting declared attributes.
        '''
        try:
            # Check if the attribute exists. If we can get it, it exists.
            __ = getattr(self, name)
        except AttributeError:
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                'StaticObjects do not support mutation of attributes once '
                'they have been declared.'
            )
            
    def __delattr__(self, name):
        ''' Prevent deleting declared attributes.
        '''
        raise AttributeError(
            'StaticObjects do not support deletion of attributes.'
        )
            
    def __repr__(self):
        return type(self).__name__ + ('('
                'author=' + repr(self.author) + ', '
                'address=' + repr(self.address) + ', '
                'state=' + repr(self.state) +
            ')'
        )
    
    
class DynamicObject(_ObjectBase):
    ''' A mutable object. Updatable by Agents.
    Interestingly, this could also do the whole __setattr__/__delattr__
    thing from above, since we're overriding state, and buffer updating
    is handled by the internal deque.
    '''
    __slots__ = [
        '_author',
        '_state',
        '_address',
        '_buffer'
    ]
    
    def __init__(self, author, address, buffer=None, state=None):
        # Catch declaring both -- super's _state is unused here.
        if buffer is not None and state is not None:
            raise ValueError(
                'Declare either buffer or state, but not both.'
            )
            
        elif state is not None:
            self._buffer = collections.deque((state,))
        
        elif buffer is not None:
            self._buffer = collections.deque(buffer)
            
        else:
            self._buffer = collections.deque()
            
        # super's _state is unused due to state@property override.
        super().__init__(author=author, address=address, state=None)
        
    @property
    def state(self):
        return self._buffer[0]
        
    @property
    def buffer(self):
        ''' Returns a tuple of the current buffer.
        '''
        # Note that this has the added benefit of preventing assignment
        # to the internal buffer!
        return tuple(self._buffer)
            
    def __repr__(self):
        return type(self).__name__ + ('('
                'author=' + repr(self.author) + ', '
                'address=' + repr(self.address) + ', '
                'buffer=' + repr(self.buffer) +
            ')'
        )
        
        
class _DynamicHistorian:
    ''' Helper class to track the historical state of a dynamic binding.
    '''
    pass
    
    
def _check_if_obj(obj):
    if not isinstance(obj, _ObjectBase):
        raise TypeError(
            'Obj must be StaticObject, DynamicObject, or similar.'
        )


class Agent():
    def __init__(self, persister, _golix_firstparty=None):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        '''
        if not isinstance(persister, _PersisterBase):
            raise TypeError('Persister must subclass _PersisterBase.')
        self._persister = persister
        
        if _golix_firstparty is None:
            self._identity = FirstParty()
            self._persister.publish(self._identity.second_party.packed)
        else:
            # Could do type checking here but currently no big deal?
            # This would also be a good spot to make sure our identity already
            # exists at the persister.
            self._identity = _golix_firstparty
        
        self._secrets = {}
        self._contacts = {}
        # Bindings lookup: {<target guid>: <binding guid>}
        self._bindings = {}
        # History lookup for dynamic bindings. {<dynamic guid>: <frame deque>}
        # Note that the deque must use a maxlen or it will grow indefinitely.
        self._historian = {}
        
    @property
    def persister(self):
        return self._persister
        
    def save(self, password):
        ''' Save the agent's identity to a GEOC object.
        '''
        # Condense everything we need to rebuild self._golix_provider
        keys = self._golix_provider._serialize()
        # Store the guid for the dynamic bootstrap object
        bootstrap = self._bootstrap_binding
        # Create some random-length, random padding to make it harder to
        # guess that our end-product GEOC is a saved Agent
        padding = None
        # Put it all into a GEOC.
        # Scrypt the password. Salt against the author GUID, which we know
        # (when reloading) from the author of the file!
        # Use 2**14 for t<=100ms, 2**20 for t<=5s
        combined = scrypt(
            password = password, 
            salt = bytes(self._golix_provider.guid),
            key_len = 48,
            N = 2**15,
            r = 8,
            p = 1
        )
        secret = Secret(
            cipher = 1,
            key = combined[:32],
            seed = combined[32:48]
        )
        
    @classmethod
    def load(cls, password, data):
        ''' Load an Agent from an identity contained within a GEOC.
        '''
        pass
        
    def _prep_geoc(self, data):
        secret = self._identity.new_secret()
        container = self._identity.make_container(
            secret = secret,
            plaintext = data
        )
        self._secrets[container.guid] = secret
        return container
        
    def _prep_bind(self, container):
        binding = self._identity.make_bind_static(
            target = container.guid
        )
        self._bindings[container.guid] = binding.guid
        return binding
        
    def make_static(self, data):
        ''' Makes a new static object, handling binding, persistence, 
        and so on. Returns a StaticObject.
        '''
        container = self._prep_geoc(data)
        binding = self._prep_bind(container)
        # This would be a good spot to figure out a way to make use of
        # publish_unsafe.
        # Note that if these raise exceptions and we catch them, we'll
        # have an incorrect state in self._bindings
        self.persister.publish(binding.packed)
        self.persister.publish(container.packed)
        return StaticObject(
            author = self._identity.guid,
            address = container.guid,
            state = data
        )
        
    def make_dynamic(self, data=None, link=None, _legroom=3):
        ''' Makes a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        The _legroom argument determines how many frames should be used 
        as history in the dynamic binding.
        '''
        self._check_dynamic_args(data, link)
            
        if data is not None:
            container = self._make_geoc(data)
            link = container.guid
            
        dynamic = self._identity.make_bind_dynamic(
            target = link
        )
        
        self.persister.publish(dynamic.packed)    
        if data is not None:
            self.persister.publish(container.packed)
            
        # Historian manages the history definition for the object.
        self._historian[dynamic.guid_dynamic] = collections.deque(
            iterable = (dynamic.guid,),
            maxlen = _legroom
        )
            
        # Add a note to _bindings that "I am my own keeper"
        self._bindings[dynamic.guid_dynamic] = dynamic.guid_dynamic
        
    def update_dynamic(self, obj, data=None, link=None):
        ''' Updates a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        Could add a way to update the legroom parameter while we're at
        it.
        '''
        if not isinstance(obj, DynamicObject):
            raise TypeError(
                'Obj must be a DynamicObject or similar.'
            )
        if obj.address not in self._historian:
            raise ValueError(
                'The Agent could not find a record of the object\'s history. '
                'Agents cannot update objects they did not create.'
            )
        self._check_dynamic_args(data, link)
            
        if data is not None:
            container = self._make_geoc(data)
            link = container.guid
            
        dynamic = self._identity.make_bind_dynamic(
            target = link,
            guid_dynamic = obj.address,
            history = self._historian[obj.address]
        )
            
        self.persister.publish(dynamic.packed)
        if data is not None:
            self.persister.publish(container.packed)
            
        self._historian[obj.address].appendleft(dynamic.guid)
            
    @staticmethod
    def _check_dynamic_args(data, link):
        ''' Validate data, link arguments passed to make_ and 
        update_dynamic methods.
        '''
        if (data is None and link is None) or \
        (data is not None and link is not None):
            raise TypeError('Must pass either data XOR link to make_dynamic.')
        
    def freeze_dynamic(self, obj):
        '''
        '''
        pass
        
    def delete_object(self, obj):
        ''' Removes an object (if possible). May produce a warning if
        the persistence provider cannot remove the object due to another 
        conflicting binding.
        '''
        _check_if_obj(obj)
            
        if obj.address not in self._bindings:
            raise ValueError(
                'Agents cannot attempt to delete objects they did not create. '
                'This may also indicate that the object has already been '
                'deleted.'
            )
            
        binding_guid = self._bindings[obj.address]
        debind = self._identity.make_debind(
            target = binding_guid
        )
        self.persister.publish(debind.packed)
        del self._bindings[obj.address]
        
    def share_object(self, obj, recipient):
        '''
        '''
        pass
        

class _ClientBase:
    pass
    
    
class EmbeddedClient:
    pass
    
    
class LocalhostClient:
    pass
    
    
class PipeClient:
    pass
    
    
class FileClient:
    pass