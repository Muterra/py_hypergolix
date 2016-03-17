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
        try:
            __ = getattr(self, name)
        except AttributeError:
            # Note that this will also raise an AttributeError!
            super().__delattr__(name, value)
        else:
            raise AttributeError(
                'StaticObjects do not support deletion of attributes once '
                'they have been declared.'
            )
            
    def __repr__(self):
        c = type(self).__name__
        s = ('('
                'author=' + repr(self.author) + ', '
                'address=' + repr(self.address) + ', '
                'state=' + repr(self.state) +
            ')'
        )
        return c + s
    
    
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
        c = type(self).__name__
        s = ('('
                'author=' + repr(self.author) + ', '
                'address=' + repr(self.address) + ', '
                'buffer=' + repr(self.buffer) +
            ')'
        )
        return c + s
        
        
class _DynamicHistorian:
    ''' Helper class to track the historical state of a dynamic binding.
    '''
    pass


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
        
    def make_static(self, data):
        ''' Makes a new static object, handling binding, persistence, 
        and so on. Returns a StaticObject.
        '''
        secret = self._identity.new_secret()
        container = self._identity.make_container(
            secret = secret,
            plaintext = data
        )
        binding = self._identity.make_bind_static(
            target = container.guid
        )
        # This would be a good spot to figure out a way to make use of
        # publish_unsafe.
        self.persister.publish(binding.packed)
        self.persister.publish(container.packed)
        self._bindings[container.guid] = binding.guid
        return StaticObject(
            author = self._identity.guid,
            address = container.guid,
            state = data
        )
        
    def make_dynamic(self, data):
        '''
        '''
        pass
        
    def update_dynamic(self, obj, data):
        ''' 
        '''
        pass
        
    def freeze_dynamic(self, obj):
        '''
        '''
        pass
        
    def delete_object(self, obj):
        ''' 
        '''
        pass
        
    def share_object(self, obj, recipient):
        '''
        '''
        pass