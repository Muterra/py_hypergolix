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
]

# Global dependencies
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

        
# ###############################################
# Utilities, etc
# ###############################################


class Agent():
    def __init__(self, _golix_provider=None):
        if _golix_provider == None:
            self._golix_provider = FirstParty()
        else:
            self._golix_provider = _golix_provider
            
        # This needs to be a MUID for a dynamic binding
        self._bootstrap_binding = None
        
    def save(self, password):
        # Condense everything we need to rebuild self._golix_provider
        keys = self._golix_provider._serialize()
        # Store the muid for the dynamic bootstrap object
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
        pass
        
    def new_stream(self):
        ''' Create a new dynamic binding.
        '''
        pass
        
    def update_stream(self, stream, data):
        ''' Updates the stream.
        '''
        pass
        
    def refresh_stream(self, stream, persister):
        ''' The callback registered to the persistence provider to 
        create the plaintext object from an updated binding.
        '''
        pass
        
    def close_stream(self, stream):
        ''' Closes the stream.
        '''
        pass
    
        
class Stream():
    ''' Streams cannot update themselves. Only Agents can modify them.
    If you subclass, you should be careful to avoid putting any refs to
    Agents within Streams, because you'll be increasing the attack (and 
    even just exposure) surface of your private key data.
    
    This is a local plaintext object. Persistence providers will never
    have anything to do with it.
    '''
    def __init__(self, address, state):
        ''' Creates a new stream. Address is the dynamic guid. State is
        the initial state.
        '''
        self._state = state
        self._address = address
        
    @property
    def address(self):
        return self._address
        
    @property
    def state(self):
        return self._state