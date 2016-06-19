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

# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'AgentAccount',
]

# Global dependencies
import collections
import weakref
import threading
import os
import msgpack
import abc
import traceback
import warnings

from golix import FirstParty
from golix import SecondParty
from golix import Ghid
from golix import Secret
from golix import SecurityError

from golix._getlow import GEOC
from golix._getlow import GOBD

from golix.utils import AsymHandshake
from golix.utils import AsymAck
from golix.utils import AsymNak

from Crypto.Protocol.KDF import scrypt
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import HKDF

# Intra-package dependencies
from .utils import _JitSetDict
from .utils import _JitDictDict
from .utils import RawObj
from .utils import DispatchObj

from .exceptions import NakError
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning
from .exceptions import InaccessibleError
from .exceptions import UnknownPartyError
from .exceptions import DispatchError
from .exceptions import DispatchWarning

from .persisters import _PersisterBase
from .persisters import MemoryPersister

from .ipc_hosts import _IPCBase
from .ipc_hosts import _EndpointBase


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Utilities, etc
# ###############################################


class AgentAccount:
    ''' Agent accounts include various utility functions for agent 
    persistence across multiple devices and/or logins.
    
    Must be paired (also classed with) AgentBase and DispatcherBase.
    Will not work without them.
    '''
    def __init__(self, _preexisting=None, *args, **kwargs):
        '''
        _preexisting isinstance tuple
            _preexisting[0] isinstance golix.Ghid
            _preexisting[1] isinstance golix.Secret
        '''
        super().__init__(_preexisting=_preexisting, *args, **kwargs)
        
        if _preexisting is None:
            self._bootstrap = AgentBootstrap(
                agent = self,
                obj = self._new_bootstrap_container()
            )
            # Default for legroom. Currently hard-coded and not overrideable.
            self._bootstrap['legroom'] = self.DEFAULT_LEGROOM
            
        else:
            # Get bootstrap
            bootstrap_ghid = _preexisting[0]
            bootstrap_secret = _preexisting[1]
            self._set_secret_pending(
                ghid = bootstrap_ghid,
                secret = bootstrap_secret
            )
            bootstrap_obj = self.get_object(bootstrap_ghid)
            bootstrap = AgentBootstrap.from_existing(
                obj = bootstrap_obj,
                agent = self
            )
            self._bootstrap = bootstrap
        
    def _new_bootstrap_container(self):
        ''' Creates a new container to use for the bootstrap object.
        '''
        padding_size = int.from_bytes(os.urandom(1), byteorder='big')
        padding = os.urandom(padding_size)
        return self.new_object(padding, dynamic=True)
        
    @property
    def _legroom(self):
        ''' Get the legroom from our bootstrap. If it hasn't been 
        created yet (aka within __init__), return the class default.
        '''
        try:
            return self._bootstrap['legroom']
        except (AttributeError, KeyError):
            # Default to the parent implementation
            return super()._legroom
        
    def register(self, password):
        ''' Save the agent's identity to a GEOC object.
        
        THIS NEEDS TO BE A DYNAMIC BINDING SO IT CAN UPDATE THE KEY TO
        THE BOOTSTRAP OBJECT. Plus, futureproofing. But, we also cannot,
        under any circumstances, reuse a Secret. So, instead of simply 
        using the scrypt output directly, we should put it through a
        secondary hkdf, using the previous frame ghid as salt, to ensure
        a new key, while maintaining updateability and accesibility.
        '''
        # Condense everything we need to rebuild self._golix_provider
        keys = self._golix_provider._serialize()
        # Store the ghid for the dynamic bootstrap object
        bootstrap = self._bootstrap_binding
        # Create some random-length, random padding to make it harder to
        # guess that our end-product GEOC is a saved Agent
        padding = None
        # Put it all into a GEOC.
        # Scrypt the password. Salt against the author GHID, which we know
        # (when reloading) from the author of the file!
        # Use 2**14 for t<=100ms, 2**20 for t<=5s
        combined = scrypt(
            password = password, 
            salt = bytes(self._golix_provider.ghid),
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
    def login(cls, password, data, persister, dispatcher):
        ''' Load an Agent from an identity contained within a GEOC.
        '''
        pass


class AgentBootstrap(dict):
    ''' Threadsafe. Handles all of the stuff needed to actually build an
    agent and keep consistent state across devices / logins.
    '''
    def __init__(self, agent, obj):
        self._mutlock = threading.Lock()
        self._agent = agent
        self._obj = obj
        self._def = None
        
    @classmethod
    def from_existing(cls, agent, obj):
        self = cls(agent, obj)
        self._def = obj.state
        super().update(msgpack.unpackb(obj.state))
        
    def __setitem__(self, key, value):
        with self._mutlock:
            super().__setitem__(key, value)
            self._update_def()
        
    def __getitem__(self, key):
        ''' For now, this is actually just unpacking self._def and 
        returning the value for that. In the future, when we have some
        kind of callback mechanism in dynamic objects, we'll probably
        cache and stuff.
        '''
        with self._mutlock:
            tmp = msgpack.unpackb(self._def)
            return tmp[key]
        
    def __delitem__(self, key):
        with self._mutlock:
            super().__delitem__(key)
            self._update_def()
        
    def pop(*args, **kwargs):
        raise TypeError('AgentBootstrap does not support popping.')
        
    def popitem(*args, **kwargs):
        raise TypeError('AgentBootstrap does not support popping.')
        
    def update(*args, **kwargs):
        with self._mutlock:
            super().update(*args, **kwargs)
            self._update_def()
        
    def _update_def(self):
        self._def = msgpack.packb(self)
        self._agent.update_object(self._obj, self._def)