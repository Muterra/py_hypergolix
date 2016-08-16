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
    'AgentBootstrap',
]

# Global dependencies
import threading

from Crypto.Protocol.KDF import scrypt
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import HKDF

from golix import Ghid
from golix import FirstParty

# Intra-package dependencies
from .core import GolixCore
from .core import Oracle
from .core import GhidProxier
from .core import _GAOSet
from .core import _GAODict

from .persistence import PersistenceCore
from .persistence import Doorman
from .persistence import Enlitener
from .persistence import Enforcer
from .persistence import Lawyer
from .persistence import Bookie
# from .persistence import DiskLibrarian
from .persistence import MemoryLibrarian
from .persistence import MrPostman
from .persistence import Undertaker
from .persistence import Salmonator

from .dispatch import Dispatcher
from .dispatch import _Dispatchable

from .ipc import IPCCore
from .privateer import Privateer
from .rolodex import Rolodex

from .utils import Aengel
from .utils import threading_autojoin
from .utils import SetMap


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Utilities, etc
# ###############################################


class AgentBootstrap:
    ''' Agent bootstraps create and assemble the individual components
    needed to run the hypergolix service from a username and password.
    
    Also binds everything within a single namespace, etc etc.
    '''
    def __init__(self, credential, bootstrap=None, aengel=None, debug=False):
        ''' Creates everything and puts it into a singular namespace.
        
        If bootstrap (ghid) is passed, we'll use the credential to 
        extract an identity. If bootstrap_ghid is not passed, will use 
        the credential to create one.
        
        TODO: move entire bootstrap creation process (or as much as 
        possible, anyways) into register().
        '''
        # First we need to create everything.
        self.percore = PersistenceCore()
        self.doorman = Doorman()
        self.enforcer = Enforcer()
        self.lawyer = Lawyer()
        self.bookie = Bookie()
        self.librarian = MemoryLibrarian()
        self.postman = MrPostman()
        self.undertaker = Undertaker()
        self.salmonator = Salmonator()
        self.golcore = GolixCore()
        self.privateer = Privateer()
        self.oracle = Oracle()
        self.rolodex = Rolodex()
        self.ghidproxy = GhidProxier()
        self.dispatch = Dispatcher()
        self.ipccore = IPCCore(
            aengel = aengel, 
            threaded = True, 
            thread_name = 'ipccore',
            debug = debug, 
        )
        
        # Now we need to link everything together.
        self.percore.assemble(self.doorman, self.enforcer, self.lawyer, 
                            self.bookie, self.librarian, self.postman,
                            self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.lawyer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore, self.salmonator)
        self.postman.assemble(self.golcore, self.librarian, self.bookie, 
                            self.rolodex)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        self.salmonator.assemble(self.golcore, self.percore, self.doorman,
                                self.postman, self.librarian)
        self.golcore.assemble(self.librarian)
        self.privateer.assemble(self.golcore, self.oracle)
        self.ghidproxy.assemble(self.librarian, self.salmonator)
        self.oracle.assemble(self.golcore, self.ghidproxy, self.privateer,
                            self.percore, self.bookie, self.librarian, 
                            self.postman, self.salmonator)
        self.rolodex.assemble(self.golcore, self.privateer, self.dispatch, 
                            self.percore, self.librarian, self.salmonator,
                            self.ghidproxy, self.ipccore)
        self.dispatch.assemble()
        self.ipccore.assemble(self.golcore, self.oracle, self.dispatch, 
                            self.rolodex, self.salmonator)
            
        # Now we need to bootstrap everything.
        if bootstrap is None:
            # Golix core bootstrap.
            # ----------------------------------------------------------
            identity = FirstParty()
            self.percore.ingest(identity.second_party.packed)
            self.golcore.bootstrap(identity)
            
            # Privateer bootstrap.
            # ----------------------------------------------------------
            self.privateer.prep_bootstrap()
            
            # self.privateer.bootstrap(
            #     persistent_secrets = {}, 
            #     staged_secrets = {},
            #     chains = {}
            # )
            
            # Rolodex bootstrap:
            # ----------------------------------------------------------
            # Dict-like mapping of all pending requests.
            # Used to lookup {<request address>: <target address>}
            pending_requests = self.oracle.new_object(
                gaoclass = _GAODict,
                dynamic = True,
                state = {}
            )
            self.rolodex.bootstrap(
                pending_requests = pending_requests, 
                outstanding_shares = SetMap()
            )
            
            # Dispatch bootstrap:
            # ----------------------------------------------------------
            # Set of all known tokens. Add b'\x00\x00\x00\x00' to prevent its 
            # use. Persistent across all clients for any given agent.
            all_tokens = self.oracle.new_object(
                gaoclass = _GAOSet,
                dynamic = True,
                state = set()
            )
            all_tokens.add(b'\x00\x00\x00\x00')
            # SetMap of all objects to be sent to an app upon app startup.
            # TODO: make this distributed state object.
            startup_objs = SetMap()
            
            # Dict-like lookup for <private obj ghid>: <parent token>
            private_by_ghid = self.oracle.new_object(
                gaoclass = _GAODict,
                dynamic = True,
                state = {}
            )
            
            # And now bootstrap.
            self.dispatch.bootstrap(
                all_tokens = all_tokens, 
                startup_objs = startup_objs, 
                private_by_ghid = private_by_ghid,
                token_lock = threading.Lock()
            )
            
            # IPCCore bootstrap:
            # ----------------------------------------------------------
            self.ipccore.bootstrap(
                incoming_shares = set(),
                orphan_acks = SetMap(),
                orphan_naks = SetMap()
            )
            
        else:
            raise NotImplementedError('Not just yet buddy-o!')
        
    def _new_bootstrap_container(self):
        ''' Creates a new container to use for the bootstrap object.
        '''
        padding_size = int.from_bytes(os.urandom(1), byteorder='big')
        padding = os.urandom(padding_size)
        return self.new_object(padding, dynamic=True)
        
    @classmethod
    def register(cls, password):
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
        secret = Secret(
            cipher = 1,
            key = combined[:32],
            seed = combined[32:48]
        )
        
    @classmethod
    def login(cls, bootstrap_ghid, password):
        ''' Load an Agent from an identity contained within a GEOC.
        '''
        pass
        
class Credential:
    ''' Handles password expansion into a master key, master key into
    purposeful Secrets, etc.
    '''
    def __init__(self, ghid, password):
        self.master = self._password_expansion(ghid, password)
            
    @staticmethod
    def _password_expansion(ghid, password):
        ''' Expands the author's ghid and password into a master key for
        use in generating specific keys.
        '''
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
        
    def _derive_secret(self, salt):
        ''' Derives a Secret from the master key.
        '''
        pass
        
    def get_master(self, proxy):
        ''' Returns a master secret for the passed proxy. Proxy should
        be a bootstrapping container, basically either the one for the
        Golix private key container, or the two Privateer secrets repos.
        '''