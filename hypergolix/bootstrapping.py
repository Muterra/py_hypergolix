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
import weakref
import os

from Crypto.Protocol.KDF import scrypt
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import HKDF

import pyscrypt

from golix import Ghid
from golix import Secret
from golix import FirstParty

# Intra-package dependencies
from .core import GolixCore
from .core import Oracle
from .core import GhidProxier
from .core import _GAO
from .core import _GAOSet
from .core import _GAODict
from .core import _GAOSetMap

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
    def __init__(self, aengel=None, debug=False):
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
        
    def assemble(self):
        # Now we need to link everything together.
        self.percore.assemble(self.doorman, self.enforcer, self.lawyer, 
                            self.bookie, self.librarian, self.postman,
                            self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.lawyer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore)
        self.postman.assemble(self.golcore, self.librarian, self.bookie, 
                            self.rolodex)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        self.salmonator.assemble(self.golcore, self.percore, self.doorman,
                                self.postman, self.librarian)
        self.golcore.assemble(self.librarian)
        self.privateer.assemble(self.golcore, self.ghidproxy, self.oracle)
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
            
    def bootstrap_zero(self, password):
        ''' Bootstrap zero is run on the creation of a new account, to
        initialize everything and set it up and stuff.
        
        Will return the user_id.
        
        Note: this whole thing is extremely sensitive to order. There's
        definitely a little bit of black magic in here.
        '''
        # Delete this once we start passing an actual password
        password = b'hello world'
        
        # Primary bootstrap (golix core, privateer, and secondary manifest)
        # ----------------------------------------------------------
        
        # First create a new credential
        logger.info('Generating a new credential.')
        credential = Credential.new(password)
        del password
        # Now publish its public keys to percore, so we can create objects
        logger.info('Publishing credential public keys.')
        self.percore.ingest(credential.identity.second_party.packed)
        # Now bootstrap golcore with the credential, so we can create objects
        logger.info('Bootstrapping the golix core.')
        self.golcore.bootstrap(credential)
        # Now prep privateer to create the bootstrap objects
        self.privateer.prep_bootstrap()
        
        # Now we need to create the primary bootstrap objects (those which 
        # descend only from the credential's master secrets)
        # DON'T PUBLISH OUR IDENTITY until after we've set up privateer fully!
        logger.info('Creating primary bootstrap bindings and manifest.')
        identity_container = self.oracle.new_object(
            gaoclass = _GAODict,
            dynamic = True,
            state = {}
        )
        persistent_secrets = self.oracle.new_object(
            gaoclass = _GAODict,
            dynamic = True,
            state = {}
        )
        quarantine_secrets = self.oracle.new_object(
            gaoclass = _GAODict,
            dynamic = True,
            state = {}
        )
        # Also, preallocate the primary manifest.
        primary_manifest = self.oracle.new_object(
            gaoclass = _GAO,
            dynamic = True,
            state = b'hello world'
        )
        
        # We also need to update credential about them before we're ready to 
        # bootstrap the privateer
        credential.declare_primary(
            primary_manifest.ghid,
            identity_container.ghid,
            persistent_secrets.ghid,
            quarantine_secrets.ghid
        )
        
        # Now we're ready to bootstrap the privateer to bring it fully online.
        logger.info('Bootstrapping the symmetric key store.')
        self.privateer.bootstrap(
            persistent = persistent_secrets,
            quarantine = quarantine_secrets,
            credential = credential
        )
        
        # We should immediately update our identity container in case something
        # goes wrong.
        logger.info('Saving private keys to encrypted container.')
        identity_container.update(credential.identity._serialize())
        
        # Now that the privateer is fully online, we can create the secondary
        # manifest.
        logger.info('Creating the secondary bootstrap manifest.')
        secondary_manifest = self.oracle.new_object(
            gaoclass = _GAODict,
            dynamic = True,
            state = {}
        )
        credential.secondary_manifest = secondary_manifest.ghid
        
        # Okay, now the credential is completed. We should save it in case 
        # anything goes awry
        logger.info('Saving credential.')
        credential.save(primary_manifest)
        logger.info('Credential saved. Proceeding to secondary bootstrap.')
        
        # Rolodex bootstrap:
        # ----------------------------------------------------------
        logger.info('Bootstrapping sharing subsystem.')
        
        # Dict-like mapping of all pending requests.
        # Used to lookup {<request address>: <target address>}
        pending_requests = self.oracle.new_object(
            gaoclass = _GAODict,
            dynamic = True,
            state = {}
        )
        secondary_manifest['rolodex.pending'] = pending_requests.ghid
        
        outstanding_shares = self.oracle.new_object(
            gaoclass = _GAOSetMap,
            dynamic = True,
            state = SetMap()
        )
        secondary_manifest['rolodex.outstanding'] = outstanding_shares.ghid
        
        self.rolodex.bootstrap(
            pending_requests = pending_requests, 
            outstanding_shares = outstanding_shares
        )
        
        # Dispatch bootstrap:
        # ----------------------------------------------------------
        logger.info('Bootstrapping object dispatch.')
        
        # Set of all known tokens. Add b'\x00\x00\x00\x00' to prevent its 
        # use. Persistent across all clients for any given agent.
        all_tokens = self.oracle.new_object(
            gaoclass = _GAOSet,
            dynamic = True,
            state = set()
        )
        all_tokens.add(b'\x00\x00\x00\x00')
        secondary_manifest['dispatch.alltokens'] = all_tokens.ghid
        
        # SetMap of all objects to be sent to an app upon app startup.
        startup_objs = self.oracle.new_object(
            gaoclass = _GAOSetMap,
            dynamic = True,
            state = SetMap()
        )
        secondary_manifest['dispatch.startup'] = startup_objs.ghid
        
        # Dict-like lookup for <private obj ghid>: <parent token>
        private_by_ghid = self.oracle.new_object(
            gaoclass = _GAODict,
            dynamic = True,
            state = {}
        )
        secondary_manifest['dispatch.private'] = private_by_ghid.ghid
        
        # And now bootstrap.
        self.dispatch.bootstrap(
            all_tokens = all_tokens, 
            startup_objs = startup_objs, 
            private_by_ghid = private_by_ghid,
            # TODO: figure out a distributed lock system
            token_lock = threading.Lock()
        )
        
        # IPCCore bootstrap:
        # ----------------------------------------------------------
        logger.info('Bootstrapping inter-process communication core.')
        
        incoming_shares = self.oracle.new_object(
            gaoclass = _GAOSet,
            dynamic = True,
            state = set()
        )
        secondary_manifest['ipc.incoming'] = incoming_shares.ghid
        
        orphan_acks = self.oracle.new_object(
            gaoclass = _GAOSetMap,
            dynamic = True,
            state = SetMap()
        )
        secondary_manifest['ipc.orphanacks'] = orphan_acks.ghid
        
        orphan_naks = self.oracle.new_object(
            gaoclass = _GAOSetMap,
            dynamic = True,
            state = SetMap()
        )
        secondary_manifest['ipc.orphannaks'] = orphan_naks.ghid
        
        self.ipccore.bootstrap(
            incoming_shares = incoming_shares,
            orphan_acks = orphan_acks,
            orphan_naks = orphan_naks
        )
        
        logger.info('Bootstrap completed successfully. Continuing with setup.')
        
        # And don't forget to return the user_id
        return primary_manifest.ghid
            
    def bootstrap(self, user_id, password):
        ''' Called to reinstate an existing account.
        '''
        self.privateer.prep_bootstrap()

        
class Credential:
    ''' Handles password expansion into a master key, master key into
    purposeful Secrets, etc.
    '''
    def __init__(self, identity, primary_master, identity_master, 
                persistent_master, quarantine_master):
        self.identity = identity
        
        # Just set this as an empty ghid for now, so it's always ignored
        self._user_id = None
        self._primary_master = primary_master
        
        # The ghid and master secret for the identity private key container
        self._identity_ghid = None
        self._identity_master = identity_master
        # The ghid and master secret for the privateer persistent store
        self._persistent_ghid = None
        self._persistent_master = persistent_master
        # The ghid and master secret for the privateer quarantine store
        self._quarantine_ghid = None
        self._quarantine_master = quarantine_master
        # The ghid for the secondary manifest. No master secret needed.
        self.secondary_manifest = None
        
    def is_primary(self, ghid):
        ''' Checks to see if the ghid is one of the primary bootstrap
        objects. Returns True/False.
        '''
        if not self.prepped:
            raise RuntimeError(
                'Credential must be prepped before checking for primary ghids.'
            )
            
        return (ghid == self._identity_ghid or
                ghid == self._persistent_ghid or
                ghid == self._quarantine_ghid or
                ghid == self._user_id)
        
    def declare_primary(self, user_id, identity_ghid, persistent_ghid, 
                        quarantine_ghid):
        ''' Declares all of the primary bootstrapping addresses, making
        the credential fully-prepped.
        '''
        self._user_id = user_id
        self._identity_ghid = identity_ghid
        self._persistent_ghid = persistent_ghid
        self._quarantine_ghid = quarantine_ghid
        
    @property
    def prepped(self):
        ''' Checks to see that we're ready for use for master secret
        lookup: namely, that we have proxy addresses for all three
        primary bootstrap objects.
        '''
        return (self._identity_ghid is not None and
                self._persistent_ghid is not None and
                self._quarantine_ghid is not None and
                self._user_id is not None)
        
    @property
    def complete(self):
        ''' Checks to see whether the credential is both prepped AND has
        a defined self.secondary_manifest. 
        '''
        return (self.prepped and self.secondary_manifest is not None)
        
    def get_master(self, proxy):
        ''' Returns a master secret for the passed proxy. Proxy should
        be a bootstrapping container, basically either the one for the
        Golix private key container, or the two Privateer secrets repos.
        '''
        if not self.prepped:
            raise RuntimeError(
                'Credential must know its various addresses before being used '
                'to track master secrets.'
            )
            
        else:
            lookup = {
                self._identity_ghid: self._identity_master,
                self._persistent_ghid: self._persistent_master,
                self._quarantine_ghid: self._quarantine_master,
                self._user_id: self._primary_master,
            }
            return lookup[proxy]
        
    @classmethod
    def new(cls, password):
        ''' Generates a new credential. Does NOT containerize it, nor 
        does it send it to the persistence system, etc etc. JUST gets it
        up and going.
        '''
        logger.info('Generating a new set of private keys. Please be patient.')
        identity = FirstParty()
        logger.info('Private keys generated.')
        # Expand the password into the primary master key
        logger.info('Expanding password using scrypt. Please be patient.')
        # Note: with pure python scrypt, this is taking me approx 90-120 sec
        primary_master = cls._password_expansion(identity.ghid, password)
        logger.info('Password expanded.')
        # Might as well do this immediately
        del password
        
        self = cls(
            identity = identity,
            primary_master = primary_master,
            identity_master = identity.new_secret(),
            persistent_master = identity.new_secret(),
            quarantine_master = identity.new_secret()
        )
        
        return self
    
    @classmethod
    def load(cls, oracle, user_id, password):
        ''' Loads a credential container from the <librarian>, with a 
        ghid of <user_id>, encrypted with scrypted <password>.
        '''
        # User_id resolves the "primary manifest", a dynamic object containing:
        #   <private key container dynamic ghid>
        #   <private key container master secret>
        #   <privateer persistent store dynamic ghid>
        #   <privateer persistent store master secret>
        #   <privateer quarantine store dynamic ghid>
        #   <privateer quarantine master secret>
        #   <secondary manifest dynamic ghid>
        #   <random length, random fill padding>
        
        # The primary manifest is encrypted via the privateer.ratchet_bootstrap
        # process, using the inflated password as the master secret.
        
        # The secondary manifest secret is maintained by the privateer. From
        # there forwards, everything is business as usual.
        
        logger.info(
            'Recovering the primary manifest from the persistence subsystem.'
        )
        primary_manifest = oracle.get_object(
            gaoclass = _GAO,
            ghid = user_id
        )
        user_fingerprint = primary_manifest.author
        logger.info('Expanding password using scrypt. Please be patient.')
        primary_master = cls._password_expansion(user_fingerprint, password)
        del password
        
    def save(self, primary_manifest):
        ''' Containerizes the credential, sending it to the persistence
        core to be retained. Returns the resulting user_id for loading.
        '''
        # User_id resolves the "primary manifest", a dynamic object containing:
        #   <private key container dynamic ghid>                65b
        #   <private key container master secret>               53b
        #   <privateer persistent store dynamic ghid>           65b
        #   <privateer persistent store master secret>          53b
        #   <privateer quarantine store dynamic ghid>           65b
        #   <privateer quarantine master secret>                53b
        #   <secondary manifest dynamic ghid>                   65b
        #   <random length, random fill padding>
        
        # Check to make sure we're capable of doing this
        if not self.complete:
            raise RuntimeError(
                'Credentials must be fully completed before saving. This '
                'requires all three primary bootstrap ghids to be defined, as '
                'well as the secondary manifest ghid.'
            )
        
        # Generate random-length, random-content padding
        logger.info('Generating noisy padding.')
        padding_sizes = range(1024, 3072, 8)
        padding_choice = int.from_bytes(os.urandom(1), byteorder='big')
        padding = os.urandom(padding_sizes[padding_choice])
        
        # Serialize the manifest as per above
        logger.info('Serializing primary manifest.')
        manifest = (bytes(self._identity_ghid) + 
                    bytes(self._identity_master) + 
                    bytes(self._persistent_ghid) + 
                    bytes(self._persistent_master) +
                    bytes(self._quarantine_ghid) + 
                    bytes(self._quarantine_master) + 
                    bytes(self.secondary_manifest) +
                    padding)
        
        primary_manifest.apply_state(manifest)
        logger.info('Pushing credential to persistence core.')
        primary_manifest.push()
            
    @staticmethod
    def _password_expansion(salt_ghid, password):
        ''' Expands the author's ghid and password into a master key for
        use in generating specific keys.
        '''
        # Scrypt the password. Salt against the author GHID.
        # Use 2**14 for t<=100ms, 2**20 for t<=5s
        combined = pyscrypt.hash(
            password = password, 
            salt = bytes(salt_ghid),
            dkLen = 48,
            N = 2**15,
            r = 8,
            p = 1
        )
        key = combined[0:32]
        seed = combined[32:48]
        master_secret = Secret(
            cipher = 1, 
            version = 'latest',
            key = key,
            seed = seed
        )
        return master_secret