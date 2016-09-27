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

# Global dependencies
import threading
import weakref
import os
import random

from Crypto.Protocol.KDF import scrypt

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
from .persistence import DiskLibrarian
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
# Boilerplate
# ###############################################


# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'AgentBootstrap',
]


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Utilities, etc
# ###############################################


# Use 2**14 for t<=100ms, 2**20 for t<=5s.
_DEFAULT_SCRYPT_HARDNESS = 2**15
# 256-bit password validator.
_PASSWORD_VALIDATOR_LEN = 32


class AgentBootstrap:
    ''' Agent bootstraps create and assemble the individual components
    needed to run the hypergolix service from a username and password.
    
    Also binds everything within a single namespace, etc etc.
    '''
    def __init__(self, cache_dir, aengel=None, debug=False):
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
        self.librarian = DiskLibrarian(cache_dir=cache_dir)
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
        
    @property
    def whoami(self):
        # Proxy for golcore whoami.
        return self.golcore.whoami
        
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
            
    def bootstrap_zero(self, password, _scrypt_hardness=None):
        ''' Bootstrap zero is run on the creation of a new account, to
        initialize everything and set it up and stuff.
        
        Will return the user_id.
        
        Note: this whole thing is extremely sensitive to order. There's
        definitely a little bit of black magic in here.
        '''        
        # Primary bootstrap (golix core, privateer, and secondary manifest)
        # ----------------------------------------------------------
        
        # First create a new credential
        logger.info('Generating a new credential.')
        credential = Credential.new(password, _scrypt_hardness)
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
        logger.info('Creating primary bootstrap objects.')
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
            # _GAOs don't like being committed with no content
            state = b'hello world'
        )
        # And the secondary manifest.
        secondary_manifest = self.oracle.new_object(
            gaoclass = _GAODict,
            dynamic = True,
            state = {}
        )
        
        # We also need to update credential about them before we're ready to 
        # bootstrap the privateer
        credential.declare_primary(
            primary_manifest.ghid,
            identity_container.ghid,
            persistent_secrets.ghid,
            quarantine_secrets.ghid,
            secondary_manifest.ghid
        )
        
        # Now we're ready to bootstrap the privateer to bring it fully online.
        logger.info('Bootstrapping the symmetric key store.')
        self.privateer.bootstrap(
            persistent = persistent_secrets,
            quarantine = quarantine_secrets,
            credential = credential
        )
        # Now that privateer has been bootstrapped, we should forcibly update
        # both secrets lookups so that they cannot possibly have a zero history
        # (which would prevent us from reloading them!)
        # We don't need to do this with the other containers, because all of 
        # them will definitely have changes. Quarantine secrets is particularly
        # susceptible to "not having an extra secret").
        persistent_secrets.push()
        quarantine_secrets.push()
        
        # We should immediately update our identity container in case something
        # goes wrong.
        logger.info('Saving private keys to encrypted container.')
        identity_container.update(credential.identity._serialize())
        
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
        secondary_manifest['ipc.orphan_acks'] = orphan_acks.ghid
        
        orphan_naks = self.oracle.new_object(
            gaoclass = _GAOSetMap,
            dynamic = True,
            state = SetMap()
        )
        secondary_manifest['ipc.orphan_naks'] = orphan_naks.ghid
        
        self.ipccore.bootstrap(
            incoming_shares = incoming_shares,
            orphan_acks = orphan_acks,
            orphan_naks = orphan_naks
        )
        
        logger.info('Bootstrap completed successfully. Continuing with setup.')
        
        # And don't forget to return the user_id
        return primary_manifest.ghid
            
    def bootstrap(self, user_id, password, _scrypt_hardness=None):
        ''' Called to reinstate an existing account.
        '''
        # FIRST FIRST, we have to restore the librarian.
        self.librarian.restore()
        
        # Note that FirstParty digestion methods are all classmethods, so we 
        # can prep it to be the class, and then bootstrap it to be the actual
        # credential.
        logger.info('Prepping Hypergolix for login.')
        self.golcore.prep_bootstrap(FirstParty)
        self.privateer.prep_bootstrap()
        
        # Load the credential
        logger.info('Loading credential.')
        credential = Credential.load(
            self.librarian,
            self.oracle,
            self.privateer,
            user_id,
            password,
            _scrypt_hardness = _scrypt_hardness,
        )
        del password
        logger.info(
            'Credential loaded. Loading symmetric key stores and rebooting '
            'Golix core.'
        )
        
        # Properly bootstrap golcore and then load the privateer objects
        self.golcore.bootstrap(credential)
        
        persistent_secrets = self.oracle.get_object(
            gaoclass = _GAODict,
            ghid = credential._persistent_ghid
        )
        quarantine_secrets = self.oracle.get_object(
            gaoclass = _GAODict,
            ghid = credential._quarantine_ghid
        )
        secondary_manifest = self.oracle.get_object(
            gaoclass = _GAODict,
            ghid = credential._secondary_manifest
        )
        
        # Bootstrap privateer.
        self.privateer.bootstrap(
            persistent = persistent_secrets,
            quarantine = quarantine_secrets,
            credential = credential
        )
        
        # Start loading the other bootstrap objects.
        logger.info('Golix core restarted. Restoring secondary manifest.')
        # Rolodex
        rolodex_pending = secondary_manifest['rolodex.pending']
        rolodex_outstanding = secondary_manifest['rolodex.outstanding']
        # Dispatch
        dispatch_alltokens = secondary_manifest['dispatch.alltokens']
        dispatch_startup = secondary_manifest['dispatch.startup']
        dispatch_private = secondary_manifest['dispatch.private']
        # IPCcore
        ipc_incoming = secondary_manifest['ipc.incoming']
        ipc_orphan_acks = secondary_manifest['ipc.orphan_acks']
        ipc_orphan_naks = secondary_manifest['ipc.orphan_naks']
        
        # Reboot rolodex
        logger.info('Restoring sharing subsystem.')
        pending_requests = self.oracle.get_object(
            gaoclass = _GAODict,
            ghid = rolodex_pending
        )
        outstanding_shares = self.oracle.get_object(
            gaoclass = _GAOSetMap,
            ghid = rolodex_outstanding
        )
        self.rolodex.bootstrap(
            pending_requests = pending_requests,
            outstanding_shares = outstanding_shares
        )
        
        # Reboot dispatch
        logger.info('Restoring object dispatch subsystem.')
        all_tokens = self.oracle.get_object(
            gaoclass = _GAOSet,
            ghid = dispatch_alltokens
        )
        startup_objs = self.oracle.get_object(
            gaoclass = _GAOSetMap,
            ghid = dispatch_startup
        )
        private_by_ghid = self.oracle.get_object(
            gaoclass = _GAODict,
            ghid = dispatch_private
        )
        self.dispatch.bootstrap(
            all_tokens = all_tokens, 
            startup_objs = startup_objs, 
            private_by_ghid = private_by_ghid,
            # TODO: figure out a distributed lock system
            token_lock = threading.Lock()
        )
        
        # Reboot IPCCore
        logger.info('Restoring inter-process communication core.')
        incoming_shares = self.oracle.get_object(
            gaoclass = _GAOSet,
            ghid = ipc_incoming
        )
        orphan_acks = self.oracle.get_object(
            gaoclass = _GAOSetMap,
            ghid = ipc_orphan_acks
        )
        orphan_naks = self.oracle.get_object(
            gaoclass = _GAOSetMap,
            ghid = ipc_orphan_naks
        )
        self.ipccore.bootstrap(
            incoming_shares = incoming_shares,
            orphan_acks = orphan_acks,
            orphan_naks = orphan_naks
        )
        
        logger.info('Bootstrap completed successfully. Continuing with setup.')

        
class Credential:
    ''' Handles password expansion into a master key, master key into
    purposeful Secrets, etc.
    '''
    def __init__(self, identity, primary_master, identity_master, 
                persistent_master, quarantine_master, secondary_master):
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
        # The ghid and master secret for the secondary manifest.
        self._secondary_manifest = None
        self._secondary_master = secondary_master
        
        self._scrypt_hardness = None
        
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
                ghid == self._user_id or
                ghid == self._secondary_manifest)
        
    def declare_primary(self, user_id, identity_ghid, persistent_ghid,
                        quarantine_ghid, secondary_ghid):
        ''' Declares all of the primary bootstrapping addresses, making
        the credential fully-prepped.
        '''
        self._user_id = user_id
        self._identity_ghid = identity_ghid
        self._persistent_ghid = persistent_ghid
        self._quarantine_ghid = quarantine_ghid
        self._secondary_manifest = secondary_ghid
        
    @property
    def prepped(self):
        ''' Checks to see that we're ready for use for master secret
        lookup: namely, that we have proxy addresses for all three
        primary bootstrap objects.
        '''
        return (self._identity_ghid is not None and
                self._persistent_ghid is not None and
                self._quarantine_ghid is not None and
                self._user_id is not None and
                self._secondary_manifest is not None)
        
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
                self._secondary_manifest: self._secondary_master,
            }
            return lookup[proxy]
        
    @classmethod
    def new(cls, password, _scrypt_hardness=None):
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
        primary_master = cls._password_expansion(
            identity.ghid, 
            password, 
            _scrypt_hardness,
        )
        logger.info('Password expanded.')
        # Might as well do this immediately
        del password
        
        self = cls(
            identity = identity,
            primary_master = primary_master,
            identity_master = identity.new_secret(),
            persistent_master = identity.new_secret(),
            quarantine_master = identity.new_secret(),
            secondary_master = identity.new_secret(),
        )
        
        if _scrypt_hardness:
            self._scrypt_hardness = _scrypt_hardness
        
        return self
        
    @classmethod
    def _inject_secret(cls, librarian, privateer, proxy, master_secret):
        ''' Injects a container secret into the temporary storage at the
        privateer. Calculates it through the proxy and master.
        '''
        binding = librarian.summarize(proxy)
        previous_frame = binding.history[0]
        container_secret = privateer._ratchet(
            secret = master_secret, 
            proxy = proxy,
            salt_ghid = previous_frame
        )
        privateer.stage(binding.target, container_secret)
    
    @classmethod
    def load(cls, librarian, oracle, privateer, user_id, password,
            _scrypt_hardness=None):
        ''' Loads a credential container from the <librarian>, with a
        ghid of <user_id>, encrypted with scrypted <password>.
        '''
        # User_id resolves the "primary manifest", a dynamic object containing:
        #   <private key container dynamic ghid>                65b
        #   <private key container master secret>               53b
        #   <privateer persistent store dynamic ghid>           65b
        #   <privateer persistent store master secret>          53b
        #   <privateer quarantine store dynamic ghid>           65b
        #   <privateer quarantine master secret>                53b
        #   <secondary manifest dynamic ghid>                   65b
        #   <secondary manifest master secret>                  53b
        #   <random length, random fill padding>
        
        # The primary manifest is encrypted via the privateer.ratchet_bootstrap
        # process, using the inflated password as the master secret.
        
        # The secondary manifest secret is maintained by the privateer. From
        # there forwards, everything is business as usual.
        
        logger.info(
            'Recovering the primary manifest from the persistence subsystem.'
        )
        
        primary_manifest = librarian.summarize(user_id)
        fingerprint = primary_manifest.author
        logger.info('Expanding password using scrypt. Please be patient.')
        primary_master = cls._password_expansion(
            fingerprint,
            password,
            _scrypt_hardness
        )
        del password
        
        # Calculate the primary secret and then inject it into the temporary
        # storage at privateer
        cls._inject_secret(
            librarian,
            privateer,
            proxy = user_id,
            master_secret = primary_master
        )
        # We're done with the summary, so go ahead and overwrite this name
        primary_manifest = oracle.get_object(
            gaoclass = _GAO,
            ghid = user_id
        )
        
        logger.info(
            'Password successfully expanded. Extracting the primary manifest.'
        )
        manifest = primary_manifest.extract_state()
        
        logger.info('Verifying password. Please be patient.')
        # Check the password so we can fail with a meaningful message!
        # Should probably not hard-code the password validator length or summat
        password_validator = manifest[0:32]
        checker = cls._make_password_validator(
            secret = primary_master,
            hardness = _scrypt_hardness
        )
        if checker != password_validator:
            logger.critical('Incorrect password.')
            raise ValueError('Incorrect password.')
            
        else:
            logger.info(
                'Password correct. Proceeding with manifest extraction.'
            )
            
        identity_ghid = Ghid.from_bytes(manifest[32:97])
        identity_master = Secret.from_bytes(manifest[97:150])
        persistent_ghid = Ghid.from_bytes(manifest[150:215])
        persistent_master = Secret.from_bytes(manifest[215:268])
        quarantine_ghid = Ghid.from_bytes(manifest[268:333])
        quarantine_master = Secret.from_bytes(manifest[333:386])
        secondary_manifest = Ghid.from_bytes(manifest[386:451])
        secondary_master = Secret.from_bytes(manifest[451:504])
        # Inject all the needed secrets.
        cls._inject_secret(
            librarian, 
            privateer, 
            proxy = identity_ghid, 
            master_secret = identity_master
        )
        cls._inject_secret(
            librarian, 
            privateer, 
            proxy = persistent_ghid, 
            master_secret = persistent_master
        )
        cls._inject_secret(
            librarian, 
            privateer, 
            proxy = quarantine_ghid, 
            master_secret = quarantine_master
        )
        cls._inject_secret(
            librarian, 
            privateer, 
            proxy = secondary_manifest, 
            master_secret = secondary_master
        )
        
        logger.info('Manifest recovered. Retrieving private keys.')
        identity_container = oracle.get_object(
            gaoclass = _GAODict,
            ghid = identity_ghid
        )
        identity = FirstParty._from_serialized(
            identity_container.extract_state()
        )
        
        logger.info('Rebuilding credential.')
        self = cls(
            identity = identity,
            primary_master = primary_master,
            identity_master = identity_master,
            persistent_master = persistent_master,
            quarantine_master = quarantine_master,
            secondary_master = secondary_master
        )
        
        if _scrypt_hardness:
            self._scrypt_hardness = _scrypt_hardness
        
        self.declare_primary(
            user_id,
            identity_ghid,
            persistent_ghid,
            quarantine_ghid,
            secondary_manifest
        )
        
        return self
        
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
        #   <secondary manifest master secret>                  53b
        #   <random length, random fill padding>
        
        # Check to make sure we're capable of doing this
        if not self.prepped:
            raise RuntimeError(
                'Credentials must be fully declared before saving. This '
                'requires all four primary bootstrap ghids to be defined, as '
                'well as their master secrets.'
            )
        
        # Generate secure-random-length, pseudorandom-content padding
        logger.info('Generating noisy padding.')
        # Note that we don't actually need CSRNG for the padding, just the
        # padding length, since the whole thing is encrypted. We could just as
        # easily fill it with zeros, but by filling it with pseudorandom noise,
        # we can remove a recognizable pattern and therefore slighly hinder
        # brute force attacks against the password.
        # While we COULD use CSRNG despite all that, entropy is a limited 
        # resource, and I'd rather conserve it as much as possible.
        padding_seed = int.from_bytes(os.urandom(2), byteorder='big')
        padding_min_size = 1024
        padding_clip_mask = 0b0001111111111111
        # Clip the seed to an upper range of 13 bits, of 8191, for a maximum
        # padding length of 8191 + 1024 = 9215 bytes
        padding_len = padding_min_size + (padding_seed & padding_clip_mask)
        padding_int = random.getrandbits(padding_len * 8)
        padding = padding_int.to_bytes(length=padding_len, byteorder='big')
        
        logger.info('Generating the password validator. Please be patient.')
        # Check the password so we can fail with a meaningful message!
        password_validator = self._make_password_validator(
            secret = self._primary_master,
            hardness = self._scrypt_hardness
        )
        
        # Serialize the manifest as per above
        logger.info('Serializing primary manifest.')
        manifest = (password_validator +
                    bytes(self._identity_ghid) + 
                    bytes(self._identity_master) + 
                    bytes(self._persistent_ghid) + 
                    bytes(self._persistent_master) +
                    bytes(self._quarantine_ghid) + 
                    bytes(self._quarantine_master) + 
                    bytes(self._secondary_manifest) +
                    bytes(self._secondary_master) +
                    padding)
        
        primary_manifest.apply_state(manifest)
        logger.info('Pushing credential to persistence core.')
        primary_manifest.push()
            
    @staticmethod
    def _password_expansion(salt_ghid, password, hardness=None):
        ''' Expands the author's ghid and password into a master key for
        use in generating specific keys.
        
        Hardness allows you to modify the scrypt inflation parameter. It
        defaults to something resembling a reasonable general-purpose 
        value for 2016.
        '''
        # Use 2**14 for t<=100ms, 2**20 for t<=5s.
        if hardness is None:
            hardness = _DEFAULT_SCRYPT_HARDNESS
        else:
            hardness = int(hardness)
        
        # Scrypt the password. Salt against the author GHID.
        combined = scrypt(
            password = password, 
            salt = bytes(salt_ghid),
            key_len = 48,
            N = hardness,
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
        
    @staticmethod
    def _make_password_validator(secret, hardness=None):
        ''' Re-scrypts the key (should be the scrypt expansion of the
        password) with hardness, and then compares the result against
        check_str. If wrong, raises ValueError for a bad password (and
        logs as such).
        '''
        if hardness is None:
            hardness = _DEFAULT_SCRYPT_HARDNESS
        else:
            hardness = int(hardness)
            
        checker = scrypt(
            password = secret.key,
            salt = bytes(_PASSWORD_VALIDATOR_LEN),
            key_len = _PASSWORD_VALIDATOR_LEN,
            N = hardness,
            r = 8,
            p = 1
        )
        return checker
