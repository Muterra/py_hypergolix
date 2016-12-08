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

# External deps
import logging
import os

# This is only used for padding **within** encrypted containers
import random
# import weakref
# import traceback
# import threading

# from golix import SecondParty
from hashlib import sha512
from golix import Ghid
from golix import Secret
from golix import FirstParty

# Internal deps
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_noop
from .hypothetical import fixture_api

from .utils import immortal_property
from .utils import NoContext

from .gao import GAO
from .gao import GAOSet
from .gao import GAODict
from .gao import GAOSetMap

# from golix.utils import AsymHandshake
# from golix.utils import AsymAck
# from golix.utils import AsymNak

# Local dependencies
# from .persistence import _GarqLite
# from .persistence import _GdxxLite


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Inquisitor',
]


# ###############################################
# Library
# ###############################################


class Account(metaclass=API):
    ''' Accounts settle all sorts of stuff.
    
    TODO: move GolixCore into account. That way, everything to do with
    the private keys stays within the account, and is never directly
    accessed outside of it.
    '''
    _identity = immortal_property('__identity')
    _user_id = immortal_property('__user_id')
    
    @public_api
    def __init__(self, root_secret, user_id, *args, **kwargs):
        ''' Gets everything ready for account bootstrapping.
        
        +   root_secret must always be passed. It should be the
            generated secret (through scrypt or whatever) to use for the
            account's root note.
        +   user_id explicitly passed with None means create a new
            Account.
        +   identity explicitly passed with None means load an existing
            account.
        +   user_id XOR identity must be passed.
        '''
        super().__init__(*args, **kwargs)
        
        if user_id is None:
            logger.info(
                'Generating a new set of private keys. Please be patient.'
            )
            self._identity = FirstParty()
            logger.info('Private keys generated.')
        
        else:
            self._identity = None
        
        self._root_secret = root_secret
        self._user_id = user_id
        
    @__init__.fixture
    def __init__(self, identity, *args, **kwargs):
        ''' Lulz just ignore errytang and skip calling super!
        '''
        self._identity = identity
        self.privateer_persistent = {}
        self.privateer_quarantine = {}
        
    async def load_account(self):
        ''' Turns the root_secret and user_id into a directory of GAO
        resources. Then, cleans up the root secret and user_id.
        '''
        try:
            root_node = GAO(
                ghid = self._user_id,
                dynamic = None,
                author = None,
                legroom = 7,
                golcore = self._golcore,
                privateer = self._privateer,
                percore = self._percore,
                librarian = self._librarian,
                master_secret = self._root_secret
            )
            await root_node._pull()
            
        finally:
            del self._root_secret
            
        # Should probably not hard-code the password validator length or summat
        password_validator =                    root_node[0:    64]
        password_comparator =                   root_node[64:   128]
        
        identity_ghid = Ghid.from_bytes(        root_node[128:  193])
        identity_master = Secret.from_bytes(    root_node[193:  246])
        
        persistent_ghid = Ghid.from_bytes(      root_node[246:  311])
        persistent_master = Secret.from_bytes(  root_node[311:  364])
        
        quarantine_ghid = Ghid.from_bytes(      root_node[364:  429])
        quarantine_master = Secret.from_bytes(  root_node[429:  482])
        
        secondary_manifest = Ghid.from_bytes(   root_node[482:  547])
        secondary_master = Secret.from_bytes(   root_node[547:  600])
        
        # This comparison is timing-insensitive; improper generation will be
        # simply comparing noise to noise.
        if sha512(password_validator).digest() != password_comparator:
            logger.critical('Incorrect password.')
            raise ValueError('Incorrect password.')
            
    async def make_account(self):
        ''' Used for account creation, to initialize the root node with
        its resource directory.
        '''
        # Allocate the root node
        #######################################################################
        root_node = GAO(
            ghid = None,
            dynamic = True,
            author = None,
            legroom = 7,
            state = b'you pass butter',
            golcore = self._golcore,
            privateer = self._privateer,
            percore = self._percore,
            librarian = self._librarian,
            master_secret = self._root_secret
        )
        del self._root_secret
        
        # Allocate the identity container
        #######################################################################
        # This stores the private keys
        identity_master = self._identity.new_secret()
        identity_container = GAODict(
            ghid = None,
            dynamic = True,
            author = None,
            legroom = 7,
            golcore = self._golcore,
            privateer = self._privateer,
            percore = self._percore,
            librarian = self._librarian,
            master_secret = identity_master
        )
        
        # Allocate the persistent secret store
        #######################################################################
        # This stores persistent secrets
        privateer_persistent_master = self._identity.new_secret()
        self.privateer_persistent = GAODict(
            ghid = None,
            dynamic = True,
            author = None,
            legroom = 7,
            golcore = self._golcore,
            privateer = self._privateer,
            percore = self._percore,
            librarian = self._librarian,
            master_secret = privateer_persistent_master
        )
        
        # Allocate the quarantined secret store
        #######################################################################
        # This stores quarantined secrets
        privateer_quarantine_master = self._identity.new_secret()
        self.privateer_quarantine = GAODict(
            ghid = None,
            dynamic = True,
            author = None,
            legroom = 7,
            golcore = self._golcore,
            privateer = self._privateer,
            percore = self._percore,
            librarian = self._librarian,
            master_secret = privateer_quarantine_master
        )
        
        # Allocate the secondary manifest
        #######################################################################
        # This contains references to all of the remaining account GAO objects.
        # Their secrets are stored within the persistent lookup.
        secondary_manifest_master = self._identity.new_secret()
        secondary_manifest = GAODict(
            ghid = None,
            dynamic = True,
            author = None,
            legroom = 7,
            golcore = self._golcore,
            privateer = self._privateer,
            percore = self._percore,
            librarian = self._librarian,
            master_secret = secondary_manifest_master
        )
        
        # Save all of the above.
        #######################################################################
        # First we need to load our identity into golcore...
        logger.info('Bootstrapping golcore.')
        self.golcore.bootstrap(self)
        
        # ...and our secret stores into privateer.
        logger.info('Bootstrapping privateer.')
        self.privateer.bootstrap(self)
        
        # We need to initialize: because this gao uses a master secret, the
        # first frame will be unrecoverable.
        logger.info('Allocating root node.')
        await root_node._push()
        await self._salmonator.register(root_node)
        self.oracle._lookup[root_node.ghid] = root_node

        # We need to initialize: because this gao uses a master secret, the
        # first frame will be unrecoverable.
        logger.info('Allocating identity container.')
        await identity_container._push()
        await self._salmonator.register(identity_container)
        self.oracle._lookup[identity_container.ghid] = identity_container

        # We need to initialize: because this gao uses a master secret, the
        # first frame will be unrecoverable.
        logger.info('Allocating persistent secret store.')
        await self.privateer_persistent._push()
        await self._salmonator.register(self.privateer_persistent)
        self.oracle._lookup[self.privateer_persistent.ghid] = \
            self.privateer_persistent

        # We need to initialize: because this gao uses a master secret, the
        # first frame will be unrecoverable.
        logger.info('Allocating quarantined secret store.')
        await self.privateer_quarantine._push()
        await self._salmonator.register(self.privateer_quarantine)
        self.oracle._lookup[self.privateer_quarantine.ghid] = \
            self.privateer_quarantine

        # We need to initialize: because this gao uses a master secret, the
        # first frame will be unrecoverable.
        logger.info('Allocating secondary manifest.')
        await secondary_manifest._push()
        await self._salmonator.register(secondary_manifest)
        self.oracle._lookup[secondary_manifest.ghid] = secondary_manifest
        
        logger.info('Saving identity.')
        identity_container.update(self._identity._serialize())
        await identity_container.push()
        
        logger.info('Building root node...')
        # Generate secure-random-length, pseudorandom-content padding
        logger.info('    Generating noisy padding.')
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
        
        logger.info('   Generating validator and comparator.')
        # We'll use this upon future logins to verify password correctness
        password_validator = os.urandom(64)
        password_comparator = sha512(password_validator).digest()
        
        logger.info('    Serializing primary manifest.')
        root_node.state = (password_validator +
                           password_comparator +
                           bytes(identity_container.ghid) +
                           bytes(identity_master) +
                           bytes(self.privateer_persistent.ghid) +
                           bytes(privateer_persistent_master) +
                           bytes(self.privateer_quarantine.ghid) +
                           bytes(privateer_quarantine_master) +
                           bytes(secondary_manifest.ghid) +
                           bytes(secondary_manifest_master) +
                           padding)
        
        logger.info('Saving root node.')
        await root_node.push()
        
        #######################################################################
        #######################################################################
        # ROOT NODE CREATION (PRIMARY BOOTSTRAP) COMPLETE!
        #######################################################################
        #######################################################################
        
        # Rolodex bootstrap:
        # ----------------------------------------------------------
        logger.info('Building sharing subsystem.')
        
        # Dict-like mapping of all pending requests.
        # Used to lookup {<request address>: <target address>}
        self.pending_requests = await self.oracle.new_object(
            gaoclass = GAODict,
            dynamic = True
        )
        secondary_manifest['rolodex.pending'] = self.pending_requests.ghid
        
        self.outstanding_shares = await self.oracle.new_object(
            gaoclass = GAOSetMap,
            dynamic = True
        )
        secondary_manifest['rolodex.outstanding'] = \
            self.outstanding_shares.ghid
        
        self.rolodex.bootstrap(self)
        
        # Dispatch bootstrap:
        # ----------------------------------------------------------
        logger.info('Building object dispatch.')
        
        # Set of all known tokens. Add b'\x00\x00\x00\x00' to prevent its
        # use. Persistent across all clients for any given agent.
        self.all_tokens = await self.oracle.new_object(
            gaoclass = GAOSet,
            dynamic = True
        )
        self.all_tokens.add(b'\x00\x00\x00\x00')
        secondary_manifest['dispatch.alltokens'] = self.all_tokens.ghid
        
        # SetMap of all objects to be sent to an app upon app startup.
        self.startup_objs = await self.oracle.new_object(
            gaoclass = GAOSetMap,
            dynamic = True
        )
        secondary_manifest['dispatch.startup'] = self.startup_objs.ghid
        
        # Dict-like lookup for <private obj ghid>: <parent token>
        self.private_by_ghid = await self.oracle.new_object(
            gaoclass = GAODict,
            dynamic = True
        )
        secondary_manifest['dispatch.private'] = self.private_by_ghid.ghid
        
        # TODO: figure out a distributed lock system
        self.token_lock = NoContext()
        
        self.incoming_shares = await self.oracle.new_object(
            gaoclass = GAOSet,
            dynamic = True
        )
        secondary_manifest['dispatch.incoming'] = self.incoming_shares.ghid
        
        self.orphan_acks = await self.oracle.new_object(
            gaoclass = GAOSetMap,
            dynamic = True
        )
        secondary_manifest['dispatch.orphan_acks'] = self.orphan_acks.ghid
        
        self.orphan_naks = await self.oracle.new_object(
            gaoclass = GAOSetMap,
            dynamic = True
        )
        secondary_manifest['dispatch.orphan_naks'] = self.orphan_naks.ghid
        
        # And now bootstrap.
        self.dispatch.bootstrap(self)
        
        logger.info('Updating secondary manifest.')
        await self.all_tokens.push()
        await secondary_manifest.push()
        
        logger.info('Account created successfully. Continuing with startup.')
    
    async def _bootstrap_primary(self):
        ''' Pull and initialize (or push, for the initial account
        creation) the objects making up the account that have master
        secrets.
        '''
        
    async def _bootstrap_secondary(self):
        ''' Pull and initialize (or push, for the initial account
        creation) the objects making up the account that lack master
        secrets.
        '''
        
    async def _bootstrap_single(self, gao):
        ''' Perform a single GAO bootstrap.
        
        Note that this is basically just copying oracle.get_object and
        oracle.new_object, until such time as that functionality is
        moved somewhere more appropriate.
        '''
        # This is the initial account creation.
        if gao.ghid is None:
            await gao._push()
            await self._salmonator.register(gao)
            
        # This is subsequent account restoration.
        else:
            await self._salmonator.register(gao)
            await self._salmonator.attempt_pull(gao.ghid, quiet=True)
            await gao._pull()
            
        self.oracle._lookup[gao.ghid] = gao


class Accountant:
    ''' The accountant handles all sorts of stuff to make distributed
    access possible. For example, it should track device IDs associated
    with the account, for the purposes of making a distributed GAO lock,
    etc etc etc.
    '''
    pass
