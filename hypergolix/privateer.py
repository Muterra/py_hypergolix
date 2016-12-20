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

'''


# External dependencies
import logging
import collections
import weakref
import traceback
import asyncio
import loopa

from golix import Ghid

# These are used for secret ratcheting only.
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf import hkdf
from cryptography.hazmat.backends import default_backend

# Intra-package dependencies
from .utils import weak_property
from .utils import FiniteDict

from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_noop
from .hypothetical import fixture_api

from .exceptions import PrivateerError
from .exceptions import RatchetError
from .exceptions import ConflictingSecrets
from .exceptions import UnknownSecret


# ###############################################
# Boilerplate and constants
# ###############################################


CRYPTO_BACKEND = default_backend()


# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'Privateer',
]


logger = logging.getLogger(__name__)

        
# ###############################################
# Lib
# ###############################################


class _GaoDictBootstrap(dict):
    # Just inject a class-level ghid.
    ghid = Ghid.from_bytes(b'\x01' + bytes(64))


class Privateer(metaclass=API):
    ''' Lookup system to get secret from ghid. Loopsafe, but NOT
    threadsafe.
    '''
    _account = weak_property('__account')
    _golcore = weak_property('__golcore')
    
    @public_api
    def __init__(self, *args, **kwargs):
        ''' Temporarily invalidate (but init) the various secrets
        lookups.
        '''
        super().__init__(*args, **kwargs)
        
        # These must be bootstrapped.
        self._secrets_persistent = None
        self._secrets_quarantine = None
        self._secrets = None
        # These two are local only, but included to make the code more explicit
        self._secrets_staging = None
        self._secrets_local = None
        
    @__init__.fixture
    def __init__(self, identity, *args, **kwargs):
        ''' Directly inject a local identity, and then add a ghidproxy.
        '''
        super(Privateer.__fixture__, self).__init__(*args, **kwargs)
        
        self._identity = identity
        self._secrets_persistent = {}
        self._secrets_staging = {}
        self._secrets_local = {}
        self._secrets_quarantine = {}
        self._secrets = collections.ChainMap(
            self._secrets_persistent,
            self._secrets_local,
            self._secrets_staging,
            self._secrets_quarantine,
        )
        self._secrets_committed = collections.ChainMap(
            self._secrets_persistent,
            self._secrets_local
        )
        
    @fixture_api
    def RESET(self):
        # Clear errything internal.
        self._secrets_persistent.clear()
        self._secrets_staging.clear()
        self._secrets_local.clear()
        self._secrets_quarantine.clear()
            
    def __contains__(self, ghid):
        ''' Check if we think we know a secret for the ghid.
        '''
        return ghid in self._secrets
        
    def assemble(self, golcore):
        # Chicken, meet egg.
        self._golcore = golcore
    
    @fixture_noop
    @public_api
    def bootstrap(self, account):
        ''' Initializes the privateer into a distributed state.
        persistent is a GaoDict
        '''
        self._account = account
        
        # We very obviously need to be able to look up what secrets we have.
        # Lookups: <container ghid>: <container secret>
        self._secrets_persistent = account.privateer_persistent
        self._secrets_staging = {}
        self._secrets_local = {}
        self._secrets_deprecated = FiniteDict(maxlen=100)
        self._secrets_quarantine = account.privateer_quarantine
        self._secrets = collections.ChainMap(
            self._secrets_persistent,
            self._secrets_local,
            self._secrets_deprecated,
            self._secrets_staging,
            self._secrets_quarantine,
        )
        self._secrets_committed = collections.ChainMap(
            self._secrets_persistent,
            self._secrets_local
        )
        
        # Note that we just overwrote any chains that were created when we
        # initially loaded the three above resources. So, we may need to
        # re-create them. But, we can use a fake container address for two of
        # them, since they use a different secrets tracking mechanism.
        
    @public_api
    def new_secret(self):
        # Straight pass-through to the golix new_secret bit.
        return self._golcore._identity.new_secret()
        
    @new_secret.fixture
    def new_secret(self):
        # We have a direct identity for the fixture.
        return self._identity.new_secret()
        
    def get(self, ghid):
        ''' Get a secret for a ghid, regardless of status.
        
        Raises KeyError if secret is not present.
        '''
        try:
            return self._secrets[ghid]
        except KeyError as exc:
            raise UnknownSecret('Secret not found for ' + str(ghid)) from exc
        
    def stage(self, ghid, secret):
        ''' Preliminarily set a secret for a ghid.
        
        If a secret is already staged for that ghid and the ghids are
        not equal, raises ConflictingSecrets.
        '''
        # Is there a reason we can't verify container-ness here?
        logger.debug(''.join((
            'GAO ',
            str(ghid),
            ' secret staging.'
        )))
        self._stage(ghid, secret, self._secrets_staging)
                
    def _stage(self, ghid, secret, lookup):
        ''' Raw staging, bypassing modlock. Only accessed directly
        during bootstrapping.
        '''
        if ghid in self._secrets:
            if self._secrets[ghid] != secret:
                self._calc_and_log_diff(self._secrets[ghid], secret)
                raise ConflictingSecrets(
                    'Non-matching secret already known for ' + str(ghid)
                )
        else:
            lookup[ghid] = secret
            
    def quarantine(self, ghid, secret):
        ''' Store a secret temporarily, but distributedly, separate from
        the primary secret store. Used only for incoming secrets in
        shares that have not yet been opened.
        '''
        logger.debug(''.join((
            'GAO ',
            str(ghid),
            ' secret quarantining.'
        )))
        self._stage(ghid, secret, self._secrets_quarantine)
            
    def unstage(self, ghid):
        ''' Remove a staged secret, probably due to a SecurityError.
        Returns the secret.
        '''
        secret = self._secrets_quarantine.pop(ghid, None)
        secret = secret or self._secrets_staging.pop(ghid, None)
        
        if secret is None:
            raise UnknownSecret(
                'No currently staged secret for GHID ' + str(ghid)
            )
                
        return secret
        
    def commit(self, ghid, localize=False):
        ''' Store a secret "permanently". The secret must either:
        1. Be staged, XOR
        2. Be quarantined, XOR
        3. Be committed
        
        Other states will raise UnknownSecret (a subclass of KeyError).
        
        Note that this relies upon staging logic enforcing consistency
        of secrets! If that changes to allow the staging conflicting
        secrets, this will need to be modified.
        
        This is transactional and atomic; any errors (ex: ValueError
        above) will return its state to the previous.
        
        With localize=True, the secret is committed locally only. This
        is intended to be used in combination with a master secret; in
        that case, there's no need to store frame keys -- not to mention
        that it would cause an infinitely recursive chain of secret
        committing.
        
        '''
        # Catch anything with an unknown secret
        if ghid not in self._secrets:
            raise UnknownSecret(
                'Secret not currently staged for GHID ' + str(ghid)
            )
        
        # Note that we cannot stage or quarantine a secret that we already
        # have committed. If that secret matches what we already have, it
        # will indempotently and silently exit, without staging.
        elif ghid in self._secrets_committed:
            return
        
        secret = self._secrets_quarantine.pop(ghid, None)
        secret = secret or self._secrets_staging.pop(ghid, None)
            
        # If it's a bootstrap target, just locally commit it.
        if localize:
            logger.debug(''.join((
                'GAO ',
                str(ghid),
                ' secret committed locally.'
            )))
            self._secrets_local[ghid] = secret
            
        # Nope. Commit globally.
        else:
            logger.debug(''.join((
                'GAO ',
                str(ghid),
                ' secret committed globally.'
            )))
            self._secrets_persistent[ghid] = secret
    
    @classmethod
    def _calc_and_log_diff(cls, secret, other):
        ''' Calculate the difference between two secrets.
        '''
        try:
            cipher_match = (secret.cipher == other.cipher)
            
            if cipher_match:
                key_comp = cls._bitdiff(secret.key, other.key)
                seed_comp = cls._bitdiff(secret.seed, other.seed)
                logger.info('Keys are ' + str(key_comp) + '%% different.')
                logger.info('Seeds are ' + str(seed_comp) + '%% different.')
                
            else:
                logger.info('Secret ciphers do not match. Cannot compare.')
            
        except AttributeError:
            logger.error(
                'Attribute error while diffing secrets. Type mismatch? \n'
                '    ' + repr(type(secret)) + '\n'
                '    ' + repr(type(other))
            )
            
    @staticmethod
    def _bitdiff(this_bytes, other_bytes):
        ''' Calculates the percent of different bits between two byte
        strings.
        '''
        if len(this_bytes) == 0 or len(other_bytes) == 0:
            # By returning None, we can explicitly say we couldn't perform the
            # comparison, whilst also preventing math on a comparison.
            return None
        
        # Mask to extract each bit.
        masks = [
            0b00000001,
            0b00000010,
            0b00000100,
            0b00001000,
            0b00010000,
            0b00100000,
            0b01000000,
            0b10000000,
        ]
        
        # Counters for bits.
        diffbits = 0
        totalbits = 0
        
        # First iterate over each byte.
        for this_byte, other_byte in zip(this_bytes, other_bytes):
            
            # Now, using the masks, iterate over each bit.
            for mask in masks:
                # Extract the bit using bitwise AND.
                this_masked = mask & this_byte
                other_masked = mask & other_byte
                
                # Do a bool comparison of the bits, and add any != results to
                # diffbits. Note that 7 + False == 7 and 7 + True == 8
                diffbits += (this_masked != other_masked)
                totalbits += 1
                
        # Finally, calculate a percent difference
        doubdiff = diffbits / totalbits
        return int(doubdiff * 100)
        
    def abandon(self, ghid, quiet=True):
        ''' Remove a secret. If quiet=True, silence any KeyErrors.
        '''
        # Short circuit any tests if quiet is enabled
        if not quiet and ghid not in self._secrets:
            raise UnknownSecret('Secret not found for ' + str(ghid))
            
        self._secrets_persistent.pop(ghid, None)
        self._secrets_local.pop(ghid, None)
        self._secrets_quarantine.pop(ghid, None)
        self._secrets_staging.pop(ghid, None)
        self._secrets_deprecated.pop(ghid, None)
        
    @fixture_noop
    @public_api
    def deprecate(self, ghid, quiet=True):
        ''' Deprecate a secret. It will still be available locally, but
        will be removed from shared nonlocal state.
        '''
        # Short circuit any tests if quiet is enabled
        if not quiet and ghid not in self._secrets:
            raise UnknownSecret('Secret not found for ' + str(ghid))
        
        else:
            secret1 = self._secrets_persistent.pop(ghid, None)
            secret2 = self._secrets_local.pop(ghid, None)
            secret3 = self._secrets_quarantine.pop(ghid, None)
            secret4 = self._secrets_staging.pop(ghid, None)
            secret5 = self._secrets_deprecated.pop(ghid, None)
            
            secret = secret1 or secret2 or secret3 or secret4 or secret5
            
            self._secrets_deprecated[ghid] = secret
        
    def ratchet_chain(self, proxy, current_target, master_secret=None):
        ''' Gets a new secret for the proxy. Returns the secret, and
        flags the ratchet as in-progress.
        
        If master_secret is supplied, we will use the bootstrap ratchet.
        '''
        # Without a master_secret, perform a ratchet with the existing secret
        # for the current target.
        if master_secret is None:
            existing_secret = self.get(current_target)
            ratcheted = self._ratchet(
                secret = existing_secret,
                proxy = proxy,
                salt_ghid = current_target
            )
        
        # With a master_secret, perform a ratchet with the master as a static
        # "seed" secret.
        else:
            ratcheted = self._ratchet(
                secret = master_secret,
                proxy = proxy,
                salt_ghid = current_target
            )
            
        return ratcheted
            
    def heal_chain(self, proxy, target_vector, master_secret=None):
        ''' Heals the ratchet for a binding using the gao. Call this any
        time an agent RECEIVES a new EXTERNAL ratcheted object. Stages
        the resulting secret for the most recent frame in binding, BUT
        DOES NOT RETURN (or commit) IT.
        
        Note that this is necessarily being called before the object is
        available at the oracle. It should, however, be available at
        both the ghidproxy and the librarian.
        
        NOTE that the binding is the LITEWEIGHT version from the
        librarian already, so its ghid is already the dynamic one.
        '''
        if len(target_vector) < 1:
            raise RatchetError('Target vector has no historical references.')
            
        # Master secret ratchets never break, and can alway recover from the
        # most recent frame.
        if master_secret:
            aktueller_index = 1
            
        # Other ratchets, on the other hand, have to start somewhere.
        else:
            # Go from back to front, finding the most recent target vector that
            # exists locally.
            available = [target in self._secrets for target in target_vector]
            # If we didn't find ANY of them, then we're broken.
            if not any(available):
                logger.error(
                    'Broken ratchet. Target vector: ' +
                    str([str(target) for target in target_vector])
                )
                raise RatchetError('Broken ratchet.')
        
            # Get the index for the most recent target that we already know a
            # secret for
            aktueller_index = available.index(True)
        
        # Go from the oldest to the newest, but start after the first one,
        # because we verified that above.
        # We can't use reversed(enumerate(target_vector)) without re-casting
        # target_vector into a dedicated sequence. We'd like to keep the
        # original iterator, so do it manually.
        for ii in range(aktueller_index - 1, -1, -1):
            target = target_vector[ii]
            # Skip the first one (deques don't support slicing)
            # Skip if we already have a secret
            if target in self._secrets:
                continue
            
            # Only do something if we don't know the secret
            else:
                secret = self.ratchet_chain(
                    proxy,
                    current_target = target_vector[ii + 1],
                    master_secret = master_secret
                )
                self.stage(target, secret)
        
    @staticmethod
    def _ratchet(secret, proxy, salt_ghid):
        ''' Ratchets a key using HKDF-SHA512, using the associated
        address as salt. For dynamic files, this should be the previous
        frame ghid (not the dynamic ghid).
        
        Note: this ratchet is bound to a particular dynamic address. The
        ratchet algorithm is:
        
        new_key = HKDF-SHA512(
            IKM = old_secret, (secret IV/nonce | secret key)
            salt = old_frame_ghid, (entire 65 bytes)
            info = dynamic_ghid, (entire 65 bytes)
            new_key_length = len(IV/nonce) + len(key),
            num_keys = 1,
        )
        '''
        cls = type(secret)
        cipher = secret.cipher
        version = secret.version
        len_seed = len(secret.seed)
        len_key = len(secret.key)
        source = bytes(secret.seed + secret.key)
        
        instance = hkdf.HKDF(
            algorithm = hashes.SHA512(),
            length = len_seed + len_key,
            salt = bytes(salt_ghid),
            info = bytes(proxy),
            backend = CRYPTO_BACKEND
        )
        ratcheted = instance.derive(source)
        
        return cls(
            cipher = cipher,
            version = version,
            key = ratcheted[:len_key],
            seed = ratcheted[len_key:]
        )
        
        
class Charon(loopa.TaskLooper):
    ''' Handles the removal of secrets when their targets are removed,
    deleted, or otherwise made inaccessible. Charon instances are
    notified of object removal by the undertaker, and then handle
    ferrying the object's secret into non-existence. Data isn't "dead"
    until its keys are deleted.
    
    It's pretty wasteful to use an entire event loop and thread for this
    but that's sorta the situation at hand currently.
    
    DEPRECATED AND UNUSED. Superceded by undertaker.Ferryman.
    '''
    
    def __init__(self, *args, **kwargs):
        self._privateer = None
        self._death_q = None
        super().__init__(*args, **kwargs)
        
    async def loop_init(self, *args, **kwargs):
        await super().loop_init(*args, **kwargs)
        # For now, just have an arbitrary max cap
        self._death_q = asyncio.Queue(loop=self._loop, maxsize=50)
        
    async def loop_stop(self):
        await super().loop_stop()
        self._death_q = None
        
    def assemble(self, privateer):
        self._privateer = weakref.proxy(privateer)
        
    def schedule(self, obj):
        ''' Gets a secret ready for removal.
        obj is a Hypergolix lightweight representation.
        '''
        if self._death_q is None:
            raise RuntimeError(
                'Cannot schedule secret removal until after loop init.'
            )
        
        # For now, avoid doing this as a coro by using put_nowait
        self._death_q.put_nowait(obj.ghid)
            
    async def loop_run(self, *args, **kwargs):
        ''' Very simply await stuff in the queue and then remove it.
        '''
        dead_ghid = await self._death_q.get()
        self._privateer.abandon(dead_ghid)
