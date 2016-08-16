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

# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'Privateer', 
]


# External dependencies
import threading
import collections
import weakref
import traceback

from golix import Ghid

# These are used for secret ratcheting only.
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import HKDF

# Intra-package dependencies
from .core import _GAO
from .core import _GAODict

from .utils import TraceLogger

from .exceptions import PrivateerError
from .exceptions import RatchetError
from .exceptions import ConflictingSecrets
from .exceptions import SecretUnknown


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

        
# ###############################################
# Lib
# ###############################################


class _GaoDictBootstrap(dict):
    # Just inject a class-level ghid.
    ghid = Ghid.from_bytes(b'\x01' + bytes(64))


class Privateer:
    ''' Lookup system to get secret from ghid. Threadsafe?
    
    Note: a lot of the ratcheting state preservation could POTENTIALLY
    be eliminated (and restored via oracle). But, that should probably
    wait for Golix v2.0, which will change secret ratcheting anyways.
    This will also help us track which secrets use the old ratcheting
    mechanism and which ones use the new one, once that change is 
    started.
    '''
    def __init__(self):
        # Modification lock for standard objects
        self._modlock = threading.Lock()
        # Modification lock for bootstrapping objects
        self._bootlock = threading.Lock()
        
        # These must be linked during assemble.
        self._golcore = None
        self._ghidproxy = None
        self._oracle = None
        
        # These must be bootstrapped.
        self._secrets_persistent = None
        self._secrets_staging = None
        self._secrets = None
        # Keep track of chains (this will need to be distributed)
        # Lookup <proxy ghid>: <container ghid>
        self._chains = None
        
        # Keep track of chain progress. This does not need to be distributed.
        # Lookup <proxy ghid> -> proxy secret
        self._ratchet_in_progress = {}
        
        # Just here for diagnosing a testing problem
        self._committment_problems = {}
            
    def __contains__(self, ghid):
        ''' Check if we think we know a secret for the ghid.
        '''
        return ghid in self._secrets
        
    def assemble(self, golix_core, ghidproxy, oracle):
        # Chicken, meet egg.
        self._golcore = weakref.proxy(golix_core)
        # We need the ghidproxy for bootstrapping, and ratcheting thereof.
        self._ghidproxy = weakref.proxy(ghidproxy)
        # We need an oracle for ratcheting.
        self._oracle = weakref.proxy(oracle)
        
    def prep_bootstrap(self):
        ''' Creates temporary objects for tracking secrets.
        '''
        self._chains = _GaoDictBootstrap()
        self._secrets_persistent = _GaoDictBootstrap()
        self._secrets_staging = _GaoDictBootstrap()
        self._secrets = collections.ChainMap(
            self._secrets_persistent, 
            self._secrets_staging,
        )
        
    def bootstrap(self, persistent, staging, chains, credential):
        ''' Initializes the privateer into a distributed state.
        persistent is a GaoDict
        staged is a GaoDict
        chains is a GaoDict
        credential is a bootstrapping credential.
        '''
        # TODO: should this be weakref?
        self._credential = credential
        
        persists_container = self._ghidproxy.resolve(persistent.ghid)
        staging_container = self._ghidproxy.resolve(staging.ghid)
        chains_container = self._ghidproxy.resolve(chains.ghid)
        # We have to preserve the chain container secret the first time around.
        chains_secret = self.get(chains_container)
        
        # We very obviously need to be able to look up what secrets we have.
        # Lookups: <container ghid>: <container secret>
        self._secrets_persistent = persistent
        self._secrets_staging = staging
        self._secrets = collections.ChainMap(
            self._secrets_persistent, 
            self._secrets_staging,
        )
        
        # Update secrets to include the chains container for initial bootstrap
        if chains_container not in self._secrets:
            self.stage(chains_container, chains_secret)
        
        # Keep track of chains (this will need to be distributed)
        # Lookup <proxy ghid>: <container ghid>
        self._chains = chains
        
        # Note that we just overwrote any chains that were created when we
        # initially loaded the three above resources. So, we may need to 
        # re-create them. But, we can use a fake container address for two of
        # them, since they use a different secrets tracking mechanism.
        self._ensure_bootstrap_chain(
            self._secrets_persistent.ghid, 
            persists_container
        )
        self._ensure_bootstrap_chain(
            self._secrets_staging.ghid, 
            staging_container
        )
        self._ensure_bootstrap_chain(
            self._chains.ghid, 
            chains_container
        )
            
    def _ensure_bootstrap_chain(self, proxy, container):
        ''' Makes sure that the proxy is in self._chains, and if it's 
        missing, adds it.
        '''
        if proxy not in self._chains:
            self.make_chain(proxy, container)
            
    def _is_bootstrap_chain(self, proxy):
        ''' Return True if the proxy belongs to a bootstrapping chain.
        '''
        if (proxy == self._secrets_persistent.ghid or 
            proxy == self._secrets_staging.ghid):
                return True
        else:
            return False
        
    def new_secret(self):
        # Straight pass-through to the golix new_secret bit.
        return self._golcore._identity.new_secret()
        
    def get(self, ghid):
        ''' Get a secret for a ghid, regardless of status.
        
        Raises KeyError if secret is not present.
        '''
        try:
            with self._modlock:
                return self._secrets[ghid]
        except KeyError as exc:
            raise SecretUnknown('Secret not found for ' + repr(ghid)) from exc
        
    def stage(self, ghid, secret):
        ''' Preliminarily set a secret for a ghid.
        
        If a secret is already staged for that ghid and the ghids are 
        not equal, raises ConflictingSecrets.
        '''
        with self._modlock:
            if ghid in self._secrets:
                if self._secrets[ghid] != secret:
                    self._calc_and_log_diff(self._secrets[ghid], secret)
                    raise ConflictingSecrets(
                        'Non-matching secret already staged for GHID ' + 
                        str(ghid)
                    )
            else:
                self._secrets_staging[ghid] = secret
            
    def unstage(self, ghid):
        ''' Remove a staged secret, probably due to a SecurityError.
        Returns the secret.
        '''
        with self._modlock:
            try:
                secret = self._secrets_staging.pop(ghid)
            except KeyError as exc:
                raise SecretUnknown(
                    'No currently staged secret for GHID ' + str(ghid)
                ) from exc
        return secret
        
    def commit(self, ghid):
        ''' Store a secret "permanently". The secret must already be
        staged.
        
        Raises KeyError if ghid is not currently in staging
        
        This is indempotent; if a ghid is currently in staging AND 
        already committed, will compare the two and raise ValueError if
        they don't match.
        
        This is transactional and atomic; any errors (ex: ValueError 
        above) will return its state to the previous.
        '''
        with self._modlock:
            if ghid in self._secrets_persistent:
                self._compare_staged_to_persistent(ghid)
            else:
                try:
                    secret = self._secrets_staging.pop(ghid)
                except KeyError as exc:
                    raise SecretUnknown(
                        'Secret not currently staged for GHID ' + str(ghid)
                    ) from exc
                else:
                    # It doesn't exist, so commit it directly.
                    self._secrets_persistent[ghid] = secret
                    
                # Just keep track of shit for this fucking error
                self._committment_problems[ghid] = TraceLogger.dump_my_trace()
    
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
                    
    def _compare_staged_to_persistent(self, ghid):
        try:
            staged = self._secrets_staging.pop(ghid)
        except KeyError:
            # Nothing is staged. Short-circuit.
            pass
        else:
            if staged != self._secrets_persistent[ghid]:
                # Re-stage, just in case.
                self._secrets_staging[ghid] = staged
                self._calc_and_log_diff(self._secrets_persistent[ghid], staged)
                raise ConflictingSecrets(
                    'Non-matching secret already committed for GHID ' +
                    str(ghid)
                )
        
    def abandon(self, ghid, quiet=True):
        ''' Remove a secret. If quiet=True, silence any KeyErrors.
        '''
        # Short circuit any tests if quiet is enabled
        fail_test = not quiet
        
        with self._modlock:
            try:
                del self._secrets_staging[ghid]
            except KeyError:
                fail_test &= True
                logger.debug('Secret not staged for GHID ' + str(ghid))
            else:
                fail_test = False
                
            try:
                del self._secrets_persistent[ghid]
            except KeyError:
                fail_test &= True
                logger.debug('Secret not stored for GHID ' + str(ghid))
            else:
                fail_test = False
                
        if fail_test:
            raise SecretUnknown('Secret not found for GHID ' + str(ghid))
        
    def make_chain(self, proxy, container):
        ''' Makes a ratchetable chain. Must be owned by a particular
        dynamic address (proxy). Need to know the container address so
        we can ratchet it properly.
        '''
        with self._modlock:
            if proxy in self._chains:
                raise ValueError('Proxy has already been chained.')
                
            if container not in self._secrets:
                raise ValueError(
                    'Cannot chain unless the container secret is known.'
                )
        
            self._chains[proxy] = container
        
    def ratchet_chain(self, proxy):
        ''' Gets a new secret for the proxy. Returns the secret, and 
        flags the ratchet as in-progress.
        '''
        if proxy in self._ratchet_in_progress:
            raise ValueError('Must update chain prior to re-ratcheting.')
            
        if proxy not in self._chains:
            raise ValueError('No chain for that proxy.')
                
        if self._is_bootstrap_chain(self, proxy):
            ratcheted = self._ratchet_bootstrap(proxy)
            
        else:
            ratcheted = self._ratchet_standard(proxy)
            
        self._ratchet_in_progress[proxy] = ratcheted
        return ratcheted
            
    def _ratchet_standard(self, proxy):
        ''' Ratchets a secret used in a non-bootstrapping container.
        '''
        with self._modlock:
            # TODO: make sure this is not a race condition.
            binding = self._oracle.get_object(gaoclass=_GAO, ghid=proxy)
            last_target = binding._history_targets[0]
            last_frame = binding._history[0]
            
            try:
                existing_secret = self._secrets[last_target]
            except KeyError as exc:
                raise RatchetError('No secret for existing target?') from exc
            
            return self._ratchet(
                secret = existing_secret,
                proxy = proxy,
                salt_ghid = last_frame
            )
            
    def _ratchet_bootstrap(self, proxy):
        ''' Ratchets a secret used in a bootstrapping container.
        '''
        with self._bootlock:
            # TODO: make sure this is not a race condition.
            binding = self._oracle.get_object(gaoclass=_GAO, ghid=proxy)
            last_frame = binding._history[0]
            
            master_secret = self._credential.get_master(proxy)
            
            return self._ratchet(
                secret = master_secret,
                proxy = proxy,
                salt_ghid = last_frame
            )
        
    def update_chain(self, proxy, container):
        ''' Updates a chain container address. Must have been ratcheted
        prior to update, and cannot be ratcheted again until updated.
        '''
        with self._modlock:
            if proxy not in self._chains:
                raise ValueError('No chain for proxy.')
                
            if proxy not in self._ratchet_in_progress:
                raise ValueError('No ratchet in progress for proxy.')
                
            secret = self._ratchet_in_progress.pop(proxy)
            self._chains[proxy] = container
            
            # NOTE: this does not bypass the usual stage -> commit process, 
            # even though it can only be used locally during the creation of a 
            # new container, because the container creation still calls commit.
            self._secrets_staging[container] = secret
        
    def reset_chain(self, proxy, container):
        ''' Used to reset a chain back to a pristine state.
        '''
        with self._modlock:
            if proxy not in self._chains:
                raise ValueError('Proxy has no existing chain.')
                
            if container not in self._secrets:
                raise ValueError('Container secret is unknown; cannot reset.')
                
            try:
                del self._ratchet_in_progress[proxy]
            except KeyError:
                pass
                
            self._chains[proxy] = container
            
    def heal_chain(self, gao, binding):
        ''' Heals the ratchet for a binding using the gao. Call this any 
        time an agent RECEIVES a new EXTERNAL ratcheted object. Stages
        the resulting secret for the most recent frame in binding, BUT
        DOES NOT RETURN (or commit) IT.
        '''
        if self._is_bootstrap_chain(gao.ghid):
            self._heal_bootstrap(gao, binding)
        
        else:
            self._heal_standard(gao, binding)
            
    def _heal_standard(self, gao, binding):
        ''' Heals a chain used in a non-bootstrapping container.
        '''
        # Get a local copy of _history_targets, since it's not currently tsafe
        # TODO: make gao tsafe; fix this leaky abstraction; make sure not race
        gao_targets = gao._history_targets.copy()
        gao_history = gao._history.copy()
        
        with self._modlock:
            # This finds the first target for which we have a secret.
            for offset in range(len(gao_targets)):
                if gao_targets[offset] in self._secrets:
                    known_secret = self._secrets[gao_targets[offset]]
                    break
                else:
                    continue
                    
            # If we did not find a target, the ratchet is broken.
            else:
                raise RatchetError(
                    'No available secrets for any of the object\'s known past '
                    'targets. Re-establish the ratchet.'
                )
            
        # Count backwards in index (and therefore forward in time) from the 
        # first new frame to zero (and therefore the current frame).
        # Note that we're using the previous frame's ghid as salt.
        for ii in range(offset, -1, -1):
            known_secret = self._ratchet(
                secret = known_secret,
                proxy = gao.ghid,
                salt_ghid = gao_history[ii]
            )
            
        # DON'T take _modlock here or we will be reentrant = deadlock
        try:
            self.stage(binding.target, known_secret)
            # Note that opening the container itself will commit the secret.
        except:
            logger.warning(
                'Error while staging new secret for attempted ratchet. The '
                'ratchet is very likely broken.\n' + 
                ''.join(traceback.format_exc())
            )
        
    def _heal_bootstrap(self, gao, binding):
        ''' Heals a chain used in a bootstrapping container.
        '''
        with self._bootlock:
            # We don't need to do anything fancy here. Everything is handled 
            # through the credential's master secret and the binding itself.
            master_secret = self._credential.get_master(gao.ghid)
            last_frame = binding._history[0]
            new_secret = self._ratchet(
                secret = master_secret,
                proxy = gao.ghid,
                salt_ghid = last_frame
            )
            
        # DON'T take bootlock or modlock here or we'll be reentrant = deadlock
        try:
            self.stage(binding.target, known_secret)
            # Note that opening the container itself will commit the secret.
        except:
            logger.critical(
                'Error while staging new secret for BOOTSTRAP ratchet. The '
                'ratchet is very likely broken.\n' + 
                ''.join(traceback.format_exc())
            )
        
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
        ratcheted = HKDF(
            master = source,
            salt = bytes(salt_ghid),
            key_len = len_seed + len_key,
            hashmod = SHA512,
            num_keys = 1,
            context = bytes(proxy)
        )
        return cls(
            cipher = cipher,
            version = version,
            key = ratcheted[:len_key],
            seed = ratcheted[len_key:]
        )