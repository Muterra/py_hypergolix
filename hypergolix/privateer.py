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
import threading
import collections
import weakref
import traceback
import asyncio

from golix import Ghid

# These are used for secret ratcheting only.
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf import hkdf
from cryptography.hazmat.backends import default_backend

# Intra-package dependencies
from .core import _GAO
from .core import _GAODict

from .utils import TraceLogger
from .utils import LooperTrooper
from .utils import call_coroutine_threadsafe

from .exceptions import PrivateerError
from .exceptions import RatchetError
from .exceptions import ConflictingSecrets
from .exceptions import SecretUnknown


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
        # NOTE: I'm not particularly happy about the RLock (vs plain locks),
        # but the process of updating the internal lookups is highly reentrant.
        
        # Modification lock for standard objects
        self._modlock = threading.RLock()
        # Modification lock for bootstrapping objects
        self._bootlock = threading.Lock()
        
        # These must be linked during assemble.
        self._golcore = None
        self._ghidproxy = None
        self._oracle = None
        
        # These must be bootstrapped.
        self._secrets_persistent = None
        self._secrets_quarantine = None
        self._secrets = None
        # These two are local only, but included to make the code more explicit
        self._secrets_staging = None
        self._secrets_local = None
        
        # Keep track of chains. Note that chains need NOT be distributed, as it
        # can always be bootstrapped from the combination of persistent secrets
        # and the dynamic history of the object.
        # Lookup <proxy ghid>: <container ghid>
        self._chains = {}
        # Keep track of chain progress. This does not need to be distributed.
        # Lookup <proxy ghid> -> proxy secret
        self._ratchet_in_progress = {}
            
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
        self._secrets_persistent = _GaoDictBootstrap()
        self._secrets_staging = {}
        self._secrets_local = {}
        self._secrets_quarantine = _GaoDictBootstrap()
        self._secrets = collections.ChainMap(
            self._secrets_persistent,
            self._secrets_local,
            self._secrets_staging,
            self._secrets_quarantine,
        )
        
    def bootstrap(self, persistent, quarantine, credential):
        ''' Initializes the privateer into a distributed state.
        persistent is a GaoDict
        staged is a GaoDict
        chains is a GaoDict
        credential is a bootstrapping credential.
        '''
        # TODO: should this be weakref?
        self._credential = credential
        
        persistent_container = self._ghidproxy.resolve(persistent.ghid)
        quarantine_container = self._ghidproxy.resolve(quarantine.ghid)
        identity_container = self._ghidproxy.resolve(
            self._credential._identity_ghid
        )
        credential_container = self._ghidproxy.resolve(
            self._credential._user_id
        )
        secondary_manifest_container = self._ghidproxy.resolve(
            self._credential._secondary_manifest
        )
        
        # We very obviously need to be able to look up what secrets we have.
        # Lookups: <container ghid>: <container secret>
        self._secrets_persistent = persistent
        self._secrets_staging = {}
        self._secrets_local = {}
        self._secrets_quarantine = quarantine
        self._secrets = collections.ChainMap(
            self._secrets_persistent,
            self._secrets_local,
            self._secrets_staging,
            self._secrets_quarantine,
        )
        
        # Note that we just overwrote any chains that were created when we
        # initially loaded the three above resources. So, we may need to
        # re-create them. But, we can use a fake container address for two of
        # them, since they use a different secrets tracking mechanism.
        self._ensure_bootstrap_chain(
            self._secrets_persistent.ghid,
            persistent_container
        )
        self._ensure_bootstrap_chain(
            self._secrets_quarantine.ghid,
            quarantine_container
        )
        self._ensure_bootstrap_chain(
            self._credential._identity_ghid,
            identity_container
        )
        self._ensure_bootstrap_chain(
            self._credential._user_id,
            credential_container
        )
        self._ensure_bootstrap_chain(
            self._credential._secondary_manifest,
            secondary_manifest_container
        )
            
    def _ensure_bootstrap_chain(self, proxy, container):
        ''' Makes sure that the proxy is in self._chains, and if it's
        missing, adds it.
        '''
        if proxy not in self._chains:
            # Bypass the usual make_chain mechanism, since we don't hold on to
            # the secrets for the bootstrap objects.
            self._chains[proxy] = container
            
    def _is_bootstrap_target(self, ghid):
        # TODO: fix leaky abstraction
        try:
            if (ghid == self._chains[self._secrets_persistent.ghid] or
                ghid == self._chains[self._secrets_quarantine.ghid] or
                ghid == self._chains[self._credential._identity_ghid] or
                ghid == self._chains[self._credential._user_id] or
                ghid == self._chains[self._credential._secondary_manifest]):
                    return True
            else:
                return False
        except KeyError:
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
            raise SecretUnknown('Secret not found for ' + str(ghid)) from exc
        
    def stage(self, ghid, secret):
        ''' Preliminarily set a secret for a ghid.
        
        If a secret is already staged for that ghid and the ghids are
        not equal, raises ConflictingSecrets.
        '''
        with self._modlock:
            self._stage(ghid, secret, self._secrets_staging)
            
    def _ensure_container(self, ghid):
        ''' Make sure the ghid does, in fact, resolve to a container.
        Dependent upon ghidproxy having a clear resolution.
        '''
        # Enforce container-only.
        target = self._ghidproxy.resolve(ghid)
        if ghid != target:
            raise ValueError(
                'Staged ghids must be containers, not proxies: ' + str(ghid)
            )
                
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
        with self._modlock:
            # With quarantine, we can verify it's a container first, since we
            # will only see this when pulling from upstream anyways.
            self._ensure_container(ghid)
            logger.debug(
                'Quarantining secret for ' + str(ghid)
            )
            self._stage(ghid, secret, self._secrets_quarantine)
            
    def unstage(self, ghid):
        ''' Remove a staged secret, probably due to a SecurityError.
        Returns the secret.
        '''
        with self._modlock:
            if ghid in self._secrets_quarantine:
                secret = self._secrets_quarantine.pop(ghid)
            
            elif ghid in self._secrets_staging:
                secret = self._secrets_staging.pop(ghid)
            
            else:
                raise SecretUnknown(
                    'No currently staged secret for GHID ' + str(ghid)
                )
                
        return secret
        
    def commit(self, ghid):
        ''' Store a secret "permanently". The secret must either:
        1. Be staged, XOR
        2. Be quarantined, XOR
        3. Be committed
        
        Other states will raise SecretUnknown (a subclass of KeyError).
        
        Note that this relies upon staging logic enforcing consistency
        of secrets! If that changes to allow the staging conflicting
        secrets, this will need to be modified.
        
        This is transactional and atomic; any errors (ex: ValueError
        above) will return its state to the previous.
        
        BOOTSTRAP CHAINS ARE NEVER (fully) COMMITTED. First of all,
        there's no need to and it's wasteful, because the bootstrap
        chains are deterministically derivable from the credential
        master secret. Second of all, it would unavoidably cause an
        infinitely recursive chain of secret committing. So let's not do
        that!
        
        '''
        with self._modlock:
            # Go ahead and make sure it's a container.
            self._ensure_container(ghid)
            
            # Note that we cannot stage or quarantine a secret that we already
            # have committed. If that secret matches what we already have, it
            # will indempotently and silently exit, without staging.
            if ghid in self._secrets_staging:
                secret = self._secrets_staging.pop(ghid)
                
            elif ghid in self._secrets_quarantine:
                logger.debug(
                    'Removing quarantined secret for ' + str(ghid) + ' prior '
                    'to committment.'
                )
                secret = self._secrets_quarantine.pop(ghid)
                
            # Check all remaining locations to silence SecretUnknown and return
            elif ghid in self._secrets:
                return
                
            else:
                raise SecretUnknown(
                    'Secret not currently staged for GHID ' + str(ghid)
                )
                
            # If it's a bootstrap target, just locally commit it.
            if self._is_bootstrap_target(ghid):
                logger.debug(
                    'Committing bootstrap secret to in-memory storage.'
                )
                self._secrets_local[ghid] = secret
                
            # Nope. Commit globally.
            else:
                logger.debug(
                    'Globally committing secret for ' + str(ghid)
                )
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
        fail_test = not quiet
        
        with self._modlock:
            missing_in_persist = not self._abandon(
                ghid,
                self._secrets_persistent,
                'persistent lookup'
            )
            fail_test &= missing_in_persist
            
            missing_in_local = not self._abandon(
                ghid,
                self._secrets_local,
                'local lookup'
            )
            fail_test &= missing_in_local
            
            missing_in_quaran = not self._abandon(
                ghid,
                self._secrets_quarantine,
                'quarantine lookup'
            )
            fail_test &= missing_in_quaran
            
            missing_in_staging = not self._abandon(
                ghid,
                self._secrets_staging,
                'staging lookup'
            )
            fail_test &= missing_in_staging
                
        if fail_test:
            raise SecretUnknown('Secret not found for ' + str(ghid))
            
    def _abandon(self, ghid, lookup, lookup_name='lookup'):
        ''' Abandons a secret for a ghid in a specified lookup. NOT
        THREADSAFE! Call only from within self.abandon.
        '''
        if ghid in lookup:
            del lookup[ghid]
            found = True
            
        else:
            logger.debug(
                'No secret found in ' + lookup_name + ' for ' + str(ghid) + '.'
            )
            found = False
            
        return found
        
    def has_chain(self, proxy):
        ''' Checks to see if the proxy has a chain.
        '''
        with self._modlock:
            return proxy in self._chains
        
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
            
        if self._credential.is_primary(proxy):
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
        if self._credential.is_primary(proxy):
            with self._bootlock:
                self._update_chain(proxy, container)
                
        else:
            with self._modlock:
                self._update_chain(proxy, container)
            
    def _update_chain(self, proxy, container):
        ''' Raw update chain. NOT THREADSAFE. Should only be called from
        update_chain, protected by either modlock or bootlock.
        '''
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
                logger.debug('No ratchet in progress for ' + str(proxy))
                
            self._chains[proxy] = container
            
    def heal_chain(self, gao, binding):
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
        target = self._ghidproxy.resolve(binding.ghid)
        if target not in self:
            # DON'T take _modlock here or we will be reentrant = deadlock
            try:
                if self._credential.is_primary(gao.ghid):
                    self._heal_bootstrap(gao, binding)
                
                else:
                    self._heal_standard(gao, binding)
                    
            except Exception:
                logger.warning(
                    'Error while staging new secret for attempted ratchet. '
                    'The ratchet is very likely broken.\n' +
                    ''.join(traceback.format_exc())
                )
                
            else:
                # If we have a chain for it, update it! Do this directly,
                # skipping the usual ratcheting process, because we already
                # know everything.
                if gao.ghid in self._chains:
                    self._chains[gao.ghid] = binding.target
            
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
        # Do the try/catch for this in the outside method, so that we can
        # else it.
        self.quarantine(binding.target, known_secret)
        # Note that opening the container itself will commit the secret.
        
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
        # Do the try/catch for this in the outside method, so that we can
        # else it.
        self.stage(binding.target, new_secret)
        # Note that opening the container itself will commit the secret.
        
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
        
        
class Charon(LooperTrooper):
    ''' Handles the removal of secrets when their targets are removed,
    deleted, or otherwise made inaccessible. Charon instances are
    notified of object removal by the undertaker, and then handle
    ferrying the object's secret into non-existence. Data isn't "dead"
    until its keys are deleted.
    
    It's pretty wasteful to use an entire event loop and thread for this
    but that's sorta the situation at hand currently.
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
        call_coroutine_threadsafe(
            coro = self._death_q.put(obj.ghid),
            loop = self._loop
        )
            
    async def loop_run(self, *args, **kwargs):
        ''' Very simply await stuff in the queue, pause for a hot sec,
        and then remove it.
        '''
        dead_ghid = await self._death_q.get()
        await asyncio.sleep(.1)
        self._privateer.abandon(dead_ghid)
