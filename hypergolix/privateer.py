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

# These are used for secret ratcheting only.
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import HKDF

# Intra-package dependencies
from .core import _GAO
from .core import _GAODict

from .utils import TraceLogger

from .exceptions import PrivateerError
from .exceptions import RatchetError


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

        
# ###############################################
# Lib
# ###############################################


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
        self._modlock = threading.Lock()
        
        # These must be linked during assemble.
        self._golcore = None
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
        
    def assemble(self, golix_core, oracle):
        # Chicken, meet egg.
        self._golcore = weakref.proxy(golix_core)
        # We need an oracle for ratcheting.
        self._oracle = weakref.proxy(oracle)
        
    def bootstrap(self):
        ''' Initializes the privateer.
        '''
        # We very obviously need to be able to look up what secrets we have.
        # Lookups: <container ghid>: <container secret>
        self._secrets_persistent = {}
        self._secrets_staging = {}
        self._secrets = collections.ChainMap(
            self._secrets_persistent, 
            self._secrets_staging,
        )
        
        # Keep track of chains (this will need to be distributed)
        # Lookup <proxy ghid>: <container ghid>
        self._chains = {}
        
    def new_secret(self):
        # Straight pass-through to the golix new_secret bit.
        return self._golcore._identity.new_secret()
        
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
            
            # NOTE: this bypasses the usual stage -> commit process, because it
            # can only be used locally during the creation of a new container.
            self._secrets_persistent[container] = secret
        
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
        
    def ratchet_chain(self, proxy):
        ''' Gets a new secret for the proxy. Returns the secret, and 
        flags the ratchet as in-progress.
        '''
        with self._modlock:
            if proxy in self._ratchet_in_progress:
                raise ValueError('Must update chain prior to re-ratcheting.')
                
            if proxy not in self._chains:
                raise ValueError('No chain for that proxy.')
            
            # TODO: make sure this is not a race condition.
            binding = self._oracle.get_object(gaoclass=_GAO, ghid=proxy)
            last_target = binding._history_targets[0]
            last_frame = binding._history[0]
            
            try:
                existing_secret = self._secrets[last_target]
            except KeyError as exc:
                raise RatchetError('No secret for existing target?') from exc
            
            ratcheted = self._ratchet(
                secret = existing_secret,
                proxy = proxy,
                salt_ghid = last_frame
            )
            
            self._ratchet_in_progress[proxy] = ratcheted
            
        return ratcheted
            
    def heal_chain(self, gao, binding):
        ''' Heals the ratchet for a binding using the gao. Call this any 
        time an agent RECEIVES a new EXTERNAL ratcheted object. Stages
        the resulting secret for the most recent frame in binding, BUT
        DOES NOT RETURN (or commit) IT.
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
                    
        
    def get(self, ghid):
        ''' Get a secret for a ghid, regardless of status.
        
        Raises KeyError if secret is not present.
        '''
        try:
            with self._modlock:
                return self._secrets[ghid]
        except KeyError as exc:
            raise KeyError('Secret not found for GHID ' + str(ghid)) from exc
        
    def stage(self, ghid, secret):
        ''' Preliminarily set a secret for a ghid.
        
        If a secret is already staged for that ghid and the ghids are 
        not equal, raises ValueError.
        '''
        with self._modlock:
            if ghid in self._secrets_staging:
                if self._secrets_staging[ghid] != secret:
                    raise ValueError(
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
                raise KeyError(
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
                    raise KeyError(
                        'Secret not currently staged for GHID ' + str(ghid)
                    ) from exc
                else:
                    # It doesn't exist, so commit it directly.
                    self._secrets_persistent[ghid] = secret
                    
                # Just keep track of shit for this fucking error
                self._committment_problems[ghid] = TraceLogger.dump_my_trace()
                    
    def last_commit(self, ghid):
        return self._committment_problems[ghid]
            
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
                raise ValueError(
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
            except KeyError as exc:
                fail_test &= True
                logger.debug('Secret not staged for GHID ' + str(ghid))
            else:
                fail_test = False
                
            try:
                del self._secrets_persistent[ghid]
            except KeyError as exc:
                fail_test &= True
                logger.debug('Secret not stored for GHID ' + str(ghid))
            else:
                fail_test = False
                
        if fail_test:
            raise KeyError('Secret not found for GHID ' + str(ghid))
            
    def __contains__(self, ghid):
        ''' Check if we think we know a secret for the ghid.
        '''
        return ghid in self._secrets