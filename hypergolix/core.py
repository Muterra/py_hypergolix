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
    'AgentBase', 
    'EmbeddedMemoryAgent'
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

from .exceptions import NakError
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning
from .exceptions import UnknownParty
from .exceptions import DispatchError
from .exceptions import DispatchWarning

from .persisters import _PersisterBase
from .persisters import MemoryPersister

# from .ipc import _IPCBase
# from .ipc import _EndpointBase


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

        
# ###############################################
# Utilities, etc
# ###############################################


class _Privateer:
    ''' Lookup system to get secret from ghid. Threadsafe.
    '''
    def __init__(self):
        self._modlock = threading.Lock()
        self._secrets_persistent = {}
        self._secrets_staging = {}
        self._secrets = collections.ChainMap(
            self._secrets_persistent, 
            self._secrets_staging,
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
        try:
            with self._modlock:
                secret = self._secrets_staging.pop(ghid)
                if ghid in self._secrets_persistent:
                    if self._secrets_persistent[ghid] != secret:
                        self._secrets_staging[ghid] = secret
                        raise ValueError(
                            'Non-matching secret already committed for GHID ' +
                            str(ghid)
                        )
                else:
                    self._secrets_persistent[ghid] = secret
        except KeyError as exc:
            raise KeyError(
                'Secret not currently staged for GHID ' + str(ghid)
            ) from exc
        
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
                del self._secrets_persistend[ghid]
            except KeyError as exc:
                fail_test &= True
                logger.debug('Secret not stored for GHID ' + str(ghid))
            else:
                fail_test = False
                
        if fail_test:
            raise KeyError('Secret not found for GHID ' + str(ghid))


class _GhidProxier:
    ''' Resolve the base container GHID from any associated ghid. Uses
    all weak references, so should not interfere with GCing objects.
    
    Threadsafe.
    '''
    def __init__(self):
        # The usual.
        self._refs = weakref.WeakKeyDictionary()
        self._modlock = threading.RLock()
        
    def chain(self, proxy, target):
        ''' Set, or update, a ghid proxy.
        
        Ghids must only ever have a single proxy. Calling chain on an 
        existing proxy will update the target.
        '''
        if not isinstance(target, weakref.ProxyTypes):
            target = weakref.proxy(target)
            
        if isinstance(proxy, weakref.ProxyTypes):
            # Well this is a total hack to dereference the proxy, but oh well
            proxy = proxy.__repr__.__self__
        
        with self._modlock:
            self._refs[proxy] = target
        
    def resolve(self, ghid):
        ''' Recursively resolves the container ghid for a proxy (or a 
        container).
        '''
        # Is this seriously fucking necessary?
        if isinstance(ghid, weakref.ProxyTypes):
            # Well this is a total hack to dereference the proxy, but oh well
            ghid = ghid.__repr__.__self__
            
        # Note that _modlock must be reentrant
        with self._modlock:
            try:
                return self.resolve(self._refs[ghid])
            except KeyError:
                return ghid


class AgentBase:
    ''' Base class for all Agents.
    '''
    
    DEFAULT_LEGROOM = 3
    
    def __init__(self, persister, dispatcher, _identity=None, *args, **kwargs):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        
        persister isinstance _PersisterBase
        dispatcher isinstance DispatcherBase
        _identity isinstance golix.FirstParty
        '''
        super().__init__(*args, **kwargs)
        
        if not isinstance(persister, _PersisterBase):
            raise TypeError('Persister must subclass _PersisterBase.')
        self._persister = persister
        
        if not isinstance(dispatcher, DispatcherBase):
            raise TypeError('dispatcher must subclass DispatcherBase.')
        self._dispatcher = dispatcher
        
        self._ghidproxy = _GhidProxier()
        self._privateer = _Privateer()
        
        self._contacts = {}
        # Bindings lookup: {<target ghid>: <binding ghid>}
        self._holdings = {}
        # History lookup for dynamic bindings' frame ghids. 
        # {<dynamic ghid>: <frame deque>}
        # Note that the deque must use a maxlen or it will grow indefinitely.
        self._historian = {}
        # Target lookup for most recent frame in dynamic bindings.
        # {<dynamic ghid>: <most recent target>}
        self._dynamic_targets = {}
        # Lookup for pending requests. {<request address>: <target address>}
        self._pending_requests = {}
        
        # DEPRECATED AND UNUSED?
        # Lookup for shared objects. {<object address>: {<recipients>}}
        self._shared_objects = {}
        
        # # Automatic type checking using max. Can't have smaller than 1.
        # self._legroom = max(1, _legroom)
        
        # In this case, we need to create our own bootstrap.
        if _identity is None:
            self._identity = FirstParty()
            self._persister.publish(self._identity.second_party.packed)
            
        else:
            # Could do type checking here but currently no big deal?
            # This would also be a good spot to make sure our identity already
            # exists at the persister.
            self._identity = _identity
            
        # Now subscribe to my identity at the persister.
        self._persister.subscribe(
            ghid = self._identity.ghid, 
            callback = self._request_listener
        )
        
    @property
    def _legroom(self):
        ''' Get the legroom from our bootstrap. If it hasn't been 
        created yet (aka within __init__), return the class default.
        '''
        return self.DEFAULT_LEGROOM
        
    @property
    def whoami(self):
        ''' Return the Agent's Ghid.
        '''
        return self._identity.ghid
        
    def _retrieve_contact(self, ghid):
        ''' Attempt to retrieve a contact, first from self, then from 
        persister. Raise UnknownParty if failure. Return the 
        contact.
        '''
        if ghid in self._contacts:
            contact = self._contacts[ghid]
        else:
            try:
                contact_packed = self.persister.get(ghid)
                contact = SecondParty.from_packed(contact_packed)
                self._contacts[ghid] = contact
            except NakError as e:
                raise UnknownParty(
                    'Could not find identity in known contacts or at the '
                    'Agent\'s persistence provider.'
                ) from e
        return contact
        
    def _request_listener(self, request_ghid):
        ''' Callback to handle any requests.
        '''
        request_packed = self.persister.get(request_ghid)
        request_unpacked = self._identity.unpack_request(request_packed)
        requestor_ghid = request_unpacked.author
        
        try:
            requestor = self._retrieve_contact(requestor_ghid)
        finally:
            self._do_debind(request_unpacked.ghid)
            
        request = self._identity.receive_request(
            requestor = requestor,
            request = request_unpacked
        )
            
        if isinstance(request, AsymHandshake):
            self._handle_req_handshake(request, request_unpacked.ghid)
            
        elif isinstance(request, AsymAck):
            self._handle_req_ack(request, request_unpacked.ghid)
            
        elif isinstance(request, AsymNak):
            self._handle_req_nak(request, request_unpacked.ghid)
            
        else:
            raise RuntimeError('Encountered and unknown request type.')
            
    def _handle_req_handshake(self, request, source_ghid):
        ''' Handles a handshake request after reception.
        '''
        # First, we need to figure out what the actual container object's
        # address is, and then stage the secret for it.
        real_target = self._deref_ghid(request.target)
        self._privateer.stage(
            ghid = real_target, 
            secret = request.secret
        )
        
        try:
            self.dispatcher.dispatch_handshake(request.target)
            
        except HandshakeError as e:
            # Erfolglos. Send a nak to whomever sent the handshake
            response_obj = self._identity.make_nak(
                target = source_ghid
            )
            self.cleanup_ghid(request.target)
            # print(repr(e))
            # traceback.print_tb(e.__traceback__)
            
        else:
            # Success. Send an ack to whomever sent the handshake
            response_obj = self._identity.make_ack(
                target = source_ghid
            )
            
        response = self._identity.make_request(
            recipient = self._retrieve_contact(request.author),
            request = response_obj
        )
        self.persister.publish(response.packed)
            
    def _handle_req_ack(self, request, source_ghid):
        ''' Handles a handshake ack after reception.
        '''
        target = self._pending_requests[request.target]
        del self._pending_requests[request.target]
        self.dispatcher.dispatch_handshake_ack(request, target)
            
    def _handle_req_nak(self, request, source_ghid):
        ''' Handles a handshake nak after reception.
        '''
        target = self._pending_requests[request.target]
        del self._pending_requests[request.target]
        self.dispatcher.dispatch_handshake_nak(request, target)
        
    def _deref_ghid(self, ghid):
        ''' Recursively walks the ghid to any references. If dynamic,
        stores the reference in proxies.
        '''
        unpacked = self._identity.unpack_any(
            self.persister.get(ghid)
        )
        if isinstance(unpacked, GEOC):
            target = ghid
            
        elif isinstance(unpacked, GOBD):
            # Resolve and store the proxy, then call recursively.
            target = self._identity.receive_bind_dynamic(
                binder = self._retrieve_contact(unpacked.binder),
                binding = unpacked
            )
            
            # Make sure to update historian, too.
            if ghid in self._historian:
                self._historian[ghid].append(unpacked.ghid)
            else:
                self._historian[ghid] = collections.deque(
                iterable = (unpacked.ghid,),
                maxlen = self._legroom
            )
            
            self._ghidproxy.chain(
                proxy = ghid,
                target = target,
            )
            target = self._deref_ghid(target)
            
        else:
            raise RuntimeError(
                'Ghid did not resolve to a dynamic binding or container.'
            )
            
        return target
        
    @property
    def persister(self):
        return self._persister
        
    @property
    def dispatcher(self):
        return self._dispatcher
        
    def _make_static(self, data, secret=None):
        if secret is None:
            secret = self._identity.new_secret()
        container = self._identity.make_container(
            secret = secret,
            plaintext = data
        )
        self._privateer.stage(container.ghid, secret)
        self._privateer.commit(container.ghid)
        return container
        
    def _make_bind(self, ghid):
        binding = self._identity.make_bind_static(
            target = ghid
        )
        self._holdings[ghid] = binding.ghid
        return binding
        
    def _do_debind(self, ghid):
        ''' Creates a debinding and removes the object from persister.
        '''
        debind = self._identity.make_debind(
            target = ghid
        )
        self.persister.publish(debind.packed)
        
    def new_static(self, state):
        ''' Makes a new static object, handling binding, persistence, 
        and so on. Returns container ghid.
        '''
        container = self._make_static(state)
        binding = self._make_bind(container.ghid)
        # This would be a good spot to figure out a way to make use of
        # publish_unsafe.
        # Note that if these raise exceptions and we catch them, we'll
        # have an incorrect state in self._holdings
        self.persister.publish(binding.packed)
        self.persister.publish(container.packed)
        return container.ghid
        
    def new_dynamic(self, state, _legroom=None):
        ''' Makes a dynamic object. May link to a static (or dynamic) 
        object's address. state must be either Ghid or bytes-like.
        
        The _legroom argument determines how many frames should be used 
        as history in the dynamic binding. If unused, sources from 
        self._legroom.
        '''
        if _legroom is None:
            _legroom = self._legroom
        
        dynamic = self._do_dynamic(state)
            
        # Historian manages the history definition for the object.
        self._historian[dynamic.ghid_dynamic] = collections.deque(
            iterable = (dynamic.ghid,),
            maxlen = _legroom
        )
        # Add a note to _holdings that "I am my own keeper"
        self._holdings[dynamic.ghid_dynamic] = dynamic.ghid_dynamic
        
        return dynamic.ghid_dynamic
        
    def update_dynamic(self, ghid_dynamic, state):
        ''' Like update_object, but does not perform type checking, and
        presumes a correctly formatted state. Not generally recommended
        for outside use.
        '''
        if ghid_dynamic not in self._historian:
            raise ValueError(
                'The Agent could not find a record of the object\'s history. '
                'Agents cannot update objects they did not create.'
            )
            
        frame_history = self._historian[ghid_dynamic]
        
        old_tail = frame_history[len(frame_history) - 1]
        old_frame = frame_history[0]
        
        # TODO CRITICAL SECURITY ISSUE:
        # First we need to decide if we're going to ratchet the secret, or 
        # create a new one. The latter will require updating anyone who we've
        # shared it with. Ratcheting is only available if the last target was
        # directly referenced.
        # But, keep in mind that if the Golix spec ever changes, we could 
        # potentially create two separate top-level refs to containers. So in
        # that case, we would need to implement some kind of ownership of the
        # secret by a particular dynamic binding.
        # TEMPORARY FIX: Don't support nesting dynamic bindings. Document that 
        # downstream stuff cannot reuse links in dynamic bindings (or prevent
        # their use entirely).
        # Note: currently in Hypergolix, links in dynamic objects aren't yet
        # fully supported at all, and certainly aren't documented, so they 
        # shouldn't yet be considered a public part of the api.
        
        current_container = self._ghidproxy.resolve(ghid_dynamic)
        dynamic = self._do_dynamic(
            state = state, 
            ghid_dynamic = ghid_dynamic,
            history = frame_history,
            secret = self._ratchet_secret(
                secret = self._privateer.get(current_container),
                ghid = old_frame
            )
        )
            
        # Update the various buffers and do the object's callbacks
        frame_history.appendleft(dynamic.ghid)
        
        # Note: this should all move to dispatch, which maintains the ownership
        # records for ghids.
        # # Clear out old key material
        # if old_tail not in frame_history:
        #     # So this should actually be handled via the weakvaluedict -- we
        #     # should be automatically clearing out anything that isn't the most
        #     # recent key for the binding.
        #     if old_tail in self._secrets:
        #         warnings.warn(RuntimeWarning(
        #             'Tail secret was inappropriately retained.'
        #         ))
        #     # self._del_secret(old_tail)
        
        # # Since we're not doing a ratchet right now, just do this every time.
        # if obj.address in self._shared_objects:
        #     for recipient in self._shared_objects[obj.address]:
        #         self.hand_object(obj, recipient)
        
    def _do_dynamic(self, state, ghid_dynamic=None, history=None, secret=None):
        ''' Actually generate a dynamic binding.
        '''
        if isinstance(state, Ghid):
            target = state
            container = None
            
        else:
            container = self._make_static(state, secret)
            target = container.ghid
            
        dynamic = self._identity.make_bind_dynamic(
            target = target,
            ghid_dynamic = ghid_dynamic,
            history = history
        )
        
        # Don't forget to update our proxy!
        self._ghidproxy.chain(
            proxy = dynamic.ghid_dynamic,
            target = target,
        )
        
        # Note: the dispatcher worries about ghid lifetimes (and therefore 
        # also secret lifetimes).
            
        self.persister.publish(dynamic.packed)
        if container is not None:
            self.persister.publish(container.packed)
            
        self._dynamic_targets[dynamic.ghid_dynamic] = target
        
        return dynamic
        
    def sync_dynamic(self, ghid):
        ''' Checks self.persister for a new dynamic frame for ghid, and 
        if one exists, gets its state and calls an update on obj.
        
        Should probably modify this to also check state vs current.
        
        TODO: throw out this whole fucking mess, because it's redundant
        with get_ghid, and shitty, etc etc.
        '''
        # Is this doing what I think it's doing?
        frame_ghid = ghid
        unpacked_binding = self._identity.unpack_bind_dynamic(
            self.persister.get(frame_ghid)
        )
        ghid = unpacked_binding.ghid_dynamic
        # Goddammit. It is. Why the hell are we passing frame_ghid as ghid?!
        
        # First, make sure we're not being asked to update something we 
        # initiated the update for.
        if ghid not in self._ignore_subs_because_updating:
            # This bit trusts that the persistence provider is enforcing proper
            # monotonic state progression for dynamic bindings. Check that our
            # current frame is the most recent, and if so, return immediately.
            last_known_frame = self._historian[ghid][0]
            if unpacked_binding.ghid == last_known_frame:
                return None
                
            # Get our CURRENT secret (before dereffing)
            secret = self._privateer.get(self._ghidproxy.resolve(ghid))
            # Get our FUTURE target
            target = self._deref_ghid(ghid)
            offset = unpacked_binding.history.index(last_known_frame)
            
            # Count backwards in index (and therefore forward in time) from the 
            # first new frame to zero (and therefore the current frame).
            # Note that we're using the previous frame's ghid as salt.
            # This attempts to heal any broken ratchet.
            for ii in range(offset, -1, -1):
                secret = self._ratchet_secret(
                    secret = secret,
                    ghid = unpacked_binding.history[ii]
                )
                
            # Stage our FUTURE secret
            self._privateer.stage(target, secret)
            
            # Dispatcher handles secret lifetimes, so don't worry about that
            # here.
            self._historian[ghid].appendleft(unpacked_binding.ghid)
            
            author, is_dynamic, state = self.get_ghid(target)
            return ghid, state
            
        else:
            return None
        
    def freeze_dynamic(self, ghid_dynamic):
        ''' Creates a static binding for the most current state of a 
        dynamic binding. Returns the static container's ghid.
        '''
        if not isinstance(ghid_dynamic, Ghid):
            raise TypeError(
                'Must pass a dynamic ghid to freeze_dynamic.'
            )
            
        # frame_ghid = self._historian[obj.address][0]
        target = self._dynamic_targets[ghid_dynamic]
        self.hold_ghid(target)
        
        # Return the ghid that was just held.
        return target
        
    def hold_ghid(self, ghid):
        ''' Prevents the deletion of a Ghid by binding to it.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError(
                'Only ghids may be held.'
            )
        # There's not really a reason to create a binding for something you
        # already created, whether static or dynamic, but there's also not
        # really a reason to check if that's the case.
        binding = self._make_bind(ghid)
        self.persister.publish(binding.packed)
        self._holdings[ghid] = binding.ghid
        
        # Note that this is no longer needed, since secret retention is handled
        # exclusively by the dispatcher.
        # # Also make sure the secret is persistent.
        # self._set_secret(ghid, self._get_secret(ghid))
        
    def delete_ghid(self, ghid):
        ''' Removes an object identified by ghid (if possible). May 
        produce a warning if the persistence provider cannot remove the 
        object due to another conflicting binding.
        ''' 
        if ghid not in self._holdings:
            raise ValueError(
                'Agents cannot attempt to delete objects they have not held. '
                'This may also indicate that the object has already been '
                'deleted.'
            )
            
        binding_ghid = self._holdings[ghid]
        self._do_debind(binding_ghid)
        del self._holdings[ghid]
        
        return True
        
    def hand_ghid(self, target, recipient):
        ''' Initiates a handshake request with the recipient to share 
        the ghid.
        
        This approach may be perhaps somewhat suboptimal for dynamic 
        objects.
        '''
        if not isinstance(target, Ghid):
            raise TypeError(
                'target must be Ghid or similar.'
            )
        if not isinstance(recipient, Ghid):
            raise TypeError(
                'recipient must be Ghid or similar.'
            )
            
        contact = self._retrieve_contact(recipient)
        # TODO: make sure that the ghidproxy has already resolved the container
        container_ghid = self._ghidproxy.resolve(target)
        
        handshake = self._identity.make_handshake(
            target = target,
            secret = self._privateer.get(container_ghid)
        )
        
        request = self._identity.make_request(
            recipient = contact,
            request = handshake
        )
        
        # Note the potential race condition here. Should catch errors with the
        # persister in case we need to resolve pending requests that didn't
        # successfully post.
        self._pending_requests[request.ghid] = target
        self.persister.publish(request.packed)
        
    def get_ghid(self, ghid):
        ''' Gets a new local copy of the object, assuming the Agent has 
        access to it, as identified by ghid. Does not automatically 
        create an object.
        
        Note that for dynamic objects, initial retrieval DOES NOT parse
        any history. It begins with the target and goes from there.
        
        returns the object's state (not necessarily its plaintext) if
        successful.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('Passed ghid must be a Ghid or similar.')
            
        # First assume we've already dereferenced it.
        target = self._ghidproxy.resolve(ghid)
        packed = self.persister.get(target)
        unpacked = self._identity.unpack_any(packed=packed)
        # TODO: add caching of unpacked objects.
        
        if isinstance(unpacked, GEOC):
            # Do nothing; just hold on a second.
            pass
            
        # Evidently our assumption was wrong, and it hasn't yet been derefed.
        elif isinstance(unpacked, GOBD):
            target = self._deref_ghid(target)
            packed = self.persister.get(target)
            
            unpacked = self._identity.unpack_container(packed)
            
        else:
            raise ValueError(
                'Ghid resolves to an invalid Golix object. get_ghid must '
                'target either a container (GEOC) or a dynamic binding (GOBD).'
            )
            
        try:
            author, state = self._open_container(unpacked)
            
        except SecurityError:
            self._privateer.abandon(target)
            raise
            
        else:
            self._privateer.commit(target)
            
        # If the real target and the passed ghid differ, this must be a dynamic
        is_dynamic = (target != ghid)
            
        # This is gross, not sure I like it.
        return author, is_dynamic, state
        
    def _open_container(self, unpacked):
        ''' Takes an unpacked GEOC and converts it to author and state.
        '''
        author = unpacked.author
        contact = self._retrieve_contact(author)
        state = self._identity.receive_container(
            author = contact,
            secret = self._privateer.get(unpacked.ghid),
            container = unpacked
        )
        return author, state
        
    def cleanup_ghid(self, ghid):
        ''' Does anything needed to clean up the object with address 
        ghid: removing secrets, unsubscribing from dynamic updates, etc.
        '''
        pass
        
    @staticmethod
    def _ratchet_secret(secret, ghid):
        ''' Ratchets a key using HKDF-SHA512, using the associated 
        address as salt. For dynamic files, this should be the previous
        frame ghid (not the dynamic ghid).
        '''
        cls = type(secret)
        cipher = secret.cipher
        version = secret.version
        len_seed = len(secret.seed)
        len_key = len(secret.key)
        source = bytes(secret.seed + secret.key)
        ratcheted = HKDF(
            master = source,
            salt = bytes(ghid),
            key_len = len_seed + len_key,
            hashmod = SHA512,
            num_keys = 1
        )
        return cls(
            cipher = cipher,
            version = version,
            key = ratcheted[:len_key],
            seed = ratcheted[len_key:]
        )
        
        
class DispatcherBase(metaclass=abc.ABCMeta):
    ''' Base class for dispatchers. Dispatchers handle objects; they 
    translate between raw Golix payloads and application objects, as 
    well as shepherding objects appropriately to/from/between different
    applications. Dispatchers are intended to be combined with agents,
    and vice versa.
    '''
    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
        
    @abc.abstractmethod
    def dispatch_handshake(self, target):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        pass
        
    @abc.abstractmethod
    def dispatch_handshake_ack(self, ack, target):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        pass
    
    @abc.abstractmethod
    def dispatch_handshake_nak(self, nak, target):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        pass
        
        
class _TestDispatcher(DispatcherBase):
    ''' An dispatcher that ignores all dispatching for test purposes.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._orphan_shares_pending = []
        self._orphan_shares_incoming = []
        self._orphan_shares_outgoing = []
        self._orphan_shares_failed = []
        
    def dispatch_handshake(self, target):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        self._orphan_shares_incoming.append(target)
        
    def dispatch_handshake_ack(self, ack, target):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        self._orphan_shares_outgoing.append(ack)
    
    def dispatch_handshake_nak(self, nak, target):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        self._orphan_shares_failed.append(nak)
        
    def retrieve_recent_handshake(self):
        return self._orphan_shares_incoming.pop()
        
    def retrieve_recent_ack(self):
        return self._orphan_shares_outgoing.pop()
        
    def retrieve_recent_nak(self):
        return self._orphan_shares_failed.pop()


class Dispatcher(DispatcherBase):
    ''' A standard, working dispatcher.
    
    Objects are dispatched to endpoints based on two criteria:
    
    1. API identifiers, or
    2. Application tokens.
    
    Application tokens should never be shared, as doing so may allow for
    local phishing. The token is meant to create a private and unique 
    identifier for that application; it is specific to that particular
    agent. Objects being dispatched by token will only be usable by that
    application at that agent.
    
    API identifiers (and objects dispatched by them), however, may be 
    shared as desired. They are not necessarily specific to a particular
    application; when sharing, the originating application has no 
    guarantees that the receiving application will be the same. They 
    serve only to enforce data interoperability.
    
    Ideally, the actual agent will eventually be capable of configuring
    which applications should have access to which APIs, but currently
    anything installed is considered trusted.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Lookup for app_tokens -> endpoints. Will be specific to the current
        # state of this particular client for this agent.
        self._active_tokens = weakref.WeakValueDictionary()
        # Defining b'\x00\x00\x00\x00' will prevent using it as a token.
        self._active_tokens[b'\x00\x00\x00\x00'] = self
        # Set of all known tokens. Add b'\x00\x00\x00\x00' to prevent its use.
        # Should be made persistent across all clients for any given agent.
        self._known_tokens = set()
        self._known_tokens.add(b'\x00\x00\x00\x00')
        
        # Lookup for api_ids -> app_tokens. Should this contain ONLY the apps 
        # that are currently available? That might encourage ephemeral API 
        # subscription, which could be good or bad.
        self._api_ids = _JitSetDict()
        
        # Lookup for handshake ghid -> handshake object
        self._outstanding_handshakes = {}
        # Lookup for handshake ghid -> app_token, recipient
        self._outstanding_shares = {}
        
        # Lookup for ghid -> token (meaningful for app-private objs only)
        self._token_by_ghid = {}
        # Lookup for ghid -> api_id
        self._api_by_ghid = {}
        # Lookup for ghid -> state (either bytes-like or Ghid)
        self._state_by_ghid = {}
        # Lookup for ghid -> author
        self._author_by_ghid = {}
        # Lookup for ghid -> dynamic
        self._dynamic_by_ghid = {}
        
        # Lookup for ghid -> tokens that specifically requested the ghid
        self._requestors_by_ghid = _JitSetDict()
        self._discarders_by_ghid = _JitSetDict()
        
        self._orphan_shares_incoming = set()
        self._orphan_shares_outgoing_success = []
        self._orphan_shares_outgoing_failed = []
        
        # Lookup for token -> waiting ghid -> operations
        self._pending_by_token = _JitDictDict()
        
        # Very, very quick hack to ignore updates from persisters when we're 
        # the ones who initiated the update. Simple set of ghids.
        self._ignore_subs_because_updating = set()
        
    def register_object(self, address, author, state, api_id, app_token, dynamic):
        ''' Sets all applicable tracking dict entries.
        '''
        # This is redundant with new_object, but oh well.
        api_id, app_token = self._normalize_api_and_token(api_id, app_token)
        
        self._dynamic_by_ghid[address] = dynamic
        self._state_by_ghid[address] = state
        self._author_by_ghid[address] = author
        self._token_by_ghid[address] = app_token
        self._api_by_ghid[address] = api_id
        
        if dynamic:
            self.persister.subscribe(address, self.dispatch_update)
            
    def deregister_object(self, address):
        ''' Removes all applicable tracking, state management, etc.
        '''
        if self._dynamic_by_ghid[address]:
            self.persister.unsubscribe(address, self.dispatch_update)
        
        del self._dynamic_by_ghid[address]
        del self._state_by_ghid[address]
        del self._author_by_ghid[address]
        del self._token_by_ghid[address]
        del self._api_by_ghid[address]
        
        # Todo: what about requestors_by_ghid and discarders_by_ghid?
        
    def _pack_dispatchable(self, state, api_id, app_token):
        ''' Packs the object state, its api_id, and its app_token into a
        format that can be read by a fellow dispatcher.
        '''
        version = b'\x00'
        return b'hgxd' + version + api_id + app_token + state
        
    def _unpack_dispatchable(self, packed):
        ''' Packs the object state, its api_id, and its app_token into a
        format that can be read by a fellow dispatcher.
        '''
        magic = packed[0:4]
        version = packed[4:5]
        
        if magic != b'hgxd':
            raise DispatchError('Object does not appear to be dispatchable.')
        if version != b'\x00':
            raise DispatchError('Incompatible dispatchable version number.')
            
        api_id = packed[5:70]
        app_token = packed[70:74]
        state = packed[74:]
        
        return state, api_id, app_token
            
    def _normalize_api_and_token(self, api_id, app_token):
        ''' Converts app_token and api_id into appropriate values from 
        what may or may not be None.
        '''
        undefined = (app_token is None and api_id is None)
        
        if undefined:
            raise DispatchError(
                'Cannot leave both app_token and api_id undefined.'
            )
            
        if app_token is None:
            app_token = bytes(4)
        else:
            # Todo: "type" check app_token.
            pass
            
        if api_id is None:
            # Todo: "type" check api_id.
            api_id = bytes(65)
        else:
            pass
            
        return api_id, app_token
        
    def _get_object(self, ghid):
        ''' Gets an object, but doesn't do any tracking based on the 
        requestor. Also doesn't check to see if the object is private or
        not.
        '''
        if ghid in self._state_by_ghid:
            state = self._state_by_ghid[ghid]
            api_id = self._api_by_ghid[ghid]
            app_token = self._token_by_ghid[ghid]
            author = self._author_by_ghid[ghid]
            dynamic = self._dynamic_by_ghid[ghid]
            
        else:
            author, dynamic, wrapped_state = self.get_ghid(ghid)
            state, api_id, app_token = self._unpack_dispatchable(wrapped_state)
            self.register_object(ghid, author, state, api_id, app_token, dynamic)
        
        return author, state, api_id, app_token, dynamic
        
    def get_object(self, asking_token, ghid):
        ''' Gets an object by ghid for a specific endpoint. Currently 
        only works for non-private objects.
        '''
        author, state, api_id, app_token, dynamic = self._get_object(ghid)
        
        if app_token != bytes(4) and app_token != asking_token:
            raise DispatchError(
                'Attempted to load private object from different application.'
            )
        
        self._requestors_by_ghid[ghid].add(asking_token)
        self._discarders_by_ghid[ghid].discard(asking_token)
        
        return author, state, api_id, app_token, dynamic
        
    def new_object(self, asking_token, state, api_id, app_token, dynamic, _legroom=None):
        ''' Creates a new object with the upstream golix provider.
        '''
        # Restore a default legroom.
        if _legroom is None:
            _legroom = self._legroom
            
        # Make sure api_id & app_token are valid, and then wrap them up for use
        api_id, app_token = self._normalize_api_and_token(api_id, app_token)
        wrapped_state = self._pack_dispatchable(state, api_id, app_token)
            
        if dynamic:
            # Normalize dynamic definition to bool
            dynamic = True
            
            if isinstance(state, Ghid):
                address = self.new_dynamic(state=state)
            else:
                address = self.new_dynamic(state=wrapped_state)
                
        else:
            # Normalize dynamic definition to bool and then make a static obj
            dynamic = False
            address = self.new_static(state=wrapped_state)
            
        self.register_object(address, self.whoami, state, api_id, app_token, dynamic)
        
        # Note: should we add some kind of mechanism to defer passing to other 
        # endpoints until we update the one that actually requested the obj?
        self._distribute_to_endpoints(
            ghid = address, 
            skip_token = asking_token
        )
        
        return address
        
    def update_object(self, asking_token, ghid, state):
        ''' Initiates an update of an object. Must be tied to a specific
        endpoint, to prevent issuing that endpoint a notification in 
        return.
        '''
        try:
            if not self._dynamic_by_ghid[ghid]:
                raise DispatchError(
                    'Object is not dynamic. Cannot update.'
                )
        except KeyError as e:
            raise DispatchError(
                'Object unknown to dispatcher. Call get_object on address to '
                'sync it before updating.'
            ) from e
        
        
        # So now we know it's a dynamic object and that we know of it.
        api_id = self._api_by_ghid[ghid]
        app_token = self._token_by_ghid[ghid]
        wrapped_state = self._pack_dispatchable(state, api_id, app_token)
        
        # Try updating golix before local.
        # Temporarily silence updates from persister about the ghid we're 
        # in the process of updating
        try:
            self._ignore_subs_because_updating.add(ghid)
            self.update_dynamic(ghid, wrapped_state)
        finally:
            self._ignore_subs_because_updating.remove(ghid)
        # Successful, so propagate local
        self._state_by_ghid[ghid] = state
        # Finally, tell everyone what's up.
        self._distribute_to_endpoints(ghid, skip_token=asking_token)
        
    def share_object(self, asking_token, ghid, recipient):
        ''' Do the whole super thing, and then record which application
        initiated the request, and who the recipient was.
        '''
        # First make sure we actually know the object
        try:
            if self._token_by_ghid[ghid] != bytes(4):
                raise DispatchError('Cannot share a private object.')
        except KeyError as e:
            raise DispatchError('Attempt to share an unknown object.') from e
        
        try:
            self._outstanding_shares[ghid] = (
                asking_token, 
                recipient
            )
            
            # Currently, just perform a handshake. In the future, move this 
            # to a dedicated exchange system.
            self.hand_ghid(ghid, recipient)
            
        except:
            del self._outstanding_shares[ghid]
            raise
        
    def freeze_object(self, asking_token, ghid):
        ''' Converts a dynamic object to a static object, returning the
        static ghid.
        '''
        try:
            if not self._dynamic_by_ghid[ghid]:
                raise DispatchError('Cannot freeze a static object.')
        except KeyError as e:
            raise DispatchError(
                'Object unknown to dispatch; cannot freeze. Call get_object.'
            ) from e
            
        author, state, api_id, app_token, dynamic = self._get_object(ghid)
        
        address = self.freeze_dynamic(
            ghid_dynamic = ghid
        )
        
        self.register_object(
            address, 
            author, 
            state, 
            api_id, 
            app_token, 
            dynamic = False
        )
        
        return address
        
    def hold_object(self, asking_token, ghid):
        ''' Binds to an address, preventing its deletion. Note that this
        will publicly identify you as associated with the address and
        preventing its deletion to any connected persistence providers.
        '''
        if ghid not in self._state_by_ghid:
            raise DispatchError(
                'Object unknown to dispatch; cannot hold. Call get_object.'
            )
            
        self.hold_ghid(ghid)
        
    def delete_object(self, asking_token, ghid):
        ''' Debinds an object, attempting to delete it. This operation
        will succeed if the persistence provider accepts the deletion,
        but that doesn't necessarily mean the object was removed. A
        warning may be issued if the object was successfully debound, 
        but other bindings are preventing its removal.
        
        NOTE THAT THIS DELETES ALL COPIES OF THE OBJECT! It will become
        subsequently unavailable to other applications using it.
        '''
        if ghid not in self._state_by_ghid:
            raise DispatchError(
                'Object unknown to dispatch; cannot delete. Call get_object.'
            )
            
        try:
            self._ignore_subs_because_updating.add(ghid)
            self.delete_ghid(ghid)
        finally:
            self._ignore_subs_because_updating.remove(ghid)
        
        # Todo: check to see if this actually results in deleting the object
        # upstream.
        
        # There's only a race condition here if the object wasn't actually 
        # removed upstream.
        self._distribute_to_endpoints(
            ghid, 
            skip_token = asking_token, 
            deleted = True
        )
        self.deregister_object(ghid)
        
    def discard_object(self, asking_token, ghid):
        ''' Removes the object from *only the asking application*. The
        asking_token will no longer receive updates about the object.
        However, the object itself will persist, and remain available to
        other applications. This is the preferred way to remove objects.
        '''
        # This is sorta an accidental check that we're actually tracking the
        # object. Could make it explicit I suppose.
        api_id = self._api_by_ghid[ghid]
        
        # Completely discard/deregister anything we don't care about anymore.
        interested_tokens = set()
        
        if self._token_by_ghid[ghid] == bytes(4):
            interested_tokens.update(self._api_ids[api_id])
        else:
            interested_tokens.add(self._token_by_ghid[ghid])
            
        interested_tokens.update(self._requestors_by_ghid[ghid])
        interested_tokens.difference_update(self._discarders_by_ghid[ghid])
        interested_tokens.discard(asking_token)
        
        # Now perform actual updates
        if len(interested_tokens) == 0:
            self.deregister_object(ghid)
        else:
            self._requestors_by_ghid[ghid].discard(asking_token)
            self._discarders_by_ghid[ghid].add(asking_token)
    
    def dispatch_share(self, ghid):
        ''' Dispatches shares that were not created via handshake.
        '''
        raise NotImplementedError('Cannot yet share without handshakes.')
        
    def dispatch_handshake(self, target):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        # Well first we need to actually get the object, so it's available to
        # distribute.
        author, state, api_id, app_token, dynamic = self._get_object(target)
        
        # Go ahead and distribute it to the appropriate endpoints.
        self._distribute_to_endpoints(target)
        
        # Note: we should add something in here to catch issues if we can't
        # distribute to endpoints or something, so that the handshake doesn't
        # get stuck in limbo.
        
        # Note that unless we raise a HandshakeError RIGHT NOW, we'll be
        # sending an ack to the handshake, just to indicate successful receipt 
        # of the share. If the originating app wants to check for availability, 
        # well, that's currently on them. In the future, add handle for that in
        # SHARE instead of HANDSHAKE? <-- probably good idea
    
    def dispatch_handshake_ack(self, ack, target):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        # This was added in our overriden share_object
        app_token, recipient = self._outstanding_shares[target]
        del self._outstanding_shares[target]
        # Now notify just the requesting app of the successful share. Note that
        # this will also handle any missing endpoints.
        self._attempt_contact_endpoint(
            app_token, 
            'notify_share_success',
            target, 
            recipient = recipient
        )
    
    def dispatch_handshake_nak(self, nak, target):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        app_token, ghid, recipient = self._outstanding_shares[target]
        del self._outstanding_shares[target]
        # Now notify just the requesting app of the failed share. Note that
        # this will also handle any missing endpoints.
        self._attempt_contact_endpoint(
            app_token, 
            'notify_share_failure',
            target, 
            recipient = recipient
        )
                    
    def dispatch_update(self, ghid):
        ''' Updates local state tracking and distributes the update to 
        the endpoints.
        '''
        # NOTE THAT GHID HERE IS THE FRAME GHID, NOT THE DYNAMIC GHID!
        # Todo: change that, because holy shit.
        # Okay, now do sync and stuff
        result = self.sync_dynamic(ghid)
        # That will return None if no update was found
        if result is not None:
            ghid_dynamic, state = result
            
            # if ghid2 != ghid:
            #     raise RuntimeError(
            #         'Error while unpacking incoming update: mismatched ghids'
            #     )
            
            state, api_id, app_token = self._unpack_dispatchable(state)
            
            # Explicitly catch empty API_ID to avoid problems with private objs
            if api_id != bytes(65) and api_id != self._api_by_ghid[ghid_dynamic]:
                raise RuntimeError(
                    'Error while unpacking incoming update: mismatched api_ids'
                )
            
            self._state_by_ghid[ghid_dynamic] = state
            self._distribute_to_endpoints(ghid_dynamic)
                    
    def _distribute_to_endpoints(self, ghid, skip_token=None, deleted=False):
        ''' Passes the object to all endpoints supporting its api via 
        command.
        
        If tokens to skip is defined, they will be skipped.
        tokens_to_skip isinstance iter(app_tokens)
        
        Should suppressing notifications for original creator be handled
        by the endpoint instead?
        '''
        # Create a temporary set
        callsheet = set()
            
        # The app token is defined, so contact that endpoint (and only that 
        # endpoint) directly
        if self._token_by_ghid[ghid] != bytes(4):
            callsheet.add(self._token_by_ghid[ghid])
            
        # It's not defined, so get everyone that uses that api_id
        else:
            api_id = self._api_by_ghid[ghid]
            callsheet.update(self._api_ids[api_id])
            
        # Now add anyone explicitly tracking that object
        callsheet.update(self._requestors_by_ghid[ghid])
        
        # And finally, remove the skip token if present, as well as any apps
        # that have discarded the object
        callsheet.discard(skip_token)
        callsheet.difference_update(self._discarders_by_ghid[ghid])
            
        if len(callsheet) == 0:
            warnings.warn(HandshakeWarning(
                'Agent lacks application to handle app id.'
            ))
            self._orphan_shares_incoming.add(ghid)
        else:
            for token in callsheet:
                if deleted:
                    # It's mildly dangerous to do this -- what if we throw an 
                    # error in _attempt_contact_endpoint?
                    self._attempt_contact_endpoint(
                        token, 
                        'send_delete', 
                        ghid
                    )
                else:
                    # It's mildly dangerous to do this -- what if we throw an 
                    # error in _attempt_contact_endpoint?
                    self._attempt_contact_endpoint(
                        token, 
                        'notify_object', 
                        ghid, 
                        state = self._state_by_ghid[ghid]
                )
                
    def _attempt_contact_endpoint(self, app_token, command, ghid, *args, **kwargs):
        ''' We have a token defined for the api_id, but we don't know if
        the application is locally installed and running. Try to use it,
        and if we can't, warn and stash the object.
        '''
        if app_token not in self._known_tokens:
            raise DispatchError(
                'Agent lacks application with matching token. WARNING: object '
                'may have been discarded as a result!'
            )
        
        elif app_token not in self._active_tokens:
            warnings.warn(DispatchWarning(
                'App token currently unavailable.'
            ))
            # Add it to the (potentially jit) dict record of waiting objs, so
            # we can continue later.
            # Side note: what the fuck is this trash?
            self._pending_by_token[app_token][ghid] = (
                app_token, 
                command, 
                args, 
                kwargs
            )
            
        else:
            # This is a quick way of resolving command into the endpoint 
            # operation.
            endpoint = self._active_tokens[app_token]
            
            try:
                do_dispatch = {
                    'notify_object': endpoint.notify_object_threadsafe,
                    'send_delete': endpoint.send_delete_threadsafe,
                    'notify_share_success': endpoint.notify_share_success_threadsafe,
                    'notify_share_failure': endpoint.notify_share_failure_threadsafe,
                }[command]
            except KeyError as e:
                raise ValueError('Invalid command.') from e
                
            # TODO: fix leaky abstraction that's causing us to spit out threads
            wargs = [ghid]
            wargs.extend(args)
            worker = threading.Thread(
                target = do_dispatch,
                daemon = True,
                args = wargs,
                kwargs = kwargs,
            )
            worker.start()
    
    def register_endpoint(self, endpoint):
        ''' Registers an endpoint and all of its appdefs / APIs. If the
        endpoint has already been registered, updates it.
        
        Note: this cannot be used to create new app tokens! The token 
        must already be known to the dispatcher.
        '''
        app_token = endpoint.app_token
        # This cannot be used to create new app tokens!
        if app_token not in self._known_tokens:
            raise ValueError('Endpoint app token is unknown to dispatcher.')
        
        if app_token in self._active_tokens:
            if self._active_tokens[app_token] is not endpoint:
                raise RuntimeError(
                    'Attempt to reregister a new endpoint for the same token. '
                    'Each app token must have exactly one endpoint.'
                )
        else:
            self._active_tokens[app_token] = endpoint
        
        # Do this regardless, so that the endpoint can use this to update apis.
        for api in endpoint.apis:
            self._api_ids[api].add(app_token)
        # Note: how to handle removing api_ids?
            
    def close_endpoint(self, endpoint):
        ''' Closes this client's connection to the endpoint, meaning it
        will not be able to handle any more requests. However, does not
        (and should not) clean tokens from the api_id dict.
        '''
        del self._active_tokens[endpoint.app_token]
        
    def get_tokens(self, api_id):
        ''' Gets the local app tokens registered as capable of handling
        the passed API id.
        '''
        if api_id in self._api_ids:
            return frozenset(self._api_ids[api_id])
        else:
            raise KeyError(
                'Dispatcher does not have a token for passed api_id.'
            )
        
    def new_token(self):
        # Use a dummy api_id to force the while condition to be true initially
        token = b'\x00\x00\x00\x00'
        # Birthday paradox be damned; we can actually *enforce* uniqueness
        while token in self._known_tokens:
            token = os.urandom(4)
        # Do this right away to prevent race condition (todo: also use lock?)
        # Todo: something to make sure the token is actually being used?
        self._known_tokens.add(token)
        return token
        
        
class EmbeddedMemoryAgent(AgentBase, MemoryPersister, _TestDispatcher):
    def __init__(self):
        super().__init__(persister=self, dispatcher=self)
        
        
def agentfactory(persisters, ipc_hosts):
    pass