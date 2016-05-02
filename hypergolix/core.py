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
from golix import Guid
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
from .exceptions import InaccessibleError
from .exceptions import UnknownPartyError
from .exceptions import DispatchError
from .exceptions import DispatchWarning

from .persisters import _PersisterBase
from .persisters import MemoryPersister

from .ipc_hosts import _IPCBase
from .ipc_hosts import _EndpointBase
        
# ###############################################
# Utilities, etc
# ###############################################


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
        
        # This bit is clever. We want to allow for garbage collection of 
        # secrets as soon as they're no longer needed, but we also need a way
        # to get secrets for the targets of dynamic binding frames. So, let's
        # combine a chainmap and a weakvaluedictionary.
        self._secrets_persistent = {}
        self._secrets_staging = {}
        self._secrets_proxy = weakref.WeakValueDictionary()
        self._secrets = collections.ChainMap(
            self._secrets_persistent, 
            self._secrets_staging,
            self._secrets_proxy
        )
        self._secrets_pending = {}
        
        self._contacts = {}
        # Bindings lookup: {<target guid>: <binding guid>}
        self._holdings = {}
        # History lookup for dynamic bindings' frame guids. 
        # {<dynamic guid>: <frame deque>}
        # Note that the deque must use a maxlen or it will grow indefinitely.
        self._historian = {}
        # Target lookup for most recent frame in dynamic bindings.
        # {<dynamic guid>: <most recent target>}
        self._dynamic_targets = {}
        # Lookup for pending requests. {<request address>: <target address>}
        self._pending_requests = {}
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
            guid = self._identity.guid, 
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
        ''' Return the Agent's Guid.
        '''
        return self._identity.guid
        
    def _retrieve_contact(self, guid):
        ''' Attempt to retrieve a contact, first from self, then from 
        persister. Raise UnknownPartyError if failure. Return the 
        contact.
        '''
        if guid in self._contacts:
            contact = self._contacts[guid]
        else:
            try:
                contact_packed = self.persister.get(guid)
                contact = SecondParty.from_packed(contact_packed)
                self._contacts[guid] = contact
            except NakError as e:
                raise UnknownPartyError(
                    'Could not find identity in known contacts or at the '
                    'Agent\'s persistence provider.'
                ) from e
        return contact
        
    def _request_listener(self, request_guid):
        ''' Callback to handle any requests.
        '''
        request_packed = self.persister.get(request_guid)
        request_unpacked = self._identity.unpack_request(request_packed)
        requestor_guid = request_unpacked.author
        
        try:
            requestor = self._retrieve_contact(requestor_guid)
        finally:
            self._do_debind(request_unpacked.guid)
            
        request = self._identity.receive_request(
            requestor = requestor,
            request = request_unpacked
        )
            
        if isinstance(request, AsymHandshake):
            self._handle_req_handshake(request, request_unpacked.guid)
            
        elif isinstance(request, AsymAck):
            self._handle_req_ack(request, request_unpacked.guid)
            
        elif isinstance(request, AsymNak):
            self._handle_req_nak(request, request_unpacked.guid)
            
        else:
            raise RuntimeError('Encountered and unknown request type.')
            
    def _handle_req_handshake(self, request, source_guid):
        ''' Handles a handshake request after reception.
        '''
        # Note that get_object handles committing the secret (etc).
        self._set_secret_pending(
            secret = request.secret, 
            guid = request.target
        )
        
        try:
            self.dispatcher.dispatch_handshake(request.target)
            
        except HandshakeError as e:
            # Erfolglos. Send a nak to whomever sent the handshake
            response_obj = self._identity.make_nak(
                target = source_guid
            )
            self.cleanup_guid(request.target)
            # print(repr(e))
            # traceback.print_tb(e.__traceback__)
            
        else:
            # Success. Send an ack to whomever sent the handshake
            response_obj = self._identity.make_ack(
                target = source_guid
            )
            
        response = self._identity.make_request(
            recipient = self._retrieve_contact(request.author),
            request = response_obj
        )
        self.persister.publish(response.packed)
            
    def _handle_req_ack(self, request, source_guid):
        ''' Handles a handshake ack after reception.
        '''
        target = self._pending_requests[request.target]
        del self._pending_requests[request.target]
        self.dispatcher.dispatch_handshake_ack(request, target)
            
    def _handle_req_nak(self, request, source_guid):
        ''' Handles a handshake nak after reception.
        '''
        target = self._pending_requests[request.target]
        del self._pending_requests[request.target]
        self.dispatcher.dispatch_handshake_nak(request, target)
        
    @property
    def persister(self):
        return self._persister
        
    @property
    def dispatcher(self):
        return self._dispatcher
        
    def _get_secret(self, guid):
        ''' Return the secret for the passed guid, if one is available.
        If unknown, raise InaccessibleError.
        '''
        # If guid is a dynamic binding, resolve it into the most recent frame,
        # which is how we're storing secrets in self._secrets.
        if guid in self._historian:
            guid = self._historian[guid][0]
            
        try:
            return self._secrets[guid]
        except KeyError as e:
            raise InaccessibleError('Agent has no access to object.') from e
            
    def _set_secret(self, guid, secret):
        ''' Stores the secret for the passed guid persistently.
        '''
        # Should this add parallel behavior for handling dynamic guids?
        if guid not in self._secrets_persistent:
            self._secrets[guid] = secret
            
    def _set_secret_temporary(self, guid, secret):
        ''' Stores the secret for the passed guid only as long as it is
        used by other objects.
        '''
        if guid not in self._secrets_proxy:
            self._secrets_proxy[guid] = secret
            
    def _set_secret_pending(self, guid, secret):
        ''' Sets a pending secret of unknown target type.
        '''
        if guid not in self._secrets_pending:
            self._secrets_pending[guid] = secret
            
    def _stage_secret(self, unpacked):
        ''' Moves a secret from _secrets_pending into its appropriate
        location.
        
        NOTE: we haven't verified a dynamic binding yet, so we might 
        accidentally store a key that doesn't verify. We should check 
        and remove them if we're forced to clean up.
        '''
        guid = unpacked.guid
        
        # Short-circuit if we've already dispatched the secret.
        if guid in self._secrets:
            return
        
        if isinstance(unpacked, GEOC):
            secret = self._secrets_pending.pop(guid)
            
        elif isinstance(unpacked, GOBD):
            try:
                secret = self._secrets_pending.pop(guid)
            except KeyError:
                secret = self._secrets_pending.pop(unpacked.guid_dynamic)
                
            # No need to stage this, since it isn't persistent if we don't
            # commit. Note that _set_secret handles the case of existing 
            # secrets, so we should be immune to malicious attempts to override 
            # existing secrets.
            # Temporarily point the secret at the target, in case the target
            # is immediately GC'd on update. But, persistently stage it to the
            # frame guid (below).
            self._set_secret_temporary(unpacked.target, secret)
            
        if guid not in self._secrets_staging:
            self._secrets_staging[guid] = secret
            
    def _commit_secret(self, guid):
        ''' The secret was successfully used to load the object. Commit
        it to persistent store.
        '''
        if guid in self._secrets_staging:
            secret = self._secrets_staging.pop(guid)
            self._set_secret(guid, secret)
            
    def _abandon_secret(self, guid):
        ''' De-stage and abandon a secret, probably due to security 
        issues. Isn't absolutely necessary, as _set_secret won't defer
        to _secrets_staging.
        '''
        if guid in self._secrets_staging:
            del self._secrets_staging[guid]
        
    def _del_secret(self, guid):
        ''' Removes the secret for the passed guid, if it exists. If 
        unknown, raises KeyError.
        '''
        del self._secrets[guid]
        
    def _make_static(self, data, secret=None):
        if secret is None:
            secret = self._identity.new_secret()
        container = self._identity.make_container(
            secret = secret,
            plaintext = data
        )
        return container, secret
        
    def _make_bind(self, guid):
        binding = self._identity.make_bind_static(
            target = guid
        )
        self._holdings[guid] = binding.guid
        return binding
        
    def _do_debind(self, guid):
        ''' Creates a debinding and removes the object from persister.
        '''
        debind = self._identity.make_debind(
            target = guid
        )
        self.persister.publish(debind.packed)
        
    def new_static(self, state):
        ''' Makes a new static object, handling binding, persistence, 
        and so on. Returns container guid.
        '''
        container, secret = self._make_static(state)
        self._set_secret(container.guid, secret)
        binding = self._make_bind(container.guid)
        # This would be a good spot to figure out a way to make use of
        # publish_unsafe.
        # Note that if these raise exceptions and we catch them, we'll
        # have an incorrect state in self._holdings
        self.persister.publish(binding.packed)
        self.persister.publish(container.packed)
        return container.guid
        
    def new_dynamic(self, state, _legroom=None):
        ''' Makes a dynamic object. May link to a static (or dynamic) 
        object's address. state must be either Guid or bytes-like.
        
        The _legroom argument determines how many frames should be used 
        as history in the dynamic binding. If unused, sources from 
        self._legroom.
        '''
        if _legroom is None:
            _legroom = self._legroom
        
        dynamic = self._do_dynamic(state)
            
        # Historian manages the history definition for the object.
        self._historian[dynamic.guid_dynamic] = collections.deque(
            iterable = (dynamic.guid,),
            maxlen = _legroom
        )
        # Add a note to _holdings that "I am my own keeper"
        self._holdings[dynamic.guid_dynamic] = dynamic.guid_dynamic
        
        return dynamic.guid_dynamic
        
    def update_dynamic(self, guid_dynamic, state):
        ''' Like update_object, but does not perform type checking, and
        presumes a correctly formatted state. Not generally recommended
        for outside use.
        '''
        if guid_dynamic not in self._historian:
            raise ValueError(
                'The Agent could not find a record of the object\'s history. '
                'Agents cannot update objects they did not create.'
            )
            
        frame_history = self._historian[guid_dynamic]
        
        old_tail = frame_history[len(frame_history) - 1]
        old_frame = frame_history[0]
        dynamic = self._do_dynamic(
            state = state, 
            guid_dynamic = guid_dynamic,
            history = frame_history,
            secret = self._ratchet_secret(
                secret = self._get_secret(old_frame),
                guid = old_frame
            )
        )
            
        # Update the various buffers and do the object's callbacks
        frame_history.appendleft(dynamic.guid)
        
        # Clear out old key material
        if old_tail not in frame_history:
            # So this should actually be handled via the weakvaluedict -- we
            # should be automatically clearing out anything that isn't the most
            # recent key for the binding.
            if old_tail in self._secrets:
                warnings.warn(RuntimeWarning(
                    'Tail secret was inappropriately retained.'
                ))
            # self._del_secret(old_tail)
        
        # # Since we're not doing a ratchet right now, just do this every time.
        # if obj.address in self._shared_objects:
        #     for recipient in self._shared_objects[obj.address]:
        #         self.hand_object(obj, recipient)
        
    def _do_dynamic(self, state, guid_dynamic=None, history=None, secret=None):
        ''' Actually generate a dynamic binding.
        '''
        if isinstance(state, Guid):
            target = state
            secret = self._get_secret(target)
            container = None
            
        else:
            container, secret = self._make_static(state, secret)
            target = container.guid
            
        dynamic = self._identity.make_bind_dynamic(
            target = target,
            guid_dynamic = guid_dynamic,
            history = history
        )
            
        # Add the secret to the chamber. HOWEVER, associate it with the frame
        # guid, so that (if we've held it as a static object) it won't be 
        # garbage collected when we advance past legroom. Simultaneously add
        # a temporary proxy to it so that it's accessible from the actual 
        # container's guid.
        self._set_secret(dynamic.guid, secret)
        self._set_secret_temporary(target, secret)
            
        self.persister.publish(dynamic.packed)
        if container is not None:
            self.persister.publish(container.packed)
            
        self._dynamic_targets[dynamic.guid_dynamic] = target
        
        return dynamic
        
    def sync_dynamic(self, guid):
        ''' Checks self.persister for a new dynamic frame for guid, and 
        if one exists, gets its state and calls an update on obj.
        
        Should probably modify this to also check state vs current.
        '''
        # Is this doing what I think it's doing?
        frame_guid = guid
        unpacked_binding = self._identity.unpack_bind_dynamic(
            self.persister.get(frame_guid)
        )
        guid = unpacked_binding.guid_dynamic
        
        
        # First, make sure we're not being asked to update something we 
        # initiated the update for.
        if guid not in self._ignore_subs_because_updating:
            # This bit trusts that the persistence provider is enforcing proper
            # monotonic state progression for dynamic bindings.
            last_known_frame = self._historian[guid][0]
            if unpacked_binding.guid == last_known_frame:
                return None
                
            # Note: this probably (?) depends on our memory persister checking 
            # author consistency
            binder = self._retrieve_contact(unpacked_binding.binder)
            
            # Figure out the new target
            target = self._identity.receive_bind_dynamic(
                binder = binder,
                binding = unpacked_binding
            )
            
            secret = self._get_secret(guid)
            offset = unpacked_binding.history.index(last_known_frame)
            
            # Count backwards in index (and therefore forward in time) from the 
            # first new frame to zero (and therefore the current frame).
            # Note that we're using the previous frame's guid as salt.
            # This attempts to heal any broken ratchet.
            for ii in range(offset, -1, -1):
                secret = self._ratchet_secret(
                    secret = secret,
                    guid = unpacked_binding.history[ii]
                )
                
            # Now assign the secret, persistently to the frame guid and temporarily
            # to the container guid
            self._set_secret(unpacked_binding.guid, secret)
            self._set_secret_temporary(target, secret)
            
            # Check to see if we're losing anything from historian while we update
            # it. Remove those secrets.
            old_history = set(self._historian[guid])
            self._historian[guid].appendleft(unpacked_binding.guid)
            new_history = set(self._historian[guid])
            expired = old_history - new_history
            for expired_frame in expired:
                self._del_secret(expired_frame)
            
            return guid, self._fetch_dynamic_state(target)
            
        else:
            return None
        
    def _fetch_dynamic_state(self, target):
        ''' Grabs the state of the dynamic target. Will either return a
        bytes-like object, or a Guid. The latter indicates a linked
        object.
        
        However, can't currently handle using a GIDC as a target.
        '''
        # Unpack the target container and extract the plaintext.
        # NOTE THAT THIS IS GOING TO CAUSE PROBLEMS if it's a nested 
        # dynamic binding. Will probably need to make this recursive at 
        # some point in the future.
        unpacked = self._identity.unpack_any(
            self.persister.get(target)
        )
        if isinstance(unpacked, GEOC):
            try:
                state = self._identity.receive_container(
                    author = self._retrieve_contact(unpacked.author),
                    secret = self._get_secret(target),
                    container = unpacked
                )
            except SecurityError:
                self._abandon_secret(target)
                raise
            else:
                self._commit_secret(target)
            
        elif isinstance(unpacked, GOBD):
            # Simply return the target as the state, since we're linking to it.
            state = target
            
        else:
            raise RuntimeError(
                'The dynamic binding has an illegal target and is therefore '
                'invalid.'
            )
            
        return state
        
    def freeze_dynamic(self, guid_dynamic):
        ''' Creates a static binding for the most current state of a 
        dynamic binding. Returns the static container's guid.
        '''
        if not isinstance(guid_dynamic, Guid):
            raise TypeError(
                'Must pass a dynamic guid to freeze_dynamic.'
            )
            
        # frame_guid = self._historian[obj.address][0]
        target = self._dynamic_targets[guid_dynamic]
        self.hold_guid(target)
        
        # Don't forget to add the actual static resource to _secrets
        self._set_secret(target, self._get_secret(guid_dynamic))
        
        # Return the guid that was just held.
        return target
        
    def hold_guid(self, guid):
        ''' Prevents the deletion of a Guid by binding to it.
        '''
        if not isinstance(guid, Guid):
            raise TypeError(
                'Only guids may be held.'
            )
        # There's not really a reason to create a binding for something you
        # already created, whether static or dynamic, but there's also not
        # really a reason to check if that's the case.
        binding = self._make_bind(guid)
        self.persister.publish(binding.packed)
        self._holdings[guid] = binding.guid
        # Also make sure the secret is persistent.
        self._set_secret(guid, self._get_secret(guid))
        
    def delete_guid(self, guid):
        ''' Removes an object identified by guid (if possible). May 
        produce a warning if the persistence provider cannot remove the 
        object due to another conflicting binding.
        ''' 
        if guid not in self._holdings:
            raise ValueError(
                'Agents cannot attempt to delete objects they have not held. '
                'This may also indicate that the object has already been '
                'deleted.'
            )
            
        binding_guid = self._holdings[guid]
        self._do_debind(binding_guid)
        del self._holdings[guid]
        
        return True
        
    def hand_guid(self, target, recipient):
        ''' Initiates a handshake request with the recipient to share 
        the guid.
        
        This approach may be perhaps somewhat suboptimal for dynamic 
        objects.
        '''
        if not isinstance(target, Guid):
            raise TypeError(
                'target must be Guid or similar.'
            )
        if not isinstance(recipient, Guid):
            raise TypeError(
                'recipient must be Guid or similar.'
            )
            
        contact = self._retrieve_contact(recipient)
        handshake = self._identity.make_handshake(
            target = target,
            secret = self._get_secret(target)
        )
        
        request = self._identity.make_request(
            recipient = contact,
            request = handshake
        )
        
        # Note the potential race condition here. Should catch errors with the
        # persister in case we need to resolve pending requests that didn't
        # successfully post.
        self._pending_requests[request.guid] = target
        self.persister.publish(request.packed)
        
    def _resolve_dynamic_plaintext(self, target):
        ''' Recursively find the plaintext state of the dynamic target. 
        Correctly handles (?) nested dynamic bindings.
        
        However, can't currently handle using a GIDC as a target.
        '''
        # Unpack the target container and extract the plaintext.
        # NOTE THAT THIS IS GOING TO CAUSE PROBLEMS if it's a nested 
        # dynamic binding. Will probably need to make this recursive at 
        # some point in the future.
        unpacked = self._identity.unpack_any(
            self.persister.get(target)
        )
        if isinstance(unpacked, GEOC):
            try:
                plaintext = self._identity.receive_container(
                    author = self._retrieve_contact(unpacked.author),
                    secret = self._get_secret(target),
                    container = unpacked
                )
            except SecurityError:
                self._abandon_secret(target)
                raise
            else:
                self._commit_secret(target)
            
        elif isinstance(unpacked, GOBD):
            # Call recursively.
            nested_target = self._identity.receive_bind_dynamic(
                binder = self._retrieve_contact(unpacked.author),
                binding = unpacked
            )
            plaintext = self._resolve_dynamic_plaintext(nested_target)
            
        else:
            raise RuntimeError(
                'The dynamic binding has an illegal target and is therefore '
                'invalid.'
            )
            
        return plaintext
        
    def get_guid(self, guid):
        ''' Gets a new local copy of the object, assuming the Agent has 
        access to it, as identified by guid. Does not automatically 
        create an object.
        
        Note that for dynamic objects, initial retrieval DOES NOT parse
        any history. It begins with the target and goes from there.
        
        returns the object's state (not necessarily its plaintext) if
        successful.
        '''
        if not isinstance(guid, Guid):
            raise TypeError('Passed guid must be a Guid or similar.')
            
        packed = self.persister.get(guid)
        unpacked = self._identity.unpack_any(packed=packed)
        self._stage_secret(unpacked)
        
        if isinstance(unpacked, GEOC):
            is_dynamic = False
            author = unpacked.author
            contact = self._retrieve_contact(author)
            try:
                state = self._identity.receive_container(
                    author = contact,
                    secret = self._get_secret(guid),
                    container = unpacked
                )
            except SecurityError:
                self._abandon_secret(guid)
                raise
            else:
                self._commit_secret(guid)
            
        elif isinstance(unpacked, GOBD):
            is_dynamic = True
            author = unpacked.binder
            contact = self._retrieve_contact(author)
            target = self._identity.receive_bind_dynamic(
                binder = contact,
                binding = unpacked
            )
            
            # Recursively grab the plaintext once we add the secret
            state = self._fetch_dynamic_state(target)
            
            # Add the dynamic guid to historian using our own _legroom param.
            self._historian[guid] = collections.deque(
                iterable = (unpacked.guid,),
                maxlen = self._legroom
            )
            
        else:
            raise ValueError(
                'Guid resolves to an invalid Golix object. get_guid must '
                'target either a container (GEOC) or a dynamic binding (GOBD).'
            )
            
        # This is gross, not sure I like it.
        return author, is_dynamic, state
        
    def cleanup_guid(self, guid):
        ''' Does anything needed to clean up the object with address 
        guid: removing secrets, unsubscribing from dynamic updates, etc.
        '''
        pass
        
    @staticmethod
    def _ratchet_secret(secret, guid):
        ''' Ratchets a key using HKDF-SHA512, using the associated 
        address as salt. For dynamic files, this should be the previous
        frame guid (not the dynamic guid).
        '''
        cls = type(secret)
        cipher = secret.cipher
        version = secret.version
        len_seed = len(secret.seed)
        len_key = len(secret.key)
        source = bytes(secret.seed + secret.key)
        ratcheted = HKDF(
            master = source,
            salt = bytes(guid),
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
        # Defining b'\x00\x00\x00\x00' will prevent using it as a token.
        self._active_tokens = {
            b'\x00\x00\x00\x00': False,
        }
        # Set of all known tokens. Add b'\x00\x00\x00\x00' to prevent its use.
        self._known_tokens = set()
        self._known_tokens.add(b'\x00\x00\x00\x00')
        
        # Lookup for api_ids -> app_tokens. Should be made persistent across 
        # all clients for any given agent.
        self._api_ids = _JitSetDict()
        
        # Lookup for handshake guid -> handshake object
        self._outstanding_handshakes = {}
        # Lookup for handshake guid -> app_token, recipient
        self._outstanding_shares = {}
        
        # Lookup for guid -> token (meaningful for app-private objs only)
        self._token_by_guid = {}
        # Lookup for guid -> api_id
        self._api_by_guid = {}
        # Lookup for guid -> state (either bytes-like or Guid)
        self._state_by_guid = {}
        # Lookup for guid -> author
        self._author_by_guid = {}
        # Lookup for guid -> dynamic
        self._dynamic_by_guid = {}
        
        # Lookup for guid -> tokens that specifically requested the guid
        self._requestors_by_guid = _JitSetDict()
        self._discarders_by_guid = _JitSetDict()
        
        self._orphan_shares_incoming = []
        self._orphan_shares_outgoing_success = []
        self._orphan_shares_outgoing_failed = []
        
        # Lookup for token -> waiting guid -> operations
        self._pending_by_token = _JitDictDict()
        
        # Very, very quick hack to ignore updates from persisters when we're 
        # the ones who initiated the update. Simple set of guids.
        self._ignore_subs_because_updating = set()
        
    def register_object(self, address, author, state, api_id, app_token, dynamic):
        ''' Sets all applicable tracking dict entries.
        '''
        # This is redundant with new_object, but oh well.
        api_id, app_token = self._normalize_api_and_token(api_id, app_token)
        
        self._dynamic_by_guid[address] = dynamic
        self._state_by_guid[address] = state
        self._author_by_guid[address] = author
        self._token_by_guid[address] = app_token
        self._api_by_guid[address] = api_id
        
        if dynamic:
            self.persister.subscribe(address, self.dispatch_update)
            
    def deregister_object(self, address):
        ''' Removes all applicable tracking, state management, etc.
        '''
        if self._dynamic_by_guid[address]:
            self.persister.unsubscribe(address, self.dispatch_update)
        
        del self._dynamic_by_guid[address]
        del self._state_by_guid[address]
        del self._author_by_guid[address]
        del self._token_by_guid[address]
        del self._api_by_guid[address]
        
        # Todo: what about requestors_by_guid and discarders_by_guid?
        
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
        
    def _get_object(self, guid):
        ''' Gets an object, but doesn't do any tracking based on the 
        requestor. Also doesn't check to see if the object is private or
        not.
        '''
        if guid in self._state_by_guid:
            state = self._state_by_guid[guid]
            api_id = self._api_by_guid[guid]
            app_token = self._token_by_guid[guid]
            author = self._author_by_guid[guid]
            dynamic = self._dynamic_by_guid[guid]
            
        else:
            author, dynamic, wrapped_state = self.get_guid(guid)
            state, api_id, app_token = self._unpack_dispatchable(wrapped_state)
            self.register_object(guid, author, state, api_id, app_token, dynamic)
        
        return author, state, api_id, app_token, dynamic
        
    def get_object(self, asking_token, guid):
        ''' Gets an object by guid for a specific endpoint. Currently 
        only works for non-private objects.
        '''
        author, state, api_id, app_token, dynamic = self._get_object(guid)
        
        if app_token != bytes(4) and app_token != asking_token:
            raise DispatchError(
                'Attempted to load private object from different application.'
            )
        
        self._requestors_by_guid[guid].add(asking_token)
        self._discarders_by_guid[guid].discard(asking_token)
        
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
            
            if isinstance(state, Guid):
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
            guid = address, 
            skip_token = asking_token
        )
        
        return address
        
    def update_object(self, asking_token, guid, state):
        ''' Initiates an update of an object. Must be tied to a specific
        endpoint, to prevent issuing that endpoint a notification in 
        return.
        '''
        try:
            if not self._dynamic_by_guid[guid]:
                raise DispatchError(
                    'Object is not dynamic. Cannot update.'
                )
        except KeyError as e:
            raise DispatchError(
                'Object unknown to dispatcher. Call get_object on address to '
                'sync it before updating.'
            ) from e
        
        
        # So now we know it's a dynamic object and that we know of it.
        api_id = self._api_by_guid[guid]
        app_token = self._token_by_guid[guid]
        wrapped_state = self._pack_dispatchable(state, api_id, app_token)
        
        # Try updating golix before local.
        # Temporarily silence updates from persister about the guid we're 
        # in the process of updating
        try:
            self._ignore_subs_because_updating.add(guid)
            self.update_dynamic(guid, wrapped_state)
        finally:
            self._ignore_subs_because_updating.remove(guid)
        # Successful, so propagate local
        self._state_by_guid[guid] = state
        # Finally, tell everyone what's up.
        self._distribute_to_endpoints(guid, skip_token=asking_token)
        
    def share_object(self, asking_token, guid, recipient):
        ''' Do the whole super thing, and then record which application
        initiated the request, and who the recipient was.
        '''
        # First make sure we actually know the object
        try:
            if self._token_by_guid[guid] != bytes(4):
                raise DispatchError('Cannot share a private object.')
        except KeyError as e:
            raise DispatchError('Attempt to share an unknown object.') from e
        
        try:
            self._outstanding_shares[guid] = (
                asking_token, 
                recipient
            )
            
            # Currently, just perform a handshake. In the future, move this 
            # to a dedicated exchange system.
            self.hand_guid(guid, recipient)
            
        except:
            del self._outstanding_shares[guid]
            raise
        
    def freeze_object(self, asking_token, guid):
        ''' Converts a dynamic object to a static object, returning the
        static guid.
        '''
        try:
            if not self._dynamic_by_guid[guid]:
                raise DispatchError('Cannot freeze a static object.')
        except KeyError as e:
            raise DispatchError(
                'Object unknown to dispatch; cannot freeze. Call get_object.'
            ) from e
            
        author, state, api_id, app_token, dynamic = self._get_object(guid)
        
        address = self.freeze_dynamic(
            guid_dynamic = guid
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
        
    def hold_object(self, asking_token, guid):
        ''' Binds to an address, preventing its deletion. Note that this
        will publicly identify you as associated with the address and
        preventing its deletion to any connected persistence providers.
        '''
        if guid not in self._state_by_guid:
            raise DispatchError(
                'Object unknown to dispatch; cannot hold. Call get_object.'
            )
            
        self.hold_guid(guid)
        
    def delete_object(self, asking_token, guid):
        ''' Debinds an object, attempting to delete it. This operation
        will succeed if the persistence provider accepts the deletion,
        but that doesn't necessarily mean the object was removed. A
        warning may be issued if the object was successfully debound, 
        but other bindings are preventing its removal.
        
        NOTE THAT THIS DELETES ALL COPIES OF THE OBJECT! It will become
        subsequently unavailable to other applications using it.
        '''
        if guid not in self._state_by_guid:
            raise DispatchError(
                'Object unknown to dispatch; cannot delete. Call get_object.'
            )
            
        try:
            self._ignore_subs_because_updating.add(guid)
            self.delete_guid(guid)
        finally:
            self._ignore_subs_because_updating.remove(guid)
        
        # Todo: check to see if this actually results in deleting the object
        # upstream.
        
        # There's only a race condition here if the object wasn't actually 
        # removed upstream.
        self._distribute_to_endpoints(
            guid, 
            skip_token = asking_token, 
            deleted = True
        )
        self.deregister_object(guid)
        
    def discard_object(self, asking_token, guid):
        ''' Removes the object from *only the asking application*. The
        asking_token will no longer receive updates about the object.
        However, the object itself will persist, and remain available to
        other applications. This is the preferred way to remove objects.
        '''
        # This is sorta an accidental check that we're actually tracking the
        # object. Could make it explicit I suppose.
        api_id = self._api_by_guid[guid]
        
        # Completely discard/deregister anything we don't care about anymore.
        interested_tokens = set()
        
        print(self._api_ids[api_id])
        
        if self._token_by_guid[guid] == bytes(4):
            interested_tokens.update(self._api_ids[api_id])
        else:
            interested_tokens.add(self._token_by_guid[guid])
            
        interested_tokens.update(self._requestors_by_guid[guid])
        interested_tokens.difference_update(self._discarders_by_guid[guid])
        interested_tokens.discard(asking_token)
        
        # Now perform actual updates
        if len(interested_tokens) == 0:
            self.deregister_object(guid)
        else:
            self._requestors_by_guid[guid].discard(asking_token)
            self._discarders_by_guid[guid].add(asking_token)
    
    def dispatch_share(self, guid):
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
        app_token, guid, recipient = self._outstanding_shares[target]
        del self._outstanding_shares[target]
        # Now notify just the requesting app of the failed share. Note that
        # this will also handle any missing endpoints.
        self._attempt_contact_endpoint(
            app_token, 
            'notify_share_failure',
            target, 
            recipient = recipient
        )
                    
    def dispatch_update(self, guid):
        ''' Updates local state tracking and distributes the update to 
        the endpoints.
        '''
        # NOTE THAT GUID HERE IS THE FRAME GUID, NOT THE DYNAMIC GUID!
        # Todo: change that, because holy shit.
        # Okay, now do sync and stuff
        result = self.sync_dynamic(guid)
        # That will return None if no update was found
        if result is not None:
            guid, state = result
            self._state_by_guid[guid] = state
            self._distribute_to_endpoints(guid)
                    
    def _distribute_to_endpoints(self, guid, skip_token=None, deleted=False):
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
        if self._token_by_guid[guid] != bytes(4):
            callsheet.add(self._token_by_guid[guid])
            
        # It's not defined, so get everyone that uses that api_id
        else:
            api_id = self._api_by_guid[guid]
            callsheet.update(self._api_ids[api_id])
            
        # Now add anyone explicitly tracking that object
        callsheet.update(self._requestors_by_guid[guid])
        
        # And finally, remove the skip token if present, as well as any apps
        # that have discarded the object
        callsheet.discard(skip_token)
        callsheet.difference_update(self._discarders_by_guid[guid])
            
        if len(callsheet) == 0:
            warnings.warn(HandshakeWarning(
                'Agent lacks application to handle app id.'
            ))
            # self._orphan_shares_incoming.append(guid)
            
        for token in callsheet:
            if deleted:
                # It's mildly dangerous to do this -- what if we throw an 
                # error in _attempt_contact_endpoint?
                self._attempt_contact_endpoint(
                    token, 
                    'send_delete', 
                    guid
                )
            else:
                # It's mildly dangerous to do this -- what if we throw an 
                # error in _attempt_contact_endpoint?
                self._attempt_contact_endpoint(
                    token, 
                    'notify_object', 
                    guid, 
                    state = self._state_by_guid[guid]
                )
                
    def _attempt_contact_endpoint(self, app_token, command, guid, *args, **kwargs):
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
            self._pending_by_token[app_token][guid] = (
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
                    'notify_object': endpoint.notify_object,
                    'send_delete': endpoint.send_delete,
                    'notify_share_success': endpoint.notify_share_success,
                    'notify_share_failure': endpoint.notify_share_failure,
                }[command]
            except KeyError as e:
                raise ValueError('Invalid command.') from e
                
            do_dispatch(guid, *args, **kwargs)
    
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