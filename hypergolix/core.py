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
    'StaticObject',
    'DynamicObject',
    'EmbeddedMemoryAgent'
]

# Global dependencies
import collections
import weakref
import threading
import os
import msgpack

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
from .utils import _ObjectBase
from .utils import StaticObject
from .utils import DynamicObject

from .exceptions import NakError
from .exceptions import HandshakeError
from .exceptions import InaccessibleError
from .exceptions import UnknownPartyError

from .persisters import _PersisterBase
from .persisters import MemoryPersister

from .ipc_hosts import _IPCBase
from .ipc_hosts import TestIPC

from .embeds import AppObj
        
# ###############################################
# Utilities, etc
# ###############################################


class AgentBootstrap(dict):
    ''' Threadsafe. Handles all of the stuff needed to actually build an
    agent and keep consistent state across devices / logins.
    '''
    def __init__(self, agent, obj):
        self._mutlock = threading.Lock()
        self._agent = agent
        self._obj = obj
        self._def = None
        
    @classmethod
    def from_existing(cls, agent, obj):
        self = cls(agent, obj)
        self._def = obj.state
        super().update(msgpack.unpackb(obj.state))
        
    def __setitem__(self, key, value):
        with self._mutlock:
            super().__setitem__(key, value)
            self._update_def()
        
    def __getitem__(self, key):
        ''' For now, this is actually just unpacking self._def and 
        returning the value for that. In the future, when we have some
        kind of callback mechanism in dynamic objects, we'll probably
        cache and stuff.
        '''
        with self._mutlock:
            tmp = msgpack.unpackb(self._def)
            return tmp[key]
        
    def __delitem__(self, key):
        with self._mutlock:
            super().__delitem__(key)
            self._update_def()
        
    def pop(*args, **kwargs):
        raise TypeError('AgentBootstrap does not support popping.')
        
    def popitem(*args, **kwargs):
        raise TypeError('AgentBootstrap does not support popping.')
        
    def update(*args, **kwargs):
        with self._mutlock:
            super().update(*args, **kwargs)
            self._update_def()
        
    def _update_def(self):
        self._def = msgpack.packb(self)
        self._agent.update_object(self._obj, self._def)


class AgentBase:
    ''' Base class for all Agents.
    '''
    
    DEFAULT_LEGROOM = 3
    
    def __init__(self, persister, ipc_host, _identity=None, _bootstrap=None, *args, **kwargs):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        
        persister isinstance _PersisterBase
        ipc_host isinstance _IPCBase
        _identity isinstance golix.FirstParty
        _bootstrap isinstance tuple(golix.Guid, golix.Secret)
        '''
        super().__init__(*args, **kwargs)
        
        if not isinstance(persister, _PersisterBase):
            raise TypeError('Persister must subclass _PersisterBase.')
        self._persister = persister
        
        if not isinstance(ipc_host, _IPCBase):
            raise TypeError('ipc_host must subclass _IPCBase.')
        self._ipc_host = ipc_host
        
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
        # Lookup for shared objects. {<obj address>: {<recipients>}}
        self._shared_objects = {}
        
        # # Automatic type checking using max. Can't have smaller than 1.
        # self._legroom = max(1, _legroom)
        
        # In this case, we need to create our own bootstrap.
        if _identity is None and _bootstrap is None:
            self._identity = FirstParty()
            self._persister.publish(self._identity.second_party.packed)
            self._bootstrap = AgentBootstrap(
                agent = self,
                obj = self._new_bootstrap_container()
            )
            # Default for legroom. Currently hard-coded and not overrideable.
            self._bootstrap['legroom'] = self.DEFAULT_LEGROOM
            
        elif _identity is not None and _bootstrap is not None:
            # Could do type checking here but currently no big deal?
            # This would also be a good spot to make sure our identity already
            # exists at the persister.
            self._identity = _identity
            # Get bootstrap
            self._set_secret_pending(
                guid = _bootstrap[0],
                secret = _bootstrap[1]
            )
            bootstrap_obj = self.get_object(bootstrap_guid)
            bootstrap = AgentBootstrap.from_existing(
                obj = bootstrap_obj,
                agent = self
            )
            self._bootstrap = bootstrap
            
        else:
            raise ValueError(
                'Must either declare both _golix_firstpary and _bootstrap, or '
                'neither of them.'
            )
            
        # Now subscribe to my identity at the persister.
        self._persister.subscribe(
            guid = self._identity.guid, 
            callback = self._request_listener
        )
        
    def _new_bootstrap_container(self):
        ''' Creates a new container to use for the bootstrap object.
        '''
        padding_size = int.from_bytes(os.urandom(1), byteorder='big')
        padding = os.urandom(padding_size)
        return self.new_object(padding, dynamic=True)
        
    @property
    def _legroom(self):
        ''' Get the legroom from our bootstrap. If it hasn't been 
        created yet (aka within __init__), return the class default.
        '''
        try:
            return self._bootstrap['legroom']
        except (AttributeError, KeyError):
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
        # Note that get_obj handles committing the secret (etc).
        self._set_secret_pending(
            secret = request.secret, 
            guid = request.target
        )
        obj = self.get_object(request.target)
        
        try:
            self.ipc_host.dispatch_handshake(obj)
            
        except HandshakeError as e:
            # Erfolglos. Send a nak to whomever sent the handshake
            response_obj = self._identity.make_nak(
                target = source_guid
            )
            self.cleanup_object(obj)
            
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
        self.ipc_host.dispatch_handshake_ack(request, target)
            
    def _handle_req_nak(self, request, source_guid):
        ''' Handles a handshake nak after reception.
        '''
        target = self._pending_requests[request.target]
        del self._pending_requests[request.target]
        self.ipc_host.dispatch_handshake_nak(request, target)
        
    @property
    def persister(self):
        return self._persister
        
    @property
    def ipc_host(self):
        return self._ipc_host
        
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
        
    def new_object(self, state, dynamic=True, _legroom=None):
        ''' Creates a new object. Wrapper for AppObj.__init__.
        '''
        return AppObj(
            # Todo: update embed intelligently
            embed = self,
            state = state,
            dynamic = dynamic,
            _legroom = _legroom
        )
        
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
        
    def _do_dynamic(self, state, guid_dynamic=None, history=None, secret=None):
        ''' Actually generate a dynamic binding.
        '''
        if isinstance(state, AppObj):
            target = state.address
            secret = self._get_secret(state.address)
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
        
    def new_dynamic(self, state, _legroom=None):
        ''' Makes a dynamic object. May link to a static (or dynamic) 
        object's address. state must be either AppObj or bytes-like.
        
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
        
    def update_object(self, obj, state):
        ''' Updates a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        Wraps AppObj.update and modifies the dynamic object in place.
        
        Could add a way to update the legroom parameter while we're at
        it. That would need to update the maxlen of both the obj._buffer
        and the self._historian.
        '''
        if not isinstance(obj, AppObj):
            raise TypeError(
                'Obj must be an AppObj.'
            )
            
        obj.update(state)
        
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
            self._del_secret(old_tail)
        
        # # Since we're not doing a ratchet right now, just do this every time.
        # if obj.address in self._shared_objects:
        #     for recipient in self._shared_objects[obj.address]:
        #         self.hand_object(obj, recipient)
        
    def sync_dynamic(self, obj):
        ''' Checks self.persister for a new dynamic frame for obj, and 
        if one exists, gets its state and calls an update on obj.
        
        Should probably modify this to also check state vs current.
        '''
        binder = self._retrieve_contact(obj.author)
        
        unpacked_binding = self._identity.unpack_bind_dynamic(
            self.persister.get(obj.address)
        )
        # This bit trusts that the persistence provider is enforcing proper
        # monotonic state progression for dynamic bindings.
        last_known_frame = self._historian[obj.address][0]
        if unpacked_binding.guid == last_known_frame:
            return False
        
        # Figure out the new target
        target = self._identity.receive_bind_dynamic(
            binder = binder,
            binding = unpacked_binding
        )
        
        secret = self._get_secret(obj.address)
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
        old_history = set(self._historian[obj.address])
        self._historian[obj.address].appendleft(unpacked_binding.guid)
        new_history = set(self._historian[obj.address])
        expired = old_history - new_history
        for expired_frame in expired:
            self._del_secret(expired_frame)
        
        # Get our most recent state and update the object.
        obj.update(
            state = self._resolve_dynamic_state(target), 
            _preexisting=True
        )
        
        return True
        
    def sync_static(self, obj):
        ''' Ensures the obj's state matches its associated plaintext. If
        not, raise.
        '''
        raise NotImplementedError('Have yet to set up static syncing.')
        
    def sync_object(self, obj):
        ''' Wraps AppObj.sync.
        '''
        if not isinstance(obj, AppObj):
            raise TypeError('Must pass AppObj or subclass to sync_object.')
            
        return obj.sync()
        
    def freeze_object(self, obj):
        ''' Wraps AppObj.freeze. Note: does not currently traverse 
        nested dynamic bindings.
        '''
        if not isinstance(obj, AppObj):
            raise TypeError(
                'Only AppObj may be frozen.'
            )
        return obj.freeze()
        
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
        
    def hold_object(self, obj):
        ''' Wraps AppObj.hold.
        '''
        if not isinstance(obj, AppObj):
            raise TypeError('Only AppObj may be held by hold_object.')
        obj.hold()
        
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
        
    def delete_object(self, obj):
        ''' Wraps AppObj.delete. 
        '''
        if not isinstance(obj, AppObj):
            raise TypeError(
                'Obj must be AppObj or similar.'
            )
            
        obj.delete()
        
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
        
    def hand_object(self, obj, recipient_guid):
        ''' Initiates a handshake request with the recipient to share 
        the object.
        
        Should probably retool to be hand_guid.
        '''
        contact = self._retrieve_contact(recipient_guid)
        
        if not isinstance(obj, AppObj):
            raise TypeError(
                'Obj must be a AppObj or similar.'
            )
    
        # This is, shall we say, suboptimal, for dynamic objects.
        # frame_guid = self._historian[obj.address][0]
        # target = self._dynamic_targets[obj.address]
        target = obj.address
        
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
        self._pending_requests[request.guid] = obj.address
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
        
    def _resolve_dynamic_state(self, target):
        ''' Recursively find the state of the dynamic target. Correctly 
        handles (?) nested dynamic bindings. DOES NOT GUARANTEE that it
        will return a plaintext -- it may return nested AppObjs instead.
        
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
            # Call recursively.
            nested_target = self._identity.receive_bind_dynamic(
                binder = self._retrieve_contact(unpacked.author),
                binding = unpacked
            )
            upstream = self._resolve_dynamic_state(nested_target)
            state = AppObj(
                embed = self,
                state = upstream,
                dynamic = True,
                _preexisting = True,
                _legroom = self._legroom
            )
            
        else:
            raise RuntimeError(
                'The dynamic binding has an illegal target and is therefore '
                'invalid.'
            )
            
        return state
        
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
            state = self._resolve_dynamic_state(target)
            
            # Add the dynamic guid to historian using our own _legroom param.
            self._historian[guid] = collections.deque(
                iterable = (unpacked.guid,),
                maxlen = self._legroom
            )
            
        else:
            raise ValueError(
                'Guid resolves to an invalid Golix object. get_object must '
                'target either a container (GEOC) or a dynamic binding (GOBD).'
            )
            
        # This is gross, not sure I like it.
        return author, is_dynamic, state
        
    def get_object(self, guid):
        ''' Wraps AppObj.__init__  and get_guid for preexisting objects.
        '''
        author, is_dynamic, state = self.get_guid(guid)
            
        return AppObj(
            # Todo: make the embed more intelligent
            embed = self,
            state = state,
            dynamic = is_dynamic,
            _preexisting = (guid, author)
        )
        
    def cleanup_object(self, obj):
        ''' Does anything needed to clean up the object -- removing 
        secrets, unsubscribing from dynamic updates, etc.
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
        
    def register(self, password):
        ''' Save the agent's identity to a GEOC object.
        
        THIS NEEDS TO BE A DYNAMIC BINDING SO IT CAN UPDATE THE KEY TO
        THE BOOTSTRAP OBJECT. Plus, futureproofing. But, we also cannot,
        under any circumstances, reuse a Secret. So, instead of simply 
        using the scrypt output directly, we should put it through a
        secondary hkdf, using the previous frame guid as salt, to ensure
        a new key, while maintaining updateability and accesibility.
        '''
        # Condense everything we need to rebuild self._golix_provider
        keys = self._golix_provider._serialize()
        # Store the guid for the dynamic bootstrap object
        bootstrap = self._bootstrap_binding
        # Create some random-length, random padding to make it harder to
        # guess that our end-product GEOC is a saved Agent
        padding = None
        # Put it all into a GEOC.
        # Scrypt the password. Salt against the author GUID, which we know
        # (when reloading) from the author of the file!
        # Use 2**14 for t<=100ms, 2**20 for t<=5s
        combined = scrypt(
            password = password, 
            salt = bytes(self._golix_provider.guid),
            key_len = 48,
            N = 2**15,
            r = 8,
            p = 1
        )
        secret = Secret(
            cipher = 1,
            key = combined[:32],
            seed = combined[32:48]
        )
        
    @classmethod
    def login(cls, password, data, persister, ipc_host):
        ''' Load an Agent from an identity contained within a GEOC.
        '''
        pass
        
        
class EmbeddedMemoryAgent(AgentBase, MemoryPersister, TestIPC):
    def __init__(self):
        super().__init__(persister=self, ipc_host=self)
        
        
def agentfactory(persisters, ipc_hosts):
    pass