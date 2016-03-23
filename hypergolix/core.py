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

from golix import FirstParty
from golix import SecondParty
from golix import Guid
from golix import Secret

from golix._getlow import GEOC
from golix._getlow import GOBD

from golix.utils import AsymHandshake
from golix.utils import AsymAck
from golix.utils import AsymNak

from Crypto.Protocol.KDF import scrypt
from Crypto.Hash import SHA512
from Crypto.Protocol.KDF import HKDF

# Intra-package dependencies
from .utils import NakError
from .utils import HandshakeError
from .utils import InaccessibleError
from .utils import UnknownPartyError
from .utils import _ObjectBase
from .utils import StaticObject
from .utils import DynamicObject

from .persisters import _PersisterBase
from .persisters import MemoryPersister
from .clients import _ClientBase
from .clients import EmbeddedClient
        
# ###############################################
# Utilities, etc
# ###############################################


class AgentBase:
    def __init__(self, persister, client, _golix_firstparty=None, _legroom=3, *args, **kwargs):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        '''
        super().__init__(*args, **kwargs)
        
        if not isinstance(persister, _PersisterBase):
            raise TypeError('Persister must subclass _PersisterBase.')
        self._persister = persister
        
        if not isinstance(client, _ClientBase):
            raise TypeError('Client must subclass _ClientBase.')
        self._client = client
        
        if _golix_firstparty is None:
            self._identity = FirstParty()
            self._persister.publish(self._identity.second_party.packed)
        else:
            # Could do type checking here but currently no big deal?
            # This would also be a good spot to make sure our identity already
            # exists at the persister.
            self._identity = _golix_firstparty
            
        # Now subscribe to my identity at the persister.
        self._persister.subscribe(
            guid = self._identity.guid, 
            callback = self._request_listener
        )
        
        # Automatic type checking using max. Can't have smaller than 1.
        self._legroom = max(1, _legroom)
        
        # This bit is clever. We want to allow for garbage collection of 
        # secrets as soon as they're no longer needed, but we also need a way
        # to get secrets for the targets of dynamic binding frames. So, let's
        # combine a chainmap and a weakvaluedictionary.
        self._secrets_persistent = {}
        self._secrets_proxy = weakref.WeakValueDictionary()
        self._secrets = collections.ChainMap(
            self._secrets_persistent, 
            self._secrets_proxy
        )
        
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
        
    @property
    def address(self):
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
        # Note that get_obj handles adding the secret to the store.
        obj = self.get_object(
            secret = request.secret, 
            guid = request.target
        )
        
        try:
            self.client.dispatch_handshake(obj)
            
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
        
        # # Since we're now using ratchets, this is no longer used.
        # if target in self._shared_objects:
        #     self._shared_objects[target].add(request.author)
        # else:
        #     self._shared_objects[target] = { request.author }
            
        self.client.dispatch_handshake_ack(request)
            
    def _handle_req_nak(self, request, source_guid):
        ''' Handles a handshake nak after reception.
        '''
        del self._pending_requests[request.target]
        self.client.dispatch_handshake_nak(request)
        
    @property
    def persister(self):
        return self._persister
        
    @property
    def client(self):
        return self._client
        
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
        
    def new_static(self, data):
        ''' Makes a new static object, handling binding, persistence, 
        and so on. Returns a StaticObject.
        '''
        container, secret = self._make_static(data)
        self._set_secret(container.guid, secret)
        binding = self._make_bind(container.guid)
        # This would be a good spot to figure out a way to make use of
        # publish_unsafe.
        # Note that if these raise exceptions and we catch them, we'll
        # have an incorrect state in self._holdings
        self.persister.publish(binding.packed)
        self.persister.publish(container.packed)
        return StaticObject(
            author = self._identity.guid,
            address = container.guid,
            state = data
        )
        
    def _do_dynamic(self, data, link, guid_dynamic=None, history=None, secret=None):
        ''' Actually generate a dynamic binding.
        '''
        if data is not None and link is None:
            container, secret = self._make_static(data, secret)
            target = container.guid
            state = data
            
        elif link is not None and data is None:
            # Type check the link.
            if not isinstance(link, _ObjectBase):
                raise TypeError(
                    'Link must be a StaticObject, DynamicObject, or similar.'
                )
            target = link.address
            state = link
            secret = self._get_secret(link.address)
            
        else:
            raise TypeError('Must pass either data XOR link to make_dynamic.')
            
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
        if data is not None:
            self.persister.publish(container.packed)
            
        self._dynamic_targets[dynamic.guid_dynamic] = target
        
        return dynamic, state
        
    def new_dynamic(self, data=None, link=None, _legroom=None):
        ''' Makes a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        The _legroom argument determines how many frames should be used 
        as history in the dynamic binding. If unused, sources from 
        self._legroom.
        '''
        if _legroom is None:
            _legroom = self._legroom
        
        dynamic, state = self._do_dynamic(data, link)
            
        # Historian manages the history definition for the object.
        self._historian[dynamic.guid_dynamic] = collections.deque(
            iterable = (dynamic.guid,),
            maxlen = _legroom
        )
        # Add a note to _holdings that "I am my own keeper"
        self._holdings[dynamic.guid_dynamic] = dynamic.guid_dynamic
        
        return DynamicObject(
            author = self._identity.guid,
            address = dynamic.guid_dynamic,
            _buffer = collections.deque(
                iterable = (state,),
                maxlen = _legroom
            )
        )
        
    def update_dynamic(self, obj, data=None, link=None):
        ''' Updates a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        Modifies the dynamic object in place.
        
        Could add a way to update the legroom parameter while we're at
        it. That would need to update the maxlen of both the obj._buffer
        and the self._historian.
        '''
        if not isinstance(obj, DynamicObject):
            raise TypeError(
                'Obj must be a DynamicObject or similar.'
            )
            
        if obj.address not in self._historian:
            raise ValueError(
                'The Agent could not find a record of the object\'s history. '
                'Agents cannot update objects they did not create.'
            )
            
        frame_history = self._historian[obj.address]
        
        old_tail = frame_history[len(frame_history) - 1]
        old_frame = frame_history[0]
        dynamic, state = self._do_dynamic(
            data = data, 
            link = link, 
            guid_dynamic = obj.address,
            history = frame_history,
            secret = self._ratchet_secret(
                secret = self._get_secret(old_frame),
                guid = old_frame
            )
        )
            
        frame_history.appendleft(dynamic.guid)
        obj._buffer.appendleft(state)
        
        # Clear out old key material
        if old_tail not in frame_history:
            self._del_secret(old_tail)
        
        # # Since we're not doing a ratchet right now, just do this every time.
        # if obj.address in self._shared_objects:
        #     for recipient in self._shared_objects[obj.address]:
        #         self.hand_object(obj, recipient)
        
    def refresh_dynamic(self, obj):
        ''' Checks self.persister for a new dynamic frame for obj, and 
        if one exists, updates obj accordingly.
        
        Returns the object back (not a copy).
        '''
        if not isinstance(obj, DynamicObject):
            raise TypeError(
                'refresh_dynamic can only refresh dynamic objects!'
            )
        binder = self._retrieve_contact(obj.author)
        
        unpacked_binding = self._identity.unpack_bind_dynamic(
            self.persister.get(obj.address)
        )
        # This bit trusts that the persistence provider is enforcing proper
        # monotonic state progression for dynamic bindings.
        last_known_frame = self._historian[obj.address][0]
        if unpacked_binding.guid == last_known_frame:
            return
        
        # Figure out the new target
        target = self._identity.receive_bind_dynamic(
            binder = binder,
            binding = unpacked_binding
        )
        
        # We're only going to update with the most recent state, and not 
        # regress up the history chain. However, we should try to heal any
        # repairable ratchet.
        # print('----------------------------------------------')
        # print('Local dynamic address')
        # print(bytes(obj.address))
        # print('With local history')
        # for ent in self._historian[obj.address]:
        #     print(bytes(ent))
        # print('Looking for frame')
        # print(bytes(last_known_frame))
        # print('Remote dynamic address')
        # print(bytes(unpacked_binding.guid_dynamic))
        # print('Remote frame address')
        # print(bytes(unpacked_binding.guid))
        # print('remote history')
        # for ent in unpacked_binding.history:
        #     print(bytes(ent))
        # print('new target')
        # print(bytes(target))
        # print('----------------------------------------------')
        secret = self._get_secret(obj.address)
        offset = unpacked_binding.history.index(last_known_frame)
        # Count backwards in index (and therefore forward in time) from the 
        # first new frame to zero (and therefore the current frame).
        # Note that we're using the previous frame's guid as salt.
        for ii in range(offset, -1, -1):
            secret = self._ratchet_secret(
                secret = secret,
                guid = unpacked_binding.history[ii]
            )
            
        # Now assign the secret, persistently to the frame guid and temporarily
        # to the container guid
        self._set_secret(unpacked_binding.guid, secret)
        self._set_secret_temporary(target, secret)
        
        # # Combine target and history into a single iterable
        # all_known_targets = [target]
        # for frame_guid in unpacked_binding.history:
        #     # Find the frame's historical content
        #     # This could probably stand to see some optimization
            
        #     # First get the frame record. We're assuming the persistence
        #     # provider has checked for consistent author.
        #     unpacked_frame = self._identity.unpack_bind_dynamic(
        #         self.persister.get(frame_guid)
        #     )
        #     all_know_targets.append(
        #         self.receive_bind_dynamic(
        #             binder = author, 
        #             binding = unpacked_frame
        #         )
        #     )
        
        # Check to see if we're losing anything from historian while we update
        # it. Remove those secrets.
        old_history = set(self._historian[obj.address])
        self._historian[obj.address].appendleft(unpacked_binding.guid)
        new_history = set(self._historian[obj.address])
        expired = old_history - new_history
        for expired_frame in expired:
            self._del_secret(expired_frame)
        
        # Get our most recent plaintext value
        plaintext = self._resolve_dynamic_plaintext(target)
        
        # # This will automatically collate everything using the shorter of
        # # the two, between self._legroom and len(all_known_targets)
        # for __, guid_cont in zip(range(self._legroom), all_known_targets):
        #     # This could probably stand to see some optimization
        #     packed_container = self.persister.get(guid_cont)
        #     unpacked_container = self._identity.unpack_container(
        #         packed_target_hist
        #     )
        #     secret = self._get_secret(guid_hist)
        
        # Update the object and return
        obj._buffer.appendleft(plaintext)
        return obj
        
    def freeze_dynamic(self, obj):
        ''' Creates a frozen StaticObject from the most current state of
        a DynamicObject. Returns the StaticObject.
        '''
        if not isinstance(obj, DynamicObject):
            raise TypeError(
                'Only DynamicObjects may be frozen.'
            )
            
        # frame_guid = self._historian[obj.address][0]
        target = self._dynamic_targets[obj.address]
        
        static = StaticObject(
            author = self._identity.guid,
            address = target,
            state = obj.state
        )
        self.hold_object(static)
        
        # Don't forget to add the actual static resource to _secrets
        self._set_secret(target, self._get_secret(obj.address))
        
        return static
        
    def hold_object(self, obj):
        ''' Prevents the deletion of a StaticObject or DynamicObject by
        binding to it.
        '''
        if not isinstance(obj, _ObjectBase):
            raise TypeError(
                'Only dynamic objects and static objects may be held to '
                'prevent their deletion.'
            )
        # There's not really a reason to create a binding for something you
        # already created, whether static or dynamic, but there's also not
        # really a reason to check if that's the case.
        binding = self._make_bind(obj.address)
        self.persister.publish(binding.packed)
        self._holdings[obj.address] = binding.guid
        
    def delete_object(self, obj):
        ''' Removes an object (if possible). May produce a warning if
        the persistence provider cannot remove the object due to another 
        conflicting binding.
        '''
        # if isinstance(obj, StaticObject):
            
        # elif isinstance(obj, DynamicObject):
        #     if obj.author 
            
        # else:
        #     raise TypeError(
        #         'Obj must be StaticObject, DynamicObject, or similar.'
        #     )
            
        if not isinstance(obj, _ObjectBase):
            raise TypeError(
                'Obj must be StaticObject, DynamicObject, or similar.'
            )
            
        if obj.address not in self._holdings:
            raise ValueError(
                'Agents cannot attempt to delete objects they did not create. '
                'This may also indicate that the object has already been '
                'deleted.'
            )
            
        binding_guid = self._holdings[obj.address]
        self._do_debind(binding_guid)
        del self._holdings[obj.address]
        
    def hand_object(self, obj, recipient_guid):
        ''' Initiates a handshake request with the recipient to share 
        the object.
        '''
        contact = self._retrieve_contact(recipient_guid)
        
        if isinstance(obj, DynamicObject):
            # This is, shall we say, suboptimal.
            # frame_guid = self._historian[obj.address][0]
            # target = self._dynamic_targets[obj.address]
            target = obj.address
            
        elif isinstance(obj, StaticObject):
            target = obj.address
            
        else:
            raise TypeError(
                'Obj must be a StaticObject, DynamicObject, or similar.'
            )
        
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
            plaintext = self._identity.receive_container(
                author = self._retrieve_contact(unpacked.author),
                secret = self._get_secret(target),
                container = unpacked
            )
            
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
        
    def get_object(self, secret, guid):
        ''' Gets a new local copy of the object, assuming the Agent has 
        access to it, as identified by guid. Dynamic objects will 
        automatically update themselves.
        
        Note that for dynamic objects, initial retrieval DOES NOT parse
        any history. It begins with the target and goes from there.
        '''
        if not isinstance(guid, Guid):
            raise TypeError('Passed guid must be a Guid or similar.')
            
        packed = self.persister.get(guid)
        unpacked = self._identity.unpack_any(packed=packed)
        
        if isinstance(unpacked, GEOC):
            author = self._retrieve_contact(unpacked.author)
            plaintext = self._identity.receive_container(
                author = author,
                secret = secret,
                container = unpacked
            )
            
            # Looks legit; add the secret to our store persistently. Note that 
            # _set_secret handles the case of existing secrets, so we should be 
            # immune to malicious attempts to override existing secrets.
            self._set_secret(guid, secret)
            
            obj = StaticObject(
                author = unpacked.author,
                address = guid,
                state = plaintext
            )
            
        elif isinstance(unpacked, GOBD):
            author = self._retrieve_contact(unpacked.binder)
            target = self._identity.receive_bind_dynamic(
                binder = author,
                binding = unpacked
            )
            
            # Recursively grab the plaintext
            plaintext = self._resolve_dynamic_plaintext(target)
            
            # Looks legit; add the secret to our store persistently under the
            # frame guid, and as a proxy for the container guid. Note that 
            # _set_secret handles the case of existing secrets, so we should be 
            # immune to malicious attempts to override existing secrets.
            self._set_secret(unpacked.guid, secret)
            self._set_secret_temporary(target, secret)
            
            # Add the dynamic guid to historian using our own _legroom param.
            self._historian[guid] = collections.deque(
                iterable = (unpacked.guid,),
                maxlen = self._legroom
            )
            
            # Create local state. Ignore their history, and preserve our own,
            # since we may have different ideas about how much legroom we need
            obj = DynamicObject(
                author = unpacked.binder,
                address = guid,
                _buffer = collections.deque(
                    iterable = (plaintext,),
                    maxlen = self._legroom
                )
            )
            
            # Create an auto-update method for obj.
            updater = lambda __, obj=obj: self.refresh_dynamic(obj)
            self.persister.subscribe(guid, updater)
            
        else:
            raise ValueError(
                'Guid resolves to an invalid Golix object. get_object must '
                'target either a container (GEOC) or a dynamic binding (GOBD).'
            )
            
        return obj
        
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
    def login(cls, password, data, persister, client):
        ''' Load an Agent from an identity contained within a GEOC.
        '''
        pass
        
        
class EmbeddedMemoryAgent(AgentBase, MemoryPersister, EmbeddedClient):
    def __init__(self):
        super().__init__(persister=self, client=self)