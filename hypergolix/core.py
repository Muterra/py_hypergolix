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
    'Agent', 
    'StaticObject',
    'DynamicObject'
]

# Global dependencies
import collections

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
from .persisters import _PersisterBase
from .clients import _ClientBase
        
# ###############################################
# Utilities, etc
# ###############################################
        

class _ObjectBase:
    ''' Hypergolix objects cannot be directly updated. They must be 
    passed to Agents for modification (if applicable). They do not (and, 
    if you subclass, for security reasons they should not) reference a
    parent Agent.
    
    Objects provide a simple interface to the arbitrary binary data 
    contained within Golix containers. They track both the plaintext, 
    and the associated GUID. They do NOT expose the secret key material
    of the container.
    
    From the perspective of an external method, *all* Objects should be 
    treated as read-only. They should only ever be modified by Agents.
    '''
    __slots__ = [
        '_author',
        '_address'
    ]
    
    _REPROS = ['author', 'address']
    
    def __init__(self, author, address):
        ''' Creates a new object. Address is the dynamic guid. State is
        the initial state.
        '''
        self._author = author
        self._address = address
        
    @property
    def author(self):
        return self._author
        
    @property
    def address(self):
        return self._address
    
    # This might be a little excessive, but I guess it's nice to have a
    # little extra protection against updates?
    def __setattr__(self, name, value):
        ''' Prevent rewriting declared attributes.
        '''
        try:
            __ = getattr(self, name)
        except AttributeError:
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                'StaticObjects and DynamicObjects do not support mutation of '
                'attributes once they have been declared.'
            )
            
    def __delattr__(self, name):
        ''' Prevent deleting declared attributes.
        '''
        raise AttributeError(
            'StaticObjects and DynamicObjects do not support deletion of '
            'attributes.'
        )
            
    def __repr__(self):
        ''' Automated repr generation based upon class._REPROS.
        '''
        c = type(self).__name__ 
        
        s = '('
        for attr in self._REPROS:
            s += attr + '=' + repr(getattr(self, attr)) + ', '
        s = s[:len(s) - 2]
        s += ')'
        return c + s

        
class StaticObject(_ObjectBase):
    ''' An immutable object. Can be produced directly, or by freezing a
    dynamic object.
    '''
    __slots__ = [
        '_author',
        '_address',
        '_state'
    ]
    
    _REPROS = ['author', 'address', 'state']
    
    def __init__(self, author, address, state):
        super().__init__(author, address)
        self._state = state
        
    @property
    def state(self):
        return self._state
        
    def __eq__(self, other):
        if not isinstance(other, StaticObject):
            raise TypeError(
                'Cannot compare StaticObjects to non-StaticObject-like Python '
                'objects.'
            )
            
        return (
            self.author == other.author and
            self.address == other.address and
            self.state == other.state
        )
    
    
class DynamicObject(_ObjectBase):
    ''' A mutable object. Updatable by Agents.
    Interestingly, this could also do the whole __setattr__/__delattr__
    thing from above, since we're overriding state, and buffer updating
    is handled by the internal deque.
    '''
    __slots__ = [
        '_author',
        '_address',
        '_buffer'
    ]
    
    _REPROS = ['author', 'address', '_buffer']
    
    def __init__(self, author, address, _buffer):
        super().__init__(author, address)
        
        if not isinstance(_buffer, collections.deque):
            raise TypeError('Buffer must be collections.deque or similar.')
        if not _buffer.maxlen:
            raise ValueError(
                'Buffers without a max length will grow to infinity. Please '
                'declare a max length.'
            )
            
        self._buffer = _buffer
        
    @property
    def state(self):
        ''' Return the current value of the object. Will always return
        a value, even for a linked object.
        '''
        frame = self._buffer[0]
        
        # Resolve into the actual value if necessary
        if isinstance(frame, _ObjectBase):
            frame = frame.state
            
        return frame
        
    @property
    def buffer(self):
        ''' Returns a tuple of the current buffer.
        '''
        # Note that this has the added benefit of preventing assignment
        # to the internal buffer!
        return tuple(self._buffer)
        
    def __eq__(self, other):
        if not isinstance(other, DynamicObject):
            raise TypeError(
                'Cannot compare DynamicObjects to non-DynamicObject-like '
                'Python objects.'
            )
            
        return (
            self.author == other.author and
            self.address == other.address and
            self.buffer == other.buffer
        )


class Agent():
    def __init__(self, persister, client, _golix_firstparty=None):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        '''
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
        
        self._secrets = {}
        self._contacts = {}
        # Bindings lookup: {<target guid>: <binding guid>}
        self._bindings = {}
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
        # First add the secret to our store
        # This will break if we change behavior of self._secrets.
        if request.target not in self._secrets:
            self._set_secret(request.target, request.secret)
        obj = self.get_object(request.target)
        
        try:
            self._client.dispatch_handshake(obj)
            
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
            
        self._client.dispatch_handshake_ack(request)
            
    def _handle_req_nak(self, request, source_guid):
        ''' Handles a handshake nak after reception.
        '''
        del self._pending_requests[request.target]
        self._client.dispatch_handshake_nak(request)
        
    @property
    def persister(self):
        return self._persister
        
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
        ''' Stores the secret for the passed guid.
        '''
        # Should this add parallel behavior for handling dynamic guids?
        self._secrets[guid] = secret
        
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
        self._bindings[guid] = binding.guid
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
        # have an incorrect state in self._bindings
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
        # garbage collected when we advance past legroom.
        self._set_secret(dynamic.guid, secret)
            
        self.persister.publish(dynamic.packed)
        if data is not None:
            self.persister.publish(container.packed)
            
        self._dynamic_targets[dynamic.guid_dynamic] = target
        
        return dynamic, state
        
    def new_dynamic(self, data=None, link=None, _legroom=3):
        ''' Makes a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        The _legroom argument determines how many frames should be used 
        as history in the dynamic binding.
        '''
        dynamic, state = self._do_dynamic(data, link)
            
        # Historian manages the history definition for the object.
        self._historian[dynamic.guid_dynamic] = collections.deque(
            iterable = (dynamic.guid,),
            maxlen = _legroom
        )
        # Add a note to _bindings that "I am my own keeper"
        self._bindings[dynamic.guid_dynamic] = dynamic.guid_dynamic
        
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
        
        # This will break if we ever change the behavior of self._secrets
        # Clear out old key material
        if old_tail not in frame_history:
            del self._secrets[old_tail]
        
        # # Since we're not doing a ratchet right now, just do this every time.
        # if obj.address in self._shared_objects:
        #     for recipient in self._shared_objects[obj.address]:
        #         self.share_object(obj, recipient)
        
    def refresh_dynamic(self, obj, guid):
        ''' Retrieves the guid from the storage provider, evaluates if 
        it is a new dynamic frame for obj, and if so, updates obj 
        accordingly.
        '''
        pass
        
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
        self._bindings[obj.address] = binding.guid
        
    def delete_object(self, obj):
        ''' Removes an object (if possible). May produce a warning if
        the persistence provider cannot remove the object due to another 
        conflicting binding.
        '''
        if not isinstance(obj, _ObjectBase):
            raise TypeError(
                'Obj must be StaticObject, DynamicObject, or similar.'
            )
            
        if obj.address not in self._bindings:
            raise ValueError(
                'Agents cannot attempt to delete objects they did not create. '
                'This may also indicate that the object has already been '
                'deleted.'
            )
            
        binding_guid = self._bindings[obj.address]
        self._do_debind(binding_guid)
        del self._bindings[obj.address]
        
    def share_object(self, obj, recipient_guid):
        ''' Initiates a request with the recipient to share the object.
        '''
        contact = self._retrieve_contact(recipient_guid)
        
        if isinstance(obj, DynamicObject):
            # This is, shall we say, suboptimal.
            # frame_guid = self._historian[obj.address][0]
            target = self._dynamic_targets[obj.address]
            
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
        
    def get_object(self, guid):
        ''' Gets an object, assuming the Agent has access to it, as 
        identified by guid.
        '''
        if not isinstance(guid, Guid):
            raise TypeError('Passed guid must be a Guid or similar.')
            
        secret = self._get_secret(guid)
        packed = self.persister.get(guid)
        unpacked = self._identity.unpack_container(packed=packed)
        
        if isinstance(unpacked, GEOC):
            author = self._retrieve_contact(unpacked.author)
            plaintext = self._identity.receive_container(
                author = author,
                secret = secret,
                container = unpacked
            )
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
    def login(cls, password, data):
        ''' Load an Agent from an identity contained within a GEOC.
        '''
        pass