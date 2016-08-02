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
    'HGXCore', 
]

# Global dependencies
import collections
# import collections.abc
import weakref
import threading
import os
import msgpack
import abc
import traceback
import warnings
import atexit

from golix import SecondParty
from golix import Ghid
from golix import Secret
from golix import SecurityError

from golix._getlow import GEOC
from golix._getlow import GOBD
from golix._getlow import GARQ
from golix._getlow import GDXX

# Intra-package dependencies
from .exceptions import NakError
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning
from .exceptions import UnknownParty
from .exceptions import DoesNotExist

# from .persisters import _PersisterBase

# from .ipc import _IPCBase
# from .ipc import _EndpointBase


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

        
# ###############################################
# Lib
# ###############################################


# Do this to silence unsubscribing for GAOs when exiting
global is_exiting
is_exiting = False

@atexit.register
def _silence_unsubs_atexit(*args, **kwargs):
    global is_exiting
    is_exiting = True


class _GhidProxier:
    ''' Resolve the base container GHID from any associated ghid. Uses
    all weak references, so should not interfere with GCing objects.
    
    Threadsafe.
    '''
    def __init__(self, core, persister):
        # The usual.
        self._refs = {}
        self._librarian = persister.librarian
        self._core = weakref.proxy(core)
        self._modlock = threading.Lock()
        
    def is_proxy(self, ghid):
        ''' Return True if the ghid is known as a proxy, False if the 
        ghid is not known as a proxy.
        
        False doesn't guarantee that it's not a proxy, but True 
        guarantees that it is.
        '''
        return ghid in self._refs
        
    def chain(self, proxy, target):
        ''' Set, or update, a ghid proxy.
        
        Ghids must only ever have a single proxy. Calling chain on an 
        existing proxy will update the target.
        '''
        if not isinstance(proxy, Ghid):
            raise TypeError('Proxy must be Ghid.')
            
        if not isinstance(target, Ghid):
            raise TypeError('Target must be ghid.')
        
        with self._modlock:
            self._refs[proxy] = target
            
    def resolve(self, ghid):
        ''' Protect the entry point with a global lock, but don't leave
        the recursive bit alone.
        
        TODO: make this guarantee, through using the persister's 
        librarian, that the resolved ghid IS, in fact, a container.
        
        TODO: consider adding a depth limit to resolution.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('Can only resolve a ghid.')
            
        with self._modlock:
            return = self._resolve(ghid)
        
    def _resolve(self, ghid):
        ''' Recursively resolves the container ghid for a proxy (or a 
        container).
        '''
        # First check our local cache of aliases
        try:
            return self._resolve(self._refs[ghid])
            
        # We don't know the local cache, so let's check librarian to make sure
        # it's not a dynamic address.
        except KeyError:
            obj = self._librarian.whois(ghid)
            
            if isinstance(obj, _GobdLite):
                # Don't call chain, because we'd hit a modlock.
                self._refs[ghid] = obj.target
                return self._resolve(obj.target)
                
            else:
                return ghid
            
            
class GolixCore:
    ''' Wrapper around Golix library that automates much of the state
    management, holds the Agent's identity, etc etc.
    '''
    DEFAULT_LEGROOM = 3
    
    def __init__(self, identity):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        
        persister isinstance _PersisterBase
        dispatcher isinstance DispatcherBase
        _identity isinstance golix.FirstParty
        '''
        self._opslock = threading.Lock()
        
        # if not isinstance(persister, _PersisterBase):
        #     raise TypeError('Persister must subclass _PersisterBase.')
        self._identity = identity
        
        self._librarian = None
        self._oracle = None
        self._dispatch = None
        self._ghidproxy = None
        self._persister = None
        
    def link_proxy(self, proxy):
        ''' Chicken vs egg. Should be called ASAGDFP after __init__.
        '''
        self._ghidproxy = proxy
        
    def link_oracle(self, oracle):
        ''' Chicken vs egg. Call immediately after linking proxy.
        '''
        self._oracle = oracle
        
    def link_persister(self, persister):
        ''' Chicken vs egg. Should call immediately after linking proxy
        and oracle.
        '''
        self._persister = persister
        self._librarian = persister.librarian
    
    def link_dispatch(self, dispatch):
        ''' Chicken vs egg. Must call AFTER linking persister.
        '''
        # if not isinstance(dispatch, DispatcherBase):
        #     raise TypeError('dispatcher must subclass DispatcherBase.')
        self._dispatch = dispatch
        
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
        
    def open_request(self, request):
        ''' Just like it says on the label...
        Note that the request is PACKED, not unpacked.
        '''
        # TODO: have this interface with rolodex.
        with self._opslock:
            unpacked = self._identity.unpack_request(request)
            
        requestor = SecondParty.from_packed(
            self._librarian.dereference(unpacked.author)
        )
        payload = self._identity.receive_request(requestor, unpacked)
        return payload
        
    def make_request(self, recipient, payload):
        # Just like it says on the label...
        recipient = SecondParty.from_packed(
            self._librarian.dereference(recipient)
        )
        with self._opslock:
            return self._identity.make_request(
                recipient = recipient,
                request = payload,
            )
        
    def open_container(self, container, secret):
        # Wrapper around golix.FirstParty.receive_container.
        author = SecondParty.from_packed(
            self._librarian.dereference(container.author)
        )
        with self._opslock:
            return self._identity.receive_container(
                author = author,
                secret = secret,
                container = container
            )
        
    def make_container(self, data, secret):
        # Simple wrapper around golix.FirstParty.make_container
        with self._opslock:
            return self._identity.make_container(
                secret = secret,
                plaintext = data
            )

    def make_binding_stat(self, target):
        # Note that this requires no open() method, as bindings are verified by
        # the local persister.
        with self._opslock:
            return self._identity.make_bind_static(target)
        
    def make_binding_dyn(self, target, ghid=None, history=None):
        # Make a new binding!
        if (ghid is None and history is None):
            pass
            
        # Update an existing binding!
        elif (ghid is not None and history is not None):
            pass
            
        # Error!
        else:
            raise ValueError('Mixed def of ghid/history while dyn binding.')
            
        with self._opslock:
            return self._identity.make_bind_dynamic(
                target = target,
                ghid_dynamic = ghid_dynamic,
                history = history
            )
        
    def make_debinding(self, target):
        # Simple wrapper around golix.FirstParty.make_debind
        with self._opslock:
            return self._identity.make_debind(target)


class HGXCore:
    ''' Base class for all Agents.
    '''
    
    DEFAULT_LEGROOM = 3
    
    def __init__(self, identity):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        
        persister isinstance _PersisterBase
        dispatcher isinstance DispatcherBase
        _identity isinstance golix.FirstParty
        '''
        self._opslock = threading.Lock()
        
        # if not isinstance(persister, _PersisterBase):
        #     raise TypeError('Persister must subclass _PersisterBase.')
        self._identity = identity
        
        self._oracle = None
        self._privateer = None
        self._dispatch = None
        self._ghidproxy = None
        self._persister = None
        
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
        
    def link_proxy(self, proxy):
        ''' Chicken vs egg. Should be called ASAGDFP after __init__.
        '''
        self._ghidproxy = proxy
        
    def link_oracle(self, oracle):
        ''' Chicken vs egg. Call immediately after linking proxy.
        '''
        self._oracle = oracle
        
    def link_persister(self, persister):
        ''' Chicken vs egg. Should call immediately after linking proxy
        and oracle.
        '''
        self._persister = persister
        # Now subscribe to my identity at the persister.
        self._persister.subscribe(
            ghid = self._identity.ghid, 
            callback = self._request_listener
        )
        
    def link_privateer(self, privateer):
        ''' Chicken vs egg. Call immediately after linking oracle.
        '''
        self._privateer = privateer
    
    def link_dispatch(self, dispatch):
        ''' Chicken vs egg. Must call AFTER linking privateer.
        '''
        try:
            self._privateer
        except AttributeError as exc:
            raise RuntimeError('Must link privateer before dispatch.') from exc
            
        # if not isinstance(dispatch, DispatcherBase):
        #     raise TypeError('dispatcher must subclass DispatcherBase.')
        self._dispatch = dispatch
        
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
        
    def _request_listener(self, subscription, notification):
        ''' Callback to handle any requests.
        '''
        # Note that the notification could also be a GDXX.
        request_packed = self.persister.get(notification)
        request_unpacked = self._identity.unpack_any(request_packed)
        if isinstance(request_unpacked, GARQ):
            requestor_ghid = request_unpacked.author
        elif isinstance(request_unpacked, GDXX):
            # This case should only be relevant if we have multiple agents 
            # logged in at separate locations at the same time, processing the
            # same GARQs. For now, ignore it entirely.
            # TODO: fix this.
            return
        else:
            raise RuntimeError(
                'Unexpected Golix primitive while listening for requests.'
            )
        
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
            self.dispatch.dispatch_handshake(request.target)
            
        except HandshakeError as e:
            # Erfolglos. Send a nak to whomever sent the handshake
            response_obj = self._identity.make_nak(
                target = source_ghid
            )
            self.cleanup_ghid(request.target)
            
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
        self.dispatch.dispatch_handshake_ack(request.target)
            
    def _handle_req_nak(self, request, source_ghid):
        ''' Handles a handshake nak after reception.
        '''
        self.dispatch.dispatch_handshake_nak(request.target)
        
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
    def dispatch(self):
        return self._dispatch
        
    def _make_static(self, data, secret=None):
        with self._opslock:
            if secret is None:
                secret = self._identity.new_secret()
            container = self._identity.make_container(
                secret = secret,
                plaintext = data
            )
            try:
                self._privateer.stage(container.ghid, secret)
                self._privateer.commit(container.ghid)
            except:
                print('#############################################')
                print('ERROR WHILE STAGING AND COMMITTING SECRET.')
                print('Previously staged with traceback: ')
                print(self._privateer.last_commit(container.ghid))
                print('#############################################')
                raise
        return container
        
    def _make_bind(self, ghid):
        with self._opslock:
            binding = self._identity.make_bind_static(
                target = ghid
            )
            self._holdings[ghid] = binding.ghid
        return binding
        
    def _do_debind(self, ghid):
        ''' Creates a debinding and removes the object from persister.
        '''
        with self._opslock:
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
        
        return dynamic.ghid_dynamic, dynamic.ghid
        
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
        
        # TODO IMPORTANT:
        # First we need to decide if we're going to ratchet the secret, or 
        # create a new one. The latter will require updating anyone who we've
        # shared it with. Ratcheting is only available if the last target was
        # directly referenced.
        
        # Note that this is not directly a security issue, because of the 
        # specifics of ratcheting: each dynamic binding is salting with the
        # frame ghid, which will be different for each dynamic binding. So we
        # won't ever see a two-time pad, but we might accidentally break the
        # ratchet.
        
        # However, note that this (potentially substantially) decreases the
        # strength of the ratchet, in the event that the KDF salting does not
        # sufficiently alter the KDF seed.
        
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
            secret = self._privateer._ratchet_secret(
                secret = self._privateer.get(current_container),
                ghid = old_frame
            )
        )
            
        # Update the various buffers and do the object's callbacks
        frame_history.appendleft(dynamic.ghid)
        
        # Return the frame ghid
        return dynamic.ghid
        
    def _do_dynamic(self, state, ghid_dynamic=None, history=None, secret=None):
        ''' Actually generate a dynamic binding.
        '''
        if isinstance(state, Ghid):
            target = state
            container = None
            
        else:
            container = self._make_static(state, secret)
            target = container.ghid
            
        with self._opslock:
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
        
    def touch_dynamic(self, ghid, notification=None):
        ''' Checks self.persister for a new dynamic frame for ghid, and 
        if one exists, gets its state and calls an update on obj.
        
        Should probably modify this to also check state vs current.
        
        TODO: throw out this whole fucking mess, because it's redundant
        with get_ghid, and shitty, etc etc.
        '''
        # This should be fixed to now pass the subscription ghid and not
        # the frame ghid.
        # # Is this doing what I think it's doing?
        # frame_ghid = ghid
        try:
            unpacked_binding = self._identity.unpack_bind_dynamic(
                self.persister.get(ghid)
            )
            
        # Appears to be a deletion; let's verify.
        except DoesNotExist as exc:
            try:
                presumptive_deletion = self._identity.unpack_debind(
                    self.persister.get(notification)
                )
                if presumptive_deletion.target == ghid:
                    obj = self._oracle.get_object(gaoclass=_GAO, ghid=ghid)
                    obj.apply_delete()
                    return None
                else:
                    raise ValueError(
                        'Notification appears to delete a different binding.'
                    )
                
            # Re-raise any issues from the first exc.
            except Exception as exc2:
                raise exc2 from exc
            
        # Should be fixed, as per above.
        # ghid = unpacked_binding.ghid_dynamic
        # # Goddammit. It is. Why the hell are we passing frame_ghid as ghid?!
        
        # First, make sure we're not being asked to update something we 
        # initiated the update for.
        # TODO: fix leaky abstraction
        obj = self._oracle.get_object(gaoclass=_GAO, ghid=ghid)
        if unpacked_binding.ghid not in obj._silenced:
            # This bit trusts that the persistence provider is enforcing proper
            # monotonic state progression for dynamic bindings. Check that our
            # current frame is the most recent, and if so, return immediately.
            last_known_frame = self._historian[ghid][0]
            if unpacked_binding.ghid == last_known_frame:
                return None
                
            # Get our CURRENT secret (before dereffing)
            old_target = self._ghidproxy.resolve(ghid)
            secret = self._privateer.get(old_target)
            # Get our FUTURE target
            target = self._deref_ghid(ghid)
            offset = unpacked_binding.history.index(last_known_frame)
            
            # Count backwards in index (and therefore forward in time) from the 
            # first new frame to zero (and therefore the current frame).
            # Note that we're using the previous frame's ghid as salt.
            # This attempts to heal any broken ratchet.
            for ii in range(offset, -1, -1):
                secret = self._privateer._ratchet_secret(
                    secret = secret,
                    ghid = unpacked_binding.history[ii]
                )
                
            # Stage our FUTURE secret
            self._privateer.stage(target, secret)
            
            # Dispatcher handles secret lifetimes, so don't worry about that
            # here.
            self._historian[ghid].appendleft(unpacked_binding.ghid)
            
            author, is_dynamic, state = self.get_ghid(target)
            return state
            
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
        
        with self._opslock:
            handshake = self._identity.make_handshake(
                target = target,
                secret = self._privateer.get(container_ghid)
            )
            
            request = self._identity.make_request(
                recipient = contact,
                request = handshake
            )
        
        # Note that this must be called before publishing to the persister, or
        # there's a race condition between them.
        self._dispatch.await_handshake_response(target, request.ghid)
        
        # TODO: move all persister operations to some dedicated something or 
        # other. Oracle maybe?
        # Note the potential race condition here. Should catch errors with the
        # persister in case we need to resolve pending requests that didn't
        # successfully post.
        self.persister.publish(request.packed)
        
        return request.ghid
        
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
        
        
class Oracle:
    ''' Source for total internal truth and state tracking of objects.
    
    Maintains <ghid>: <obj> lookup. Used by dispatchers to track obj
    state. Might eventually be used by AgentBase. Just a quick way to 
    store and retrieve any objects based on an associated ghid.
    '''
    def __init__(self, core):
        ''' Sets up internal tracking.
        '''
        self._opslock = threading.Lock()
        self._core = core
        self._lookup = {}
            
    def get_object(self, gaoclass, ghid, **kwargs):
        with self._opslock:
            try:
                obj = self._lookup[ghid]
                if not isinstance(obj, gaoclass):
                    raise TypeError(
                        'Object has already been resolved, and is not the '
                        'correct GAO class.'
                    )
                    
            except KeyError:
                obj = gaoclass.from_ghid(
                    core = self._core, 
                    ghid = ghid, 
                    **kwargs
                )
                self._lookup[ghid] = obj
                
            return obj
        
    def new_object(self, gaoclass, **kwargs):
        ''' Creates a new object and returns it. Passes all *args and
        **kwargs to the declared gao_class. Eliminates the need to pass
        core, or call push.
        '''
        with self._opslock:
            obj = gaoclass(core=self._core, **kwargs)
            obj.push()
            self._lookup[obj.ghid] = obj
            return obj
        
    def forget(self, ghid):
        ''' Removes the object from the cache. Next time an application
        wants it, it will need to be acquired from persisters.
        
        Indempotent; will not raise KeyError if called more than once.
        '''
        with self._opslock:
            try:
                del self._lookup[ghid]
            except KeyError:
                pass
            
    def __contains__(self, ghid):
        ''' Checks for the ghid in cache (but does not check for global
        availability; that would require checking the persister for its
        existence and the privateer for access).
        '''
        return ghid in self._lookup
        
        
class _GAOBase(metaclass=abc.ABCMeta):
    ''' Defines the interface for _GAOs. Mostly here for documentation
    (and, eventually, maybe testing) purposes.
    '''
    @abc.abstractmethod
    def apply_state(self, state):
        ''' Apply the UNPACKED state to self.
        '''
        pass
        
    @abc.abstractmethod
    def extract_state(self):
        ''' Extract self into a packable state.
        '''
        pass
        
    @abc.abstractmethod
    def apply_delete(self):
        ''' Executes an external delete.
        '''
        pass
        
    @staticmethod
    @abc.abstractmethod
    def _pack(state):
        ''' Packs state into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        '''
        pass
        
    @staticmethod
    @abc.abstractmethod
    def _unpack(packed):
        ''' Unpacks state from a bytes object. May be overwritten in 
        subs to unpack more complex objects. Should always be a 
        staticmethod or classmethod.
        '''
        pass
        
    @classmethod
    @abc.abstractmethod
    def from_ghid(cls, core, ghid, **kwargs):
        ''' Construct the GAO from a passed (existing) ghid.
        '''
        pass
            
            
_GAOBootstrap = collections.namedtuple(
    typename = '_GAOBootstrap',
    field_names = ('bootstrap_name', 'bootstrap_secret', 'bootstrap_state'),
)
        
    
class _GAO(_GAOBase):
    ''' Base class for Golix-Aware Objects (Golix Accountability 
    Objects?). Anyways, used by core to handle plaintexts and things.
    
    TODO: thread safety? or async safety?
    
    TODO: support reference nesting.
    
    TODO: consider consolidating references to the circus (oracle, etc)
        here, into _GAO, wherever possible.
    '''
    def __init__(self, core, persister, privateer, ghidproxy, dynamic, 
                _legroom=None, _bootstrap=None, accountant=None):
        ''' Creates a golix-aware object. If ghid is passed, will 
        immediately pull from core to sync with existing object. If not,
        will create new object on first push. If ghid is not None, then
        dynamic will be ignored.
        '''
        # TODO: move all silencing code into the persistence core.
        
        self.__opslock = threading.RLock()
        self.__updater = None
        self.__state = None
        
        self._deleted = False
        self._core = core
        self._persister = persister
        self._privateer = privateer
        self._ghidproxy = ghidproxy
        
        self.dynamic = bool(dynamic)
        self.ghid = None
        self.author = None
        # self._frame_ghid = None
        
        if _legroom is None:
            _legroom = core._legroom
        # Legroom must have a minimum of 3
        _legroom = max([_legroom, 3])
        self._legroom = _legroom
        
        # TODO: reorganize to that silence will automatically silence anything
        # within _history
        # Most recent FRAME GHIDS
        self._history = collections.deque(maxlen=_legroom)
        # Most recent FRAME TARGETS
        self._history_targets = collections.deque(maxlen=_legroom)
        
        # This probably only needs to be maxlen=2, but it's a pretty negligible
        # footprint to add some headspace
        self._silenced = collections.deque(maxlen=3)
        self._update_greenlight = threading.Event()
        
        super().__init__(*args, **kwargs)
        
        # This will only get passed if this is a NEW bootstrap object TO CREATE
        if _bootstrap is not None:
            # First, short-circuit and immediately apply a state.
            self.apply_state(_bootstrap[2])
            # Now immediately create the object, bypassing the privateer's 
            # secret lookup.
            with self.__opslock:
                self.__new(_bootstrap[1])
            # Let the accountant know that we have a new bootstrap address.
            accountant.set_bootstrap_address(_bootstrap[0], self.ghid)
            
    def freeze(self):
        ''' Creates a static binding for the most current state of a 
        dynamic binding. Returns a new GAO.
        '''
        if not self.dynamic:
            raise TypeError('Cannot freeze a static GAO.')
            
        container_ghid = self._ghidproxy.resolve(self.ghid)
        binding = self._core.make_binding_stat(container_ghid)
        self._persister.ingest_gobs(binding)
        
        return self._oracle.get_object(
            gaoclass = type(self), 
            ghid = container_ghid
        )
        
    def delete(self):
        ''' Attempts to permanently remove (aka debind) the object.
        ''' 
        if self.dynamic:
            debinding = self._core.make_debinding(self.ghid)
            self._persister.ingest_gdxx(debinding)
            
        else:
            # Get frozenset of binding ghids
            bindings = self._persister.bookie.bind_status()
            
            for binding in bindings:
                obj = self._persister.librarian.whois(binding)
                if isinstance(obj, _GobsLite):
                    if obj.author == self._core.whoami:
                        debinding = self._core.make_debinding(obj.ghid)
                        self._persister.ingest_gdxx(debinding)
            
        self.apply_delete()
        
    def halt_updates(self):
        # TODO: this is going to create a resource leak in a race condition:
        # if we get an update while deleting, it will just sit there waiting
        # forever.
        with self.__opslock:
            self._update_greenlight.clear()
        
    def silence(self, notification):
        ''' Silence update processing for the object when the update
        notification ghid matches the frame ghid.
        
        Since this is only set up to silence one ghid at a time, this
        depends upon our local persister enforcing monotonic frame 
        progression.
        
        TODO: move all silencing into the persistence core.
        '''
        with self.__opslock:
            # Make this indempotent so we can't accidentally forget everything
            # else
            if notification not in self._silenced:
                self._silenced.appendleft(notification)
        
    def unsilence(self, notification):
        ''' Unsilence update processing for the object at the particular
        notification ghid.
        '''
        with self.__opslock:
            try:
                self._silenced.remove(notification)
            except ValueError:
                pass
                
    @staticmethod
    def _attempt_open_container(core, privateer, secret, packed):
        try:
            # TODO: fix this leaky abstraction.
            unpacked = core._identity.unpack_container(packed)
            packed_state = core.open_container(unpacked, secret)
        
        except SecurityError:
            privateer.abandon(unpacked.ghid)
            raise
            
        else:
            privateer.commit(unpacked.ghid)
            
        return packed_state
            
    @classmethod
    def from_ghid(cls, core, persister, ghidproxy, privateer, ghid, 
                _legroom=None, _bootstrap=None, *args, **kwargs):
        ''' Loads the GAO from an existing ghid.
        
        _bootstrap allows for bypassing looking up the secret at the
        privateer.
        '''
        container_ghid = ghidproxy.resolve(ghid)
        
        # If the container ghid does not match the passed ghid, this must be a
        # dynamic object.
        dynamic = (container_ghid != ghid)
        
        packed = persister.get(container_ghid)
        secret = privateer.get(container_ghid)
        
        packed_state = cls._attempt_open_container(core, privateer, secret, 
                                                    packed)
        
        self = cls(
            core = core, 
            persister = persister,
            privateer = privateer,
            ghidproxy = ghidproxy,
            dynamic = dynamic, 
            _legroom = _legroom, 
            *args, **kwargs
        )
        self.ghid = ghid
        self.author = author
        
        unpacked_state = self._unpack(packed_state)
        self.apply_state(unpacked_state)
        
        if dynamic:
            binding = persister.librarian.whois(ghid)
            self._history.extend(binding.history)
            self._history.appendleft(binding.frame_ghid)
            self._history_targets.appendleft(binding.target)
            persister.subscribe(ghid, self._weak_touch)
            
        # DON'T FORGET TO SET THIS!
        self._update_greenlight.set()
        
        return self
        
    @staticmethod
    def _pack(state):
        ''' Packs state into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        '''
        return state
        
    @staticmethod
    def _unpack(packed):
        ''' Unpacks state from a bytes object. May be overwritten in 
        subs to unpack more complex objects. Should always be a 
        staticmethod or classmethod.
        '''
        return packed
        
    def apply_state(self, state):
        ''' Apply the UNPACKED state to self.
        '''
        self.__state = state
        
    def extract_state(self):
        ''' Extract self into a packable state.
        '''
        return self.__state
        
    def apply_delete(self):
        ''' Executes an external delete.
        '''
        with self.__opslock:
            self._deleted = True
    
    def push(self):
        ''' Pushes updates to upstream. Must be called for every object
        mutation.
        '''
        with self.__opslock:
            if not self._deleted:
                if self.ghid is None:
                    self.__new()
                else:
                    if self.dynamic:
                        self.__update()
                    else:
                        raise TypeError('Static GAOs cannot be updated.')
            else:
                raise TypeError('Deleted GAOs cannot be pushed.')
        
    def pull(self, notification=None):
        ''' Refreshes self from upstream. Should NOT be called at object 
        instantiation for any existing objects. Should instead be called
        directly, or through _weak_touch for any new status.
        '''
        with self.__opslock:
            if self._deleted:
                raise TypeError('Deleted GAOs cannot be pulled.')
                
            else:
                if self.dynamic:
                    modified = self._pull_dynamic(notification)
                        
                else:
                    modified = self._pull_static(notification)
        
        return modified
        
    def _pull_dynamic(self, notification):
        ''' Note that this is redundant with persister_core handling
        update silencing.
        
        TODO: remove redundancy.
        '''
        if notification is None:
            check_address = self.ghid
        else:
            check_address = notification
        check_obj = self._persister.librarian.whois(check_address)
        
        # First check to see if we're deleting the object.
        if isinstance(check_obj, _GdxxLite):
            modified = self._attempt_delete(check_obj)
            
        # This is the redundant bit. Well... maybe it isn't, if we allow for
        # explicit pull calls, outside of just doing an update check. Anyways,
        # Now we check if it's something we already know about.
        elif check_obj.frame_ghid in self._history:
            modified = False
        
        # Huh, looks like it is, in fact, new.
        else:
            modified = True
            self._privateer.heal_ratchet(gao=self, binding=check_obj)
            secret = self._privateer.get(check_obj.target)
            packed = self._persister.get(check_obj.target)
            packed_state = self._attempt_open_container(
                self._core, 
                self._privateer, 
                secret, 
                packed
            )
            
            # Don't forget to extract state before applying it
            self.apply_state(self._unpack(packed_state))
            # Also don't forget to update history.
            self._advance_history(check_obj)
            
        return modified
        
    def _advance_history(self, new_obj):
        ''' Updates our history to match the new one.
        '''
        old_history = self._history
        # Cannot concatenate deque to list, so use extend instead
        new_history = collections.deque([new_obj.frame_ghid])
        new_history.extend(new_obj.history)
        
        right_offset = self._align_histories(old_history, new_history)
                
        # Check for a match. If right_offset == 0, there was no match, and we 
        # need to reset everything. Note: this will break any existing ratchet.
        if right_offset == 0:
            self._history.clear()
            self._history_targets.clear()
            self._history.appendleft(new_obj.frame_ghid)
            self._history_targets.appendleft(new_obj.target)
        
        # We did, in fact, find a match. Let's combine appropriately.
        else:
            # Note that deque.extendleft reverses the order of things.
            # Also note that new_history is a new, local object, so we don't
            # need to worry about accidentally affecting something else.
            new_history.reverse()
            self._history.extendleft(new_history[:right_offset])
            
            # Now let's create new targets. Simply populate any missing bits 
            # with None...
            new_targets = [None] * (right_offset - 1)
            # ...and then add the current target as the last item (note 
            # reversal as per above), and then extend self._history_targets.
            new_targets.append(new_obj.target)
            self._history_targets.extendleft(new_targets)
        
    @staticmethod
    def _align_histories(old_history, new_history):
        ''' Attempts to align two histories.
        '''
        jj = 0
        for ii in range(len(new_history)):
            # Check current element against offset
            if new_history[ii] == old_history[jj]:
                jj += 1
                continue
                
            # No match. Check to see if we matched the zeroth element instead.
            elif new_history[ii] == old_history[0]:
                jj = 1
                
            # No match there either. Reset.
            else:
                jj = 0
                
        return jj
            
    def _attempt_delete(self, deleter):
        ''' Attempts to apply a delete. Returns True if successful;
        False otherwise.
        '''
        if obj.target == self.ghid:
            self.apply_delete()
            modified = True
            
        else:
            logger.warning(
                'Mismatching pull debinding target: \n'
                '    obj.ghid:         ' + str(bytes(self.ghid)) + '\n'
                '    debinding.target: ' + str(bytes(obj.target))
            )
            modified = False
            
        return modified
        
    def _pull_static(self, notification):
        ''' Currently, doesn't actually do anything. Just assumes that 
        our local state is correct, and returns modified=False.
        '''
        return False
        
    def touch(self, subscription, notification):
        ''' Notifies the object to check upstream for changes.
        '''
        # While we're doing this unholy abomination of half async, half
        # threaded, just spin this out into a thread to avoid async
        # deadlocks (which we'll otherwise almost certainly encounter)
        # TODO: asyncify all the things
        worker = threading.Thread(
            target = self.__touch,
            daemon = True,
            args = (subscription, notification),
        )
        worker.start()
            
    def __touch(self, subscription, notification):
        ''' Method to be called by self.touch from within thread.
        '''
        # First we need to wait for the update greenlight.
        self._update_greenlight.wait()
        if notification not in self._silenced:
            self.pull(notification)
        
    def __new(self, bypass_secret=None):
        ''' Creates a new Golix object for self using self._state, 
        self._dynamic, etc.
        '''
        # We're doing this so that privateer can make use of GAO for its
        # own bootstrap object.
        if bypass_secret is not None:
            container = self._core.make_container(
                self._pack(self.extract_state()), 
                bypass_secret
            )
        else:
            secret = self._privateer.new_secret()
            container = self._core.make_container(
                self._pack(self.extract_state()), 
                secret
            )
            self._privateer.stage(container.ghid, secret)
            self._privateer.commit(container.ghid)
        
        if self.dynamic:
            binding = self._core.make_binding_dyn(target=container.ghid)
            # NOTE THAT THIS IS A GOLIX PRIMITIVE! And that therefore there's a
            # discrepancy between ghid_dynamic and ghid.
            address = binding.ghid_dynamic
            # Silence the **frame address** (see above) and add it to historian
            self.silence(binding.ghid)
            self._history.appendleft(binding.ghid)
            self._history_targets.appendleft(container.ghid)
            # Now assign this dynamic address as a chain owner.
            self._privateer.make_chain(address, container.ghid)
            # Finally, publish the binding and subscribe to it
            self._persister.ingest_gobd(binding)
            self._persister.subscribe(address, self._weak_touch)
            
        else:
            binding = self._core.make_binding_stat(target=container.ghid)
            address = container.ghid
            self._persister.ingest_gobs(binding)
        
        self.author = self._core.whoami
        self.ghid = address
            
        # Finally, publish the container itself.
        self._persister.ingest_geoc(container)
        
        # Successful creation. Clear us for updates.
        self._update_greenlight.set()
        
    def __update(self):
        ''' Updates an existing golix object. Must already have checked
        that we 1) are dynamic, and 2) already have a ghid.
        
        If there is an error updating, this will attempt to do a pull to
        automatically roll back the current state. NOTE THAT THIS MAY
        OR MAY NOT BE THE ACTUAL CURRENT STATE!
        '''
        try:
            # Pause updates while doing this.
            self._update_greenlight.clear()
            
            # We need a secret.
            try:
                secret = self._privateer.ratchet(self.ghid)
            # TODO: make this a specific error.
            except:
                logger.info(
                    'Failed to ratchet secret for ' + str(bytes(self.ghid))
                )
                secret = self._privateer.new_secret()
                
            container = self._core.make_container(
                self._pack(self.extract_state()),
                secret
            )
            binding = self._core.make_binding_dyn(
                target = container.ghid,
                ghid = self.ghid,
                history = self._history
            )
            # NOTE THE DISCREPANCY between the Golix dynamic binding version
            # of ghid and ours! This is silencing the frame ghid.
            self.silence(binding.ghid)
            self._history.appendleft(binding.ghid)
            self._history_targets.appendleft(container.ghid)
            # Publish to persister
            self._persister.ingest_gobd(binding)
            self._persister.ingest_geoc(container)
            
            # And now, as everything was successful, update the ratchet
            self._privateer.update_chain(self.ghid, container.ghid)
            
        except:
            # We had a problem, so we're going to forcibly restore the object
            # to the last known good state.
            logger.error(
                'Failed to update object; forcibly restoring state.\n' + 
                ''.join(traceback.format_exc())
            )
            
            # TODO: move core._ghidproxy to here.
            container_ghid = self._core._ghidproxy.resolve(self.ghid)
            secret = self._privateer.get(container_ghid)
            packed = self._persister.get(container_ghid)
            packed_state = self._attempt_open_container(
                self._core, 
                self._privateer, 
                secret, 
                packed
            )
            
            # TODO: fix these leaky abstractions.
            self.apply_state(self._unpack(packed_state))
            binding = self._persister.librarian.whois(ghid)
            # Don't forget to fix history as well
            self._advance_history(binding)
            
            raise
            
        finally:
            # Resume accepting updates.
            self._update_greenlight.set()
            
    @property
    def _weak_touch(self):
        # This is a disgusting workaround to get the weakmethod to work right.
        # Basically, I want WeakMethod to be WeakMethodProxy.
        if not self.__updater:
            # Note that this is now a closure.
            r = weakref.WeakMethod(self.touch)
            
            def updater(*args, **kwargs):
                return r()(*args, **kwargs)
            
            self.__updater = updater
            
        return self.__updater
            
    def __del__(self):
        ''' Cleanup any existing subscriptions.
        '''
        # Suppress unsubs atexit, because loops will be closed, inducing errors
        global is_exiting
        if not is_exiting:
            # This relies on the indempotent nature of unsubscribe
            try:
                self._core.persister.unsubscribe(self.ghid, self._weak_touch)
            except Exception as exc:
                logger.error(
                    'Error while cleaning up _GAO:\n' + repr(exc) + '\n' + 
                    ''.join(traceback.format_tb(exc.__traceback__))
                )
            
            
class _GAOMsgpackBase(_GAO):
    ''' Golix-aware messagepack base object.
    '''
    def __init__(self, *args, **kwargs):
        # Include these so that we can pass *args and **kwargs to the dict
        super().__init__(*args, **kwargs)
        # TODO: Convert this to a ComboLock (threadsafe and asyncsafe)
        # Note: must be RLock, because we need to take opslock in __setitem__
        # while calling push.
        self._opslock = threading.Lock()
        
    def __eq__(self, other):
        ''' Check total equality first, and then fall back on state 
        checking.
        '''
        equal = True
        try:
            equal &= (self.dynamic == other.dynamic)
            equal &= (self.ghid == other.ghid)
            equal &= (self.author == other.author)
            equal &= (self._state == other._state)
        except AttributeError:
            equal &= (self._state == other)
        
        return equal
        
    def pull(self, *args, **kwargs):
        with self._opslock:
            super().pull(*args, **kwargs)
        
    def push(self, *args, **kwargs):
        with self._opslock:
            super().push(*args, **kwargs)
        
    @staticmethod
    def _msgpack_ext_pack(obj):
        ''' Shitty hack to make msgpack work with ghids.
        '''
        if isinstance(obj, Ghid):
            return msgpack.ExtType(0, bytes(obj))
        else:
            raise TypeError('Not a ghid.')
        
    @staticmethod
    def _msgpack_ext_unpack(code, packed):
        ''' Shitty hack to make msgpack work with ghids.
        '''
        if code == 0:
            return Ghid.from_bytes(packed)
        else:
            return msgpack.ExtType(code, data)
        
    @classmethod
    def _pack(cls, state):
        ''' Packs state into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        '''
        try:
            return msgpack.packb(
                state, 
                use_bin_type = True, 
                default = cls._msgpack_ext_pack
            )
            
        except (msgpack.exceptions.BufferFull,
                msgpack.exceptions.ExtraData,
                msgpack.exceptions.OutOfData,
                msgpack.exceptions.PackException,
                msgpack.exceptions.PackValueError) as exc:
            raise ValueError(
                'Failed to pack _GAODict. Incompatible nested object?'
            ) from exc
        
    @classmethod
    def _unpack(cls, packed):
        ''' Unpacks state from a bytes object. May be overwritten in 
        subs to unpack more complex objects. Should always be a 
        staticmethod or classmethod.
        '''
        try:
            return msgpack.unpackb(
                packed, 
                encoding = 'utf-8',
                ext_hook = cls._msgpack_ext_unpack
            )
            
        # MsgPack errors mean that we don't have a properly formatted handshake
        except (msgpack.exceptions.BufferFull,
                msgpack.exceptions.ExtraData,
                msgpack.exceptions.OutOfData,
                msgpack.exceptions.UnpackException,
                msgpack.exceptions.UnpackValueError) as exc:
            raise ValueError(
                'Failed to unpack _GAODict. Incompatible serialization?'
            ) from exc
            
            
class _GAODict(_GAOMsgpackBase):
    ''' A golix-aware dictionary. For now at least, serializes:
            1. For every change
            2. Using msgpack
    '''
    def __init__(self, core, dynamic, _legroom=None, *args, **kwargs):
        # Include these so that we can pass *args and **kwargs to the dict
        super().__init__(core, dynamic, _legroom)
        self._state = dict(*args, **kwargs)
        self._statelock = threading.Lock()
        
    def apply_state(self, state):
        ''' Apply the UNPACKED state to self.
        '''
        with self._statelock:
            self._state.clear()
            self._state.update(state)
        
    def extract_state(self):
        ''' Extract self into a packable state.
        '''
        # with self._statelock:
        # Both push and pull take the opslock, and they are the only entry 
        # points that call extract_state and apply_state, so we should be good
        # without the lock.
        return self._state
        
    def __len__(self):
        # Straight pass-through
        return len(self._state)
        
    def __iter__(self):
        for key in self._state:
            yield key
            
    def __getitem__(self, key):
        with self._statelock:
            return self._state[key]
            
    def __setitem__(self, key, value):
        with self._statelock:
            self._state[key] = value
            self.push()
            
    def __delitem__(self, key):
        with self._statelock:
            del self._state[key]
            self.push()
            
    def __contains__(self, key):
        with self._statelock:
            return key in self._state
            
    def pop(self, key, *args, **kwargs):
        with self._statelock:
            result = self._state.pop(key, *args, **kwargs)
            self.push()
            
        return result
        
    def items(self, *args, **kwargs):
        # Because the return is a view object, competing use will result in
        # python errors, so we don't really need to worry about statelock.
        return self._state.items(*args, **kwargs)
        
    def keys(self, *args, **kwargs):
        # Because the return is a view object, competing use will result in
        # python errors, so we don't really need to worry about statelock.
        return self._state.keys(*args, **kwargs)
        
    def values(self, *args, **kwargs):
        # Because the return is a view object, competing use will result in
        # python errors, so we don't really need to worry about statelock.
        return self._state_values(*args, **kwargs)
        
    def setdefault(self, key, *args, **kwargs):
        ''' Careful, need state lock.
        '''
        with self._statelock:
            if key in self._state:
                result = self._state.setdefault(key, *args, **kwargs)
            else:
                result = self._state.setdefault(key, *args, **kwargs)
                self.push()
        
        return result
        
    def get(self, *args, **kwargs):
        with self._statelock:
            return self._state.get(*args, **kwargs)
        
    def popitem(self, *args, **kwargs):
        with self._statelock:
            result = self._state.popitem(*args, **kwargs)
            self.push()
        return result
        
    def clear(self, *args, **kwargs):
        with self._statelock:
            self._state.clear(*args, **kwargs)
            self.push()
        
    def update(self, *args, **kwargs):
        with self._statelock:
            result = self._state.update(*args, **kwargs)
            self.push()
        return result
            
            
class _GAOSet(_GAOMsgpackBase):
    ''' A golix-aware set. For now at least, serializes:
            1. For every change
            2. Using msgpack
    '''
    def __init__(self, core, dynamic, _legroom=None, *args, **kwargs):
        # Include these so that we can pass *args and **kwargs to the set
        super().__init__(core, dynamic, _legroom)
        self._state = set(*args, **kwargs)
        self._statelock = threading.Lock()
        
    def apply_state(self, state):
        ''' Apply the UNPACKED state to self.
        '''
        with self._statelock:
            to_add = state - self._state
            to_remove = self._state - state
            self._state -= to_remove
            self._state |= to_add
        
    def extract_state(self):
        ''' Extract self into a packable state.
        '''
        # with self._statelock:
        # Both push and pull take the opslock, and they are the only entry 
        # points that call extract_state and apply_state, so we should be good
        # without the lock.
        return self._state
            
    def __contains__(self, key):
        with self._statelock:
            return key in self._state
        
    def __len__(self):
        # Straight pass-through
        return len(self._state)
        
    def __iter__(self):
        for key in self._state:
            yield key
            
    def add(self, elem):
        ''' Do a wee bit of checking while we're at it to avoid 
        superfluous pushing.
        '''
        with self._statelock:
            if elem not in self._state:
                self._state.add(elem)
                self.push()
                
    def remove(self, elem):
        ''' The usual. No need to check; will keyerror before pushing if
        no-op.
        '''
        with self._statelock:
            self._state.remove(elem)
            self.push()
            
    def discard(self, elem):
        ''' Do need to check for modification to prevent superfluous 
        pushing.
        '''
        with self._statelock:
            if elem in self._state:
                self._state.discard(elem)
                self.push()
            
    def pop(self):
        with self._statelock:
            result = self._state.pop()
            self.push()
            
        return result
            
    def clear(self):
        with self._statelock:
            self._state.clear()
            self.push()
            
    def isdisjoint(self, other):
        with self._statelock:
            return self._state.isdisjoint(other)
            
    def issubset(self, other):
        with self._statelock:
            return self._state.issubset(other)
            
    def issuperset(self, other):
        with self._statelock:
            return self._state.issuperset(other)
            
    def union(self, *others):
        # Note that union creates a NEW set.
        with self._statelock:
            return type(self)(
                self._core, 
                self._dynamic, 
                self._legroom, 
                self._state.union(*others)
            )
            
    def intersection(self, *others):
        # Note that intersection creates a NEW set.
        with self._statelock:
            return type(self)(
                self._core, 
                self._dynamic, 
                self._legroom, 
                self._state.intersection(*others)
            )
        
    @classmethod
    def _pack(cls, state):
        ''' Wait, fucking seriously? Msgpack can't do sets?
        '''
        return super()._pack(list(state))
        
    @classmethod
    def _unpack(cls, packed):
        ''' Wait, fucking seriously? Msgpack can't do sets?
        '''
        return set(super()._unpack(packed))