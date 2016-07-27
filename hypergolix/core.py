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

from golix import FirstParty
from golix import SecondParty
from golix import Ghid
from golix import Secret
from golix import SecurityError

from golix._getlow import GEOC
from golix._getlow import GOBD
from golix._getlow import GARQ
from golix._getlow import GDXX

from golix.utils import AsymHandshake
from golix.utils import AsymAck
from golix.utils import AsymNak

# Intra-package dependencies
from .exceptions import NakError
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning
from .exceptions import UnknownParty

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


class HGXCore:
    ''' Base class for all Agents.
    '''
    
    DEFAULT_LEGROOM = 3
    
    def __init__(self, persister, oracle, _identity=None, *args, **kwargs):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        
        persister isinstance _PersisterBase
        dispatcher isinstance DispatcherBase
        _identity isinstance golix.FirstParty
        '''
        super().__init__(*args, **kwargs)
        
        # if not isinstance(persister, _PersisterBase):
        #     raise TypeError('Persister must subclass _PersisterBase.')
        self._persister = persister
        self._oracle = oracle
        
        self._ghidproxy = _GhidProxier()
        # self._privateer = privateer
        # self._privateer = _Privateer()
        
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
        
    def link_privateer(self, privateer):
        ''' Chicken vs egg. Should be called ASAGDFP after __init__.
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
        self._dispatcher = dispatch
        
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
            self.dispatcher.dispatch_handshake(request.target)
            
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
        self.dispatcher.dispatch_handshake_ack(request.target)
            
    def _handle_req_nak(self, request, source_ghid):
        ''' Handles a handshake nak after reception.
        '''
        self.dispatcher.dispatch_handshake_nak(request.target)
        
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
        
    def touch_dynamic(self, ghid):
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
        unpacked_binding = self._identity.unpack_bind_dynamic(
            self.persister.get(ghid)
        )
        # Should be fixed, as per above.
        # ghid = unpacked_binding.ghid_dynamic
        # # Goddammit. It is. Why the hell are we passing frame_ghid as ghid?!
        
        # First, make sure we're not being asked to update something we 
        # initiated the update for.
        # TODO: fix leaky abstraction
        obj = self._oracle[ghid]
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
        self._dispatcher.await_handshake_response(target, request.ghid)
        
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
        self._core = core
        self._lookup = {}
        
    def __getitem__(self, ghid):
        ''' Returns the raw data associated with the ghid. If the ghid
        is unavailable, attempts to retrieve it from core.
        '''
        # Note: should probably add some kind of explicit cache refresh 
        # mechanism, in the event that connections or updates are dropped.
        return self._lookup[ghid]
            
    def get_object(self, gaoclass, ghid, *args, **kwargs):
        obj = gaoclass.from_ghid(
            core = self._core, 
            ghid = ghid, 
            *args, **kwargs
        )
        self._lookup[ghid] = obj
        return obj
        
    def new_object(self, gaoclass, *args, **kwargs):
        ''' Creates a new object and returns it. Passes all *args and
        **kwargs to the declared gao_class. Eliminates the need to pass
        core, or call push.
        '''
        obj = gaoclass(core=self._core, *args, **kwargs)
        obj.push()
        self._lookup[obj.ghid] = obj
        return obj
        
    def forget(self, ghid):
        ''' Removes the object from the cache. Next time an application
        wants it, it will need to be acquired from persisters.
        
        Indempotent; will not raise KeyError if called more than once.
        '''
        try:
            del self._lookup[ghid]
        except KeyError:
            pass
            
    def __contains__(self, ghid):
        ''' Very quick proxy to self._lookup. Does not check for global
        access; that's mostly up to privateer.
        '''
        return ghid in self._lookup
    
    
class _GAO(metaclass=abc.ABCMeta):
    ''' Base class for Golix-Aware Objects (Golix Accountability 
    Objects?). Anyways, used by core to handle plaintexts and things.
    
    TODO: thread safety? or async safety?
    '''
    def __init__(self, core, dynamic, _legroom=None, *args, **kwargs):
        ''' Creates a golix-aware object. If ghid is passed, will 
        immediately pull from core to sync with existing object. If not,
        will create new object on first push. If ghid is not None, then
        dynamic will be ignored.
        '''
        self.__opslock = threading.RLock()
        self._core = core
        self.dynamic = bool(dynamic)
        self.ghid = None
        self.author = None
        self.__updater = None
        # self._frame_ghid = None
        
        if _legroom is None:
            _legroom = core._legroom
        self._legroom = _legroom
        
        # This probably only needs to be maxlen=2, but it's a pretty negligible
        # footprint to add some headspace
        self._silenced = collections.deque(maxlen=3)
        self._update_greenlight = threading.Event()
        
        super().__init__(*args, **kwargs)
        
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
            
    @classmethod
    def from_ghid(cls, core, ghid, _legroom=None, *args, **kwargs):
        ''' Loads the GAO from an existing ghid.
        '''
        author, dynamic, packed_state = core.get_ghid(ghid)
        # Awwwwkward
        # TODO: change
        args1, kwargs1 = cls._init_unpack(packed_state)
        args = list(args)
        args.extend(args1)
        kwargs.update(kwargs1)
        self = cls(
            core = core, 
            dynamic = dynamic, 
            _legroom = _legroom, 
            *args, **kwargs
        )
        
        self.ghid = ghid
        self.author = author
        
        if dynamic:
            core.persister.subscribe(ghid, self._weak_touch)
            
        # DON'T FORGET TO SET THIS!
        self._update_greenlight.set()
        
        return self
        
    @staticmethod
    def _init_unpack(packed):
        ''' Unpacks an initial state in from_ghid into *args, **kwargs.
        Should always be staticmethod or classmethod.
        
        TODO: make this less awkward.
        '''
        return tuple(), {}
        
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
    
    def push(self):
        ''' Pushes updates to upstream. Must be called for every object
        mutation.
        '''
        with self.__opslock:
            if self.ghid is None:
                self.__new()
            else:
                if self.dynamic:
                    self.__update()
                else:
                    raise TypeError('Static GAOs cannot be updated.')
        
    def pull(self):
        ''' Refreshes self from upstream. Should NOT be called at object 
        instantiation for any existing objects. Should instead be called
        directly, or through _weak_touch for any new status.
        '''
        # Note that, when used as a subs handler, we'll be passed our own 
        # self.ghid (as the subscription ghid) as well as the ghid for the new
        # frame (as the notification ghid)
        
        with self.__opslock:
            if self.dynamic:
                # State will be None if no update was applied.
                packed_state = self._core.touch_dynamic(self.ghid)
                if packed_state:
                    # Don't forget to extract...
                    self.apply_state(
                        self._unpack(packed_state)
                    )
                    modified = True
                else:
                    modified = False
            else:
                modified = False
        
        return modified
        
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
            self.pull()
        
    def __new(self):
        ''' Creates a new Golix object for self using self._state, 
        self._dynamic, etc.
        '''
        if self.dynamic:
            # Need both of these so we can silence the initial frame.
            address, frame_address = self._core.new_dynamic(
                state = self._pack(
                    self.extract_state()
                ),
                _legroom = self._legroom,
            )
            # Silence any updates about this initial frame address.
            self.silence(frame_address)
            # Finally, subscribe to updates.
            self._core.persister.subscribe(address, self._weak_touch)
        else:
            address = self._core.new_static(
                state = self._pack(
                    self.extract_state()
                )
            )
        
        self.author = self._core.whoami
        self.ghid = address
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
            frame_ghid = self._core.update_dynamic(
                ghid_dynamic = self.ghid,
                state = self._pack(
                    self.extract_state()
                ),
            )
            # Make sure to never call an update for this frame.
            self.silence(frame_ghid)
            
        except:
            author, dynamic, packed_state = self._core.get_ghid(self.ghid)
            state = self._unpack(packed_state)
            self.apply_state(state)
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
    def _init_unpack(cls, packed):
        ''' Unpacks an initial state in from_ghid into *args, **kwargs.
        Should always be staticmethod or classmethod.
        '''
        dispatchablestate = cls._unpack(packed)
        return (dispatchablestate,), {}
        
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