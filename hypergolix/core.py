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
    'GolixCore', 
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
# import atexit

from golix import SecondParty
from golix import Ghid
from golix import Secret
from golix import SecurityError

from golix._getlow import GEOC
from golix._getlow import GOBD
from golix._getlow import GARQ
from golix._getlow import GDXX

# Intra-package dependencies
from .exceptions import RemoteNak
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


class GhidProxier:
    ''' Resolve the base container GHID from any associated ghid. Uses
    all weak references, so should not interfere with GCing objects.
    
    Threadsafe.
    '''
    def __init__(self):
        # The usual.
        self._refs = {}
        self._modlock = threading.Lock()
        
        self._librarian = None
        self._core = None
        
    def assemble(self, golix_core, librarian):
        # Chicken, meet egg.
        self._core = weakref.proxy(core)
        self._librarian = weakref.proxy(librarian)
        
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
            return self._resolve(ghid)
        
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
            obj = self._librarian.summarize_loud(ghid)
            
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
    
    def __init__(self):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        
        persister isinstance _PersisterBase
        dispatcher isinstance DispatcherBase
        _identity isinstance golix.FirstParty
        '''
        self._opslock = threading.Lock()
        
        # Added during bootstrap
        self._identity = None
        # Added during assembly
        self._librarian = None
        
    def assemble(self, librarian):
        # Chicken, meet egg.
        self._librarian = weakref.proxy(librarian)
        
    def bootstrap(self, identity):
        # This must be done ASAGDFP. Must be absolute first thing to bootstrap.
        self._identity = identity
        
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
            self._librarian.retrieve_loud(unpacked.author)
        )
        payload = self._identity.receive_request(requestor, unpacked)
        return payload
        
    def make_request(self, recipient, payload):
        # Just like it says on the label...
        recipient = SecondParty.from_packed(
            self._librarian.retrieve_loud(recipient)
        )
        with self._opslock:
            return self._identity.make_request(
                recipient = recipient,
                request = payload,
            )
        
    def open_container(self, container, secret):
        # Wrapper around golix.FirstParty.receive_container.
        author = SecondParty.from_packed(
            self._librarian.retrieve_loud(container.author)
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
        ''' Make a new dynamic binding frame.
        If supplied, ghid is the dynamic address, and history is an 
        ordered iterable of the previous frame ghids.
        '''
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
                ghid_dynamic = ghid,
                history = history
            )
        
    def make_debinding(self, target):
        # Simple wrapper around golix.FirstParty.make_debind
        with self._opslock:
            return self._identity.make_debind(target)
        
        
class Oracle:
    ''' Source for total internal truth and state tracking of objects.
    
    Maintains <ghid>: <obj> lookup. Used by dispatchers to track obj
    state. Might eventually be used by AgentBase. Just a quick way to 
    store and retrieve any objects based on an associated ghid.
    '''
    def __init__(self):
        ''' Sets up internal tracking.
        '''
        self._opslock = threading.Lock()
        self._lookup = {}
        
        self._core = None
        self._salmonator = None
        
    def assemble(self, golix_core, salmonator):
        # Chicken, meet egg.
        self._core = weakref.proxy(core)
        self._salmonator = weakref.proxy(salmonator)
            
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
                
                if obj.dynamic:
                    self._salmonator.register(obj)
                
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
            self._salmonator.register(obj, skip_refresh=True)
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
        
        def weak_touch(*args, **kwargs):
            ''' This is a smart way of avoiding issues with weakmethods
            in WeakSetMaps. It allows the GAO to be GC'd as appropriate,
            without causing problems in the Postman subscription system.
            
            Because Postman is using WeakSetMap, this also means we 
            don't need to build a __del__ system to unsubscribe, nor do
            suppression thereof at system exit. This is much, much nicer 
            than our previous strategy.
            '''
            # Note that we now have a closure around self.
            self.touch(*args, **kwargs)
        self._weak_touch = weak_touch
        
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
            # TODO: fix leaky abstraction
            bindings = self._persister.bookie.bind_status()
            
            for binding in bindings:
                # TODO: fix leaky abstraction
                obj = self._persister.librarian.summarize_loud(binding)
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
        
        # TODO: fix leaky abstraction
        packed = persister.librarian.retrieve_loud(container_ghid)
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
            binding = persister.librarian.summarize_loud(ghid)
            self._history.extend(binding.history)
            self._history.appendleft(binding.frame_ghid)
            self._history_targets.appendleft(binding.target)
            # TODO: fix leaky abstraction
            persister.postman.register(self)
            
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
        check_obj = self._persister.librarian.summarize_loud(check_address)
        
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
            packed = self._persister.librarian.retrieve_loud(check_obj.target)
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
            self.ghid = binding.ghid_dynamic
            self.author = self._core.whoami
            # Silence the **frame address** (see above) and add it to historian
            self.silence(binding.ghid)
            self._history.appendleft(binding.ghid)
            self._history_targets.appendleft(container.ghid)
            # Now assign this dynamic address as a chain owner.
            self._privateer.make_chain(address, container.ghid)
            # Finally, publish the binding and subscribe to it
            self._persister.ingest_gobd(binding)
            self._persister.postman.register(self)
            
        else:
            binding = self._core.make_binding_stat(target=container.ghid)
            self.ghid = container.ghid
            self.author = self._core.whoami
            self._persister.ingest_gobs(binding)
            
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
            packed = self._persister.librarian.retrieve_loud(container_ghid)
            packed_state = self._attempt_open_container(
                self._core, 
                self._privateer, 
                secret, 
                packed
            )
            
            # TODO: fix these leaky abstractions.
            self.apply_state(self._unpack(packed_state))
            binding = self._persister.librarian.summarize_loud(ghid)
            # Don't forget to fix history as well
            self._advance_history(binding)
            
            raise
            
        finally:
            # Resume accepting updates.
            self._update_greenlight.set()
            
            
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