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
'''


# Global dependencies
import logging
import asyncio
import traceback
import collections
import functools
import weakref
import pickle
# Used to make random ghids for fixturing gao
import random

from golix import Ghid
from golix import SecurityError
from loopa.utils import await_coroutine_threadsafe
from loopa.utils import await_coroutine_loopsafe
from loopa.utils import make_background_future

# Local dependencies
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api
from .hypothetical import fixture_noop

from .utils import SetMap
from .utils import WeakSetMap
from .utils import ApiID
from .utils import _reap_wrapped_task
from .utils import weak_property
from .utils import readonly_property
from .utils import immortal_property
from .utils import immutable_property

from .exceptions import HGXLinkError
from .exceptions import DeadObject
from .exceptions import RatchetError
from .exceptions import UnknownSecret
from .exceptions import UnrecoverableState

from .persistence import _GobdLite
from .persistence import _GdxxLite
from .persistence import _GobsLite
from .persistence import _GeocLite

# from .objproxy import ObjBase
# from .objproxy import ObjCore


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports.
__all__ = [
    # 'PersistenceCore',
]


# ###############################################
# Utils
# ###############################################


PROXY_CORO = object()
PROXY_FUNC = object()
MUTATING_PROXY_CORO = object()
MUTATING_PROXY_FUNC = object()


class Accountable(API):
    ''' Use this metaclass to construct GAOs that use deferred-action
    methods that can store deltas before flushing. To be used in account
    GAOs to avoid shitloads of extra upstream pushing.
    
    Some thoughts:
    +   Initial support should simply be a "mutated" flag, which will
        ease up some performance concerns surrounding flushing the
        internal account GAOs
    +   From there, should transition into tracking the actual deltas
        themselves to remove contention problems
    +   Will probably need to be implemented using a read-only copy of
        the object -- potentially with chainmap or something? But if
        you're using auto-generation via @mutating decorator, it's going
        to be particularly difficult to do this unless you use a full
        copy of the overall thing, instead of just storing the deltas
    +   Could potentially also store the deltas along with a single
        copy of the thing itself, and then re-apply the deltas after
        every pull (like a rebase)
    +   That last one is probably the smartest way to do it
    
    TODO: support this. Currently, it's just one big race condition
    waiting for contention problems.
    '''
    
    def __new__(mcls, clsname, bases, namespace, *args, **kwargs):
        ''' Modify the existing namespace. Look for anything with a
        "__is_proxied__" attribute, and use it to create a proxy.
        '''
        new_namespace = {}
        for name, obj in namespace.items():
            if obj is PROXY_CORO:
                async def prox(self, *args, __proxname=name, **kwargs):
                    proxied = getattr(self.state, __proxname)
                    return (await proxied(*args, **kwargs))
                
                prox.__name__ = name
                new_namespace[name] = prox
                
            elif obj is PROXY_FUNC:
                def prox(self, *args, __proxname=name, **kwargs):
                    proxied = getattr(self.state, __proxname)
                    return proxied(*args, **kwargs)
                        
                prox.__name__ = name
                new_namespace[name] = prox
                
            elif obj is MUTATING_PROXY_CORO:
                async def prox(self, *args, __proxname=name, **kwargs):
                    self._mutated = True
                    proxied = getattr(self.state, __proxname)
                    return (await proxied(*args, **kwargs))
                
                prox.__name__ = name
                new_namespace[name] = prox
                
            elif obj is MUTATING_PROXY_FUNC:
                def prox(self, *args, __proxname=name, **kwargs):
                    self._mutated = True
                    proxied = getattr(self.state, __proxname)
                    return proxied(*args, **kwargs)
                        
                prox.__name__ = name
                new_namespace[name] = prox
                
            else:
                new_namespace[name] = obj
        
        cls = super().__new__(mcls, clsname, bases, new_namespace, *args,
                              **kwargs)
        
        async def _push(self, *args, force=False, **kwargs):
            ''' Only push if we actually changed.
            '''
            if force or self.ghid is None or self._mutated:
                # Because of the order here, we don't much need to worry about
                # a race condition. Any subsequent mutating calls will
                # correctly mark the gao as mutated, allowing the next push to
                # continue. Though, depending on timing, that may result in
                # subsequent pushes having an extra, redundant push (if it was
                # incorporated into the old _push in time).
                self._mutated = False
                return (await super(cls, self)._push(*args, **kwargs))
            
            else:
                return
                
        cls._mutated = False
        cls._push = _push
        
        return cls


def mutating(func):
    ''' Decorator to mark a method as mutating, for use in delta
    tracking.
    '''
    try:
        func.__deferred__ = True
    
    except AttributeError:
        @functools.wraps(func)
        def func(*args, _func=func, **kwargs):
            _func(*args, **kwargs)
        
        func.__deferred__ = True
    
    return func
    
    
class DefermentTracker:
    ''' Tracks all deferred objects, and allows for a single flush()
    call to push all changes.
    '''
    
    def __init__(self):
        # Create a set of all tracked objects.
        self.tracked = weakref.WeakSet()
        
    def add(self, gao):
        ''' Track a gao.
        '''
        self.tracked.add(gao)
        
    def remove(self, gao):
        ''' Remove a gao.
        '''
        self.tracked.remove(gao)
        
    async def flush(self):
        ''' Push all of the tracked GAOs. Currently dumb (in that it
        does not check for modifications, and simply always pushes.)
        '''
        tasks = set()
        for gao in self.tracked:
            tasks.add(make_background_future(gao.push()))
            
        # And wait for them all to complete. Note that make_background_future
        # handles their exception and result handling.
        await asyncio.wait(
            fs = tasks,
            return_when = asyncio.ALL_COMPLETED
        )


# ###############################################
# Lib
# ###############################################


class GAOCore(metaclass=API):
    ''' Adds functions to a GAO.
    
    Notes:
    1.  GAO are only ever created through the oracle, via either
        oracle.new_object or oracle.get_object.
    2.  Note that callbacks are only used when defined in subclasses.
        IE, Dispatchable extends pull() to notify applications of the
        update (if applicable)
    3.  Updates for internal-use GAO should defer until a flush() call
    4.  Updates for dispatchables should immediately be pushed
    5.  Salmonator pushing should probably wait to return until all
        remotes have been contacted
    6.  Internal GAOs should probably have a delta-tracking mechanism
        for conflict resolution
    7.  Dispatchables should rely upon immediate pushing, and then raise
        immediately for any conflicts
    '''
    
    # Default a few things to prevent attributeerrors
    _legroom = None
    _target_history = tuple()
    
    # Make weak properties for the various thingajobbers
    _golcore = weak_property('__golcore')
    _ghidproxy = weak_property('__ghidproxy')
    _privateer = weak_property('__privateer')
    _percore = weak_property('__percore')
    _librarian = weak_property('__librarian')
    
    # Make readonly properties for dynamic, ghid, and author. Note that the
    # author field is technically deprecated and pending removal, but is
    # currently used to infer the binder for applications.
    dynamic = readonly_property('_dynamic')
    author = readonly_property('_author')
    # This is immutable, so that it may be set just after initting.
    ghid = immutable_property('_ghid')
    _master_secret = readonly_property('__msec')
    
    # Also, an un-deletable property for history (even if unused), etc
    isalive = immortal_property('_isalive')
    # Most recent TARGET ghids
    target_history = immortal_property('_target_history')
    _counter = immortal_property('__counter')
    
    @property
    def legroom(self):
        ''' Do the thing with the thing.
        '''
        return self._legroom
        
    @legroom.setter
    def legroom(self, value):
        ''' Cast value as int and then assign it both internally and to
        the two history deques.
        '''
        legroom = int(value)
        
        if legroom != self._legroom:
            self._legroom = legroom
            # We actually have to create new deques for this, because you
            # cannot resize existing ones.
            self.target_history = collections.deque(self.target_history,
                                                    maxlen=legroom)
            
    def _inject_msec(self, msec):
        ''' Forcibly update the master secret. ONLY call this with
        extreme foresight. Intended only for use when bootstrapping the
        privateer itself.
        '''
        setattr(self, '__msec', msec)
            
    @property
    def _local_secret(self):
        ''' This determines if the secret will be committed locally only
        (for master_secreted objects), or if it will be stored
        persistently for the account.
        '''
        return bool(self._master_secret)
    
    def __init__(self, ghid, dynamic, author, legroom, *args, golcore,
                 ghidproxy, privateer, percore, librarian, master_secret=None,
                 **kwargs):
        ''' Init should be used only to create a representation of an
        EXISTING (or about to be existing) object. If any fields are
        unknown, they must be explicitly passed None.
        
        If master_secret is None, ratcheting will be iterative.
        If master_secret is a Secret, ratcheting will use the primary
            bootstrap ratcheting mechanism.
        '''
        super().__init__(*args, **kwargs)
        
        # This is an init. It will be overwritten on either a push or a pull.
        setattr(self, '__counter', None)
        
        self._golcore = golcore
        self._ghidproxy = ghidproxy
        self._privateer = privateer
        self._percore = percore
        self._librarian = librarian
        
        # Dynamic and author be explicitly None; ghid should always be defined
        self.ghid = ghid
        self._dynamic = dynamic
        self._author = author
        
        # Note that this also initializes target_history
        self.legroom = legroom
        self.isalive = True
        
        # This determines our ratcheting behavior. Set it with setattr to
        # bypass both name mangling AND the readonly property.
        setattr(self, '__msec', master_secret)
        
        # We will only ever be created from within an event loop, so this is
        # fine to do without an explicit loop.
        self._update_lock = asyncio.Lock()
        
    @fixture_noop
    @public_api
    async def apply_delete(self, debinding):
        ''' Executes an external delete, caused by the debinding at the
        passed address. By default, just makes sure the debinding target
        is valid.
        
        Debinding is a _GdxxLite.
        
        Note that this gets used by Dispatchable to dispatch deletion to
        applications.
        '''
        if self.dynamic:
            if debinding.target != self.ghid:
                raise ValueError(
                    'Debinding target does not match GAO applying delete.'
                )
        else:
            debinding_target = \
                await self._librarian.summarize(debinding.target)
            if debinding_target.target != self.ghid:
                raise ValueError(
                    'Debinding target does not match GAO applying delete.'
                )
                
        self.isalive = False
            
    @public_api
    async def freeze(self):
        ''' Creates a static binding for the most current state of a
        dynamic binding. Returns the frozen ghid.
        '''
        if not self.dynamic:
            raise TypeError('Cannot freeze a static GAO.')
            
        container_ghid = await self._ghidproxy.resolve(self.ghid)
        binding = await self._golcore.make_binding_stat(container_ghid)
        await self._percore.direct_ingest(
            obj = _GobsLite.from_golix(binding),
            packed = binding.packed,
            remotable = True
        )
        
        return container_ghid
        
    @freeze.fixture
    async def freeze(self):
        ''' Just return a pseudorandom ghid.
        '''
        return Ghid.from_bytes(
            b'\x01' + bytes([random.randint(0, 255) for i in range(0, 64)])
        )
        
    @fixture_noop
    @public_api
    async def hold(self):
        ''' Make a static binding for the gao.
        Note that, since third parties can bind arbitrary objects, there
        isn't an easy way of checking for an existing static binding for
        this specific ghid.
        '''
        binding = await self._golcore.make_binding_stat(self.ghid)
        await self._percore.direct_ingest(
            obj = _GobsLite.from_golix(binding),
            packed = binding.packed,
            remotable = True
        )
        
    @fixture_noop
    @public_api
    async def delete(self):
        ''' Permanently removes the object. This method is only called
        locally; upstream deletes should call self.apply_delete.
        Incidentally, apply_delete will eventually get called through
        this. Which means (for the record) that the deleter will also
        get a notification of the delete (iff it was actually successful
        in removing the object).
        '''
        if self.dynamic:
            debinding = await self._golcore.make_debinding(self.ghid)
            await self._percore.direct_ingest(
                obj = _GdxxLite.from_golix(debinding),
                packed = debinding.packed,
                remotable = True
            )
            
        else:
            # Get frozenset of binding ghids
            bindings = await self._librarian.bind_status(self.ghid)
            
            for binding in bindings:
                obj = await self._librarian.summarize(binding)
                if isinstance(obj, _GobsLite):
                    if obj.author == self._golcore.whoami:
                        debinding = await self._golcore.make_debinding(
                            obj.ghid
                        )
                        await self._percore.direct_ingest(
                            obj = _GdxxLite.from_golix(debinding),
                            packed = debinding.packed,
                            remotable = True
                        )
            
    def _get_new_secret(self):
        ''' Gets a new secret for self.
        '''
        # If we're getting a new secret and we already have a ghid, then we
        # MUST be a dynamic object.
        if self.ghid:
            current_target = self.target_history[0]
            secret = self._privateer.ratchet_chain(
                self.ghid,
                current_target,
                self._master_secret
            )
        
        # This is a new object, and we therefore don't have a secret. Note that
        # the first secret for a bootstrapped chain will always be discarded,
        # because the salt process needs to start somewhere.
        else:
            secret = self._privateer.new_secret()
            
        return secret
    
    @public_api
    async def push(self):
        ''' Pushes the state upstream. Must be called explicitly. Unless
        push() is invoked, the state will not be synchronized.
        '''
        if not self.isalive:
            raise DeadObject('Cannot push a deleted object. Create a new one.')
        
        # We're alive. Don't do object creation here; do it in oracle. This is
        # strictly updating.
        elif self.dynamic:
            # We need to make sure we're not pushing and pulling at the same
            # time.
            async with self._update_lock:
                try:
                    await self._push()
                
                # TODO: narrow which exceptions can cause this.
                except Exception:
                    logger.error(''.join((
                        'GAO ',
                        str(self.ghid),
                        ' PUSH: failed to update object; forcibly restoring ',
                        'state w/ traceback:\n',
                        traceback.format_exc()
                    )))
                    # We had a problem, so we're going to forcibly restore the
                    # object to the last known good state. Use _pull to avoid
                    # the lock, as well as any extra baggage in "normal" pull
                    await self._pull()
                    raise
            
        else:
            raise TypeError('Static objects cannot be updated.')
            
    @push.fixture
    async def push(self):
        ''' For fixture, just pass to _push directly.
        '''
        await self._push()
    
    @public_api
    async def _push(self):
        ''' The actual "meat and bones" for pushing.
        '''
        secret = self._get_new_secret()
        packed = await self.pack_gao()
        container = await self._golcore.make_container(packed, secret)
        
        # Dynamic object
        if self.dynamic:
            binding = await self._golcore.make_binding_dyn(
                target = container.ghid,
                ghid = self.ghid,
                history = self.target_history
            )
            binding_obj = _GobdLite.from_golix(binding)
            counter = binding.counter
            reference_ghid = binding.ghid_dynamic
            
        # Static object.
        else:
            binding = await self._golcore.make_binding_stat(container.ghid)
            binding_obj = _GobsLite.from_golix(binding)
            counter = 0
            reference_ghid = container.ghid
        
        # Okay, up until now, we haven't changed any state. But now we need to
        # update state, which needs to be atomic-ish, so we're going to do some
        # funny business here.
        
        # If this gets cancelled, we don't need to worry about anything.
        await self._percore.direct_ingest(
            obj = binding_obj,
            packed = binding.packed,
            remotable = True
        )
        
        # But if we get here, we've changed state, so we need to protect stuff.
        try:
            # It we ingested the binding, we *must* also ingest the container
            # (if **at all** possible), or the binding will effectively be
            # unrecoverable
            container_ingester = asyncio.ensure_future(
                self._percore.direct_ingest(
                    obj = _GeocLite.from_golix(container),
                    packed = container.packed,
                    remotable = True
                )
            )
        
            await asyncio.shield(container_ingester)
        
        # That container ingestion is the only remaining async call within this
        # coro, so this is the only thing we need to defer cancellation on.
        # While we're at it, defer any other exceptions, too, though they will
        # likely break the object anyways.
        except Exception as exc:
            deferred_raise = exc
            
        else:
            deferred_raise = None
            
        # We created a container and binding frame, and then we successfully
        # uploaded them. Update our frame and target history accordingly and
        # commit the container secret.
        self._privateer.stage(container.ghid, secret)
        self._privateer.commit(container.ghid, localize=self._local_secret)
        # NOTE THE DISCREPANCY between the Golix dynamic binding version
        # of ghid and ours! This is recording the frame ghid.
        # I mean, for static stuff this isn't (strictly speaking) relevant,
        # but it also doesn't really hurt anything, sooo...
        self.target_history.appendleft(container.ghid)
        self._counter = counter
        
        # Do this last!
        # Update ghid if it was not defined (new object)
        self._conditional_init(
            reference_ghid,
            False,
            self._golcore.whoami
        )
        
        if deferred_raise is not None:
            raise deferred_raise
            
    @_push.fixture
    async def _push(self):
        ''' Do a conditional init if we haven't had stuff fully defined.
        '''
        self._conditional_init(
            ghid = Ghid.from_bytes(
                b'\x01' + bytes([random.randint(0, 255) for i in range(0, 64)])
            ),
            author = Ghid.from_bytes(
                b'\x01' + bytes([random.randint(0, 255) for i in range(0, 64)])
            ),
            dynamic = None
        )
    
    @public_api
    async def pull(self, notification):
        ''' Pulls state from upstream and applies it. Does not check for
        recency.
        '''
        # Is this a good idea? It is actually possible to un-delete an object,
        # if you re-bind it, soooo...
        # TODO: think darüber
        if not self.isalive:
            raise DeadObject('Cannot pull a deleted object. Create a new one.')
            
        elif self.dynamic:
            summary = await self._librarian.summarize(notification)
            # We've been deleted.
            if isinstance(summary, _GdxxLite):
                async with self._update_lock:
                    await self.apply_delete(summary)
                    logger.info(''.join((
                        'GAO at ',
                        str(self.ghid),
                        ' PULL COMPLETED; deleted.'
                    )))
                    
            # We're being updated.
            elif summary.ghid == self.ghid:
                async with self._update_lock:
                    await self._pull()
                    logger.info(''.join((
                        'GAO at ',
                        str(self.ghid),
                        ' PULL COMPLETED with updates.'
                    )))
            
            # The update did not match us. This is sufficient to verify
            # both that the update was a dynamic binding, and that there's
            # no upstream bug for us.
            else:
                raise ValueError(
                    'Update failed pull; mismatched binding ghid.'
                )
            
        else:
            raise TypeError('Static objects cannot be pulled.')
            
    @pull.fixture
    async def pull(self, notification):
        ''' For the fixture, just pass to _pull directly.
        '''
        await self._pull()
    
    @public_api
    async def _pull(self):
        ''' The actual "meat and bones" for pulling.
        
        This is what you'll extend in Dispatchable to update
        applications.
        '''
        # Make sure we have the most recent binding, regardless of what was
        # recently passed.
        binding = await self._librarian.summarize(self.ghid)
        
        # Okay, if this is calling _pull from oracle._get_object, we may be
        # going for either a dynamic binding or a static object (container).
        if isinstance(binding, _GobdLite):
            self._recover_target_secret(binding)
            
            target_vector = binding.target_vector
            dynamic = True
                    
        # Static object, so this is the actual container (and not the binding).
        elif isinstance(binding, _GeocLite):
            target_vector = [binding.ghid]
            dynamic = False
            
        # None of the above. TypeError.
        else:
            raise TypeError('Update failed pull; mismatched Golix primitive.')
        
        try:
            # Do this before the state update so it has access to self.ghid
            # If this was just pulled for the first time, init the dynamic and
            # author attributes.
            self._conditional_init(
                ghid = binding.ghid,    # For GOBS, actually the container ghid
                dynamic = dynamic,
                author = binding.author
            )
            
            state_update = asyncio.ensure_future(
                self._update_state(target_vector)
            )
            await asyncio.shield(state_update)
        
        # Ensure the state update cannot be cancelled partway through
        except asyncio.CancelledError:
            await state_update
            raise
            
    @_pull.fixture
    async def _pull(self):
        ''' Do a conditional init if we haven't had stuff fully defined.
        '''
        self._conditional_init(
            ghid = Ghid.from_bytes(
                b'\x01' + bytes([random.randint(0, 255) for i in range(0, 64)])
            ),
            author = Ghid.from_bytes(
                b'\x01' + bytes([random.randint(0, 255) for i in range(0, 64)])
            ),
            dynamic = bool(random.randint(0, 1))
        )
        
    async def _update_state(self, target_vector):
        ''' Does a failure-resistant, semi-atomic setate update.
        '''
        for target_ghid in target_vector:
            try:
                packed_state = await self._recover_container(target_ghid)
                
                # Finally, with all of the administrative stuff handled, unpack
                # the actual payload.
                await self.unpack_gao(packed_state)
                
            except asyncio.CancelledError:
                raise
                
            except Exception:
                logger.error('Exception updating GAO state. Attempting to ' +
                             'failback to stale frame w/ traceback:\n' +
                             ''.join(traceback.format_exc()))
            
            # On the first success, stop looking for a failback container.
            else:
                break
                
        # Hitting this means we failed to find a valid state. We need to raise.
        else:
            raise UnrecoverableState('No valid state found for ' +
                                     str(self.ghid))
        
    def _recover_target_secret(self, binding):
        ''' For the passed binding, ratchet the secret chain (if
        possible), staging all intermediate secrets in the process. If
        the ratchet is broken and the secret also missing, raise.
        '''
        # Also don't forget to update history. Do this first, because it
        # will create a canonical update, regardless of success in
        # unpacking.
        try:
            # Recover the new secret for the chain.
            logger.debug('Healing ratchet for ' + str(self.ghid) + ' frame #' +
                         str(binding.counter) + '...')
            self._privateer.heal_chain(
                proxy = self.ghid,
                target_vector = self._maximize_target_vector(binding),
                master_secret = self._master_secret
            )
            
        except RatchetError as exc:
            if binding.target not in self._privateer:
                raise UnknownSecret(str(binding.target)) from exc
        
        # Now, we need to be sure our local history doesn't forget about
        # our current-most-recent frame when making an update.
        finally:
            # Finally, update our target_history accordingly.
            self.target_history = collections.deque(
                binding.target_vector,
                # Make sure we have at least one historical target
                maxlen = max(2, len(binding.target_vector))
            )
            
    async def _recover_container(self, container_ghid):
        ''' From the container ghid, recover the packed state.
        '''
        secret = self._privateer.get(container_ghid)
        
        packed = await self._librarian.retrieve(container_ghid)
        
        try:
            # TODO: fix the leaky abstraction of jumping into the _identity
            unpacked = self._golcore._identity.unpack_container(packed)
            packed_state = await self._golcore.open_container(
                unpacked,
                secret
            )
        
        except SecurityError:
            self._privateer.abandon(container_ghid)
            raise
            
        else:
            self._privateer.commit(container_ghid, localize=self._local_secret)
        
        return packed_state
        
    def _maximize_target_vector(self, new_obj):
        ''' Construct the longest possible target vector, given the
        new_obj and our knowledge of the old target history.
        '''
        # We need to accommodate a possible counter is None
        old_counter = self._counter
        if old_counter is None:
            old_counter = 0
        
        offset = new_obj.counter - old_counter
        
        new_len = len(new_obj.target_vector)
        old_len = len(self.target_history)
        maximized = list(new_obj.target_vector)
        
        # If the new target vector is smaller than the offset, then the
        # old target vector is "buried" history (there are missing frames
        # between the newest old target and the oldest new target) and
        # therefore useless. Short-circuit so we don't accidentally try to
        # do something with a negative index and then IndexError.
        if new_len - offset > 0:
            for ii in range(new_len - offset, old_len):
                maximized.append(self.target_history[ii])
            
        return maximized
        
    def _advance_history(self, new_obj):
        ''' Updates our history to match the new one.
        
        DEPRECATED AND UNUSED.
        
        NOTE THAT THIS DEPENDS ON THE PERSISTER to enforce the history
        hash chain. This is only aligning the histories, and NOT looking
        at the current frames themselves.
        '''
        old_history = self.frame_history
        new_history = new_obj.history
        
        # We need to check to make sure this isn't pulling in the zeroth frame.
        if new_history:
            new_legroom = len(new_history)
            
            # Find how far offset the new_history is from the old_history.
            for offset in range(new_legroom):
                # Align the histories by checking the ii'th new element against
                # an offset ii'th old element.
                for ii in range(new_legroom - offset):
                    # The new element matches the offset old element, so don't
                    # change the offset.
                    if new_history[offset + ii] == old_history[ii]:
                        continue
                    
                    # It didn't match, so we need to up the offset by one.
                    # Short circuit the inner loop via break.
                    else:
                        break
                    
                # We found a match. Short-circuit parent loop.
                else:
                    break
        
            # We never found a match. Offset should be made into the length of
            # the entire thing. This depends on persistence system to enforce
            # the hash chain!
            else:
                offset += 1
        
        # This is pulling the zeroth frame. We have no history.
        else:
            # Make sure new_legroom is always at least 1. Otherwise, the zeroth
            # frame (which by definition has no history) will forever damn the
            # object from being pulled (until it gets updated at its origin
            # first).
            new_legroom = 1
            offset = 0
            
        # Calculate the offset as the difference between the two counters,
        # easy-peasy style.
        offset = new_obj.counter - self._counter
                    
        
        # Now, appendleft the new frame (and an empty target) until we get to
        # the most recent one -- but hold off on adding the new target so we
        # can preserve the maximum history length
        for ii in range(offset - 1, 0, -1):
            self.frame_history.appendleft(new_history[ii])
            self.target_history.appendleft(None)
            
    def _conditional_init(self, ghid, dynamic, author):
        ''' If dynamic had not been set, set both it and the author. If
        ghid has not been set, set it.
        '''
        if self._dynamic is None:
            self._dynamic = dynamic
            
        if self._author is None:
            self._author = author
            
        if self.ghid is None:
            self.ghid = ghid
        
    async def pack_gao(self):
        ''' Packs self into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        
        May be used to implement, for example, packing self into a
        DispatchableState, etc etc.
        '''
        pass
        
    async def unpack_gao(self, packed):
        ''' Unpacks state from a bytes object and applies state to self.
        May be overwritten in subs to unpack more complex objects.
        
        May be used to implement, for example, dicts performing a
        clear() operation before an update() instead of just reassigning
        the object.
        '''
        pass
        
    def __del__(self, *args, **kwargs):
        ''' Log the removal of the GAO.
        '''
        logger.info(
            'GAO ' + str(self.ghid) + ' passed out of memory and is being ' +
            'locally garbage collected by Python.'
        )
            
            
class GAO(GAOCore):
    ''' A bare-bones GAO that performs trivial serialization (the
    serialization of the state is the state itself).
    '''
    
    def __init__(self, *args, state, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = state
        
    async def pack_gao(self):
        ''' Packs self into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        
        May be used to implement, for example, packing self into a
        DispatchableState, etc etc.
        '''
        return self.state
        
    async def unpack_gao(self, packed):
        ''' Unpacks state from a bytes object and applies state to self.
        May be overwritten in subs to unpack more complex objects.
        
        May be used to implement, for example, dicts performing a
        clear() operation before an update() instead of just reassigning
        the object.
        '''
        self.state = packed
        
    def __eq__(self, other):
        ''' Ensure the other object is the same gao with the same state.
        '''
        equal = True
        
        try:
            equal &= (self.dynamic == other.dynamic)
            equal &= (self.ghid == other.ghid)
            equal &= (self.author == other.author)
            equal &= (self.state == other.state)
            
        except AttributeError:
            equal = False
        
        return equal
            
            
class _GAOPickleBase(GAOCore):
    ''' Golix-aware base object with pickle serialization.
    '''
        
    async def pack_gao(self):
        ''' Packs self into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        
        May be used to implement, for example, packing self into a
        DispatchableState, etc etc.
        '''
        try:
            return pickle.dumps(self.state, protocol=4)
            
        except Exception:
            logger.error(
                'Failed to pickle the GAO w/ traceback: \n' +
                ''.join(traceback.format_exc())
            )
            raise
        
    async def unpack_gao(self, packed):
        ''' Unpacks state from a bytes object and applies state to self.
        May be overwritten in subs to unpack more complex objects.
        
        May be used to implement, for example, dicts performing a
        clear() operation before an update() instead of just reassigning
        the object.
        '''
        ''' Unpacks state from a bytes object. May be overwritten in
        subs to unpack more complex objects. Should always be a
        staticmethod or classmethod.
        '''
        try:
            self.state = pickle.loads(packed)
            
        except Exception:
            logger.error(
                'Failed to unpickle the GAO w/ traceback: \n' +
                ''.join(traceback.format_exc())
            )
            raise
            
    __eq__ = GAO.__eq__
            
            
class _DictMixin(metaclass=Accountable):
    ''' A golix-aware dictionary.
    '''
    
    def __init__(self, *args, state=None, **kwargs):
        super().__init__(*args, **kwargs)
        
        if state is None:
            self._state = {}
        else:
            self._state = dict(state)
        
    @property
    def state(self):
        ''' Pass through to self._state
        '''
        return self._state
        
    @state.setter
    def state(self, value):
        ''' Preserve the actual dictionary object, instead of just
        replacing it.
        
        Don't use this to mutate things, or we'll defeat the entire
        point of delta tracking by constantly marking it as modified
        while pulling updates from upstream.
        '''
        self._state.clear()
        self._state.update(value)
        
    __len__ = PROXY_FUNC
    __iter__ = PROXY_FUNC
    __contains__ = PROXY_FUNC
    __getitem__ = PROXY_FUNC
    __setitem__ = MUTATING_PROXY_FUNC
    __delitem__ = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if using default and nothing changes?
    pop = MUTATING_PROXY_FUNC
    items = PROXY_FUNC
    keys = PROXY_FUNC
    values = PROXY_FUNC
    # Note: this is lazy; what if using default and nothing changes?
    setdefault = MUTATING_PROXY_FUNC
    get = PROXY_FUNC
    # Note: this is lazy; what if using default and nothing changes?
    popitem = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if empty?
    clear = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    update = MUTATING_PROXY_FUNC
        
    # def __len__(self):
    #     # Straight pass-through
    #     return len(self._state)
        
    # def __iter__(self):
    #     # Pass through to self._state
    #     return iter(self._state)
            
    # def __getitem__(self, key):
    #     # Pass through to self._state
    #     return self._state[key]
            
    # def __setitem__(self, key, value):
    #     # Pass through to self._state
    #     self._state[key] = value
            
    # def __delitem__(self, key):
    #     # Pass through to self._state
    #     del self._state[key]
            
    # def __contains__(self, key):
    #     # Pass through to self._state
    #     return key in self._state
            
    # def pop(self, key, *args, **kwargs):
    #     # Pass through to self._state
    #     return self._state.pop(key, *args, **kwargs)
        
    # def items(self, *args, **kwargs):
    #     # Because the return is a view object, competing use will result in
    #     # python errors, so we don't really need to worry about statelock.
    #     return self._state.items(*args, **kwargs)
        
    # def keys(self, *args, **kwargs):
    #     # Because the return is a view object, competing use will result in
    #     # python errors, so we don't really need to worry about statelock.
    #     return self._state.keys(*args, **kwargs)
        
    # def values(self, *args, **kwargs):
    #     # Because the return is a view object, competing use will result in
    #     # python errors, so we don't really need to worry about statelock.
    #     return self._state.values(*args, **kwargs)
        
    # def setdefault(self, key, *args, **kwargs):
    #     # Pass through to self._state
    #     return self._state.setdefault(key, *args, **kwargs)
        
    # def get(self, *args, **kwargs):
    #     # Pass through to self._state
    #     return self._state.get(*args, **kwargs)
        
    # def popitem(self, *args, **kwargs):
    #     # Pass through to self._state
    #     return self._state.popitem(*args, **kwargs)
        
    # def clear(self, *args, **kwargs):
    #     # Pass through to self._state
    #     return self._state.clear(*args, **kwargs)
        
    # def update(self, *args, **kwargs):
    #     # Pass through to self._state
    #     return self._state.update(*args, **kwargs)
    
    
class GAODict(_GAOPickleBase, _DictMixin):
    ''' Combine GAO dicts with pickle serialization.
    '''
    pass
            
            
class _SetMixin(metaclass=Accountable):
    ''' A golix-aware set.
    '''
    
    def __init__(self, *args, state=None, **kwargs):
        super().__init__(*args, **kwargs)
        
        if state is None:
            self._state = set()
        else:
            self._state = set(state)
        
    @property
    def state(self):
        ''' Pass through to self._state
        '''
        return self._state
        
    @state.setter
    def state(self, value):
        ''' Preserve the actual set object, instead of just replacing
        it.
        
        Don't use this to mutate things, or we'll defeat the entire
        point of delta tracking by constantly marking it as modified
        while pulling updates from upstream.
        '''
        self._state.clear()
        self._state.update(value)
    
    __len__ = PROXY_FUNC
    __iter__ = PROXY_FUNC
    __contains__ = PROXY_FUNC
    add = MUTATING_PROXY_FUNC
    remove = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    discard = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if empty?
    pop = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if empty?
    clear = MUTATING_PROXY_FUNC
    isdisjoint = PROXY_FUNC
    issubset = PROXY_FUNC
    issuperset = PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    update = MUTATING_PROXY_FUNC
    
    # @property
    # def state(self):
    #     ''' Pass through to self._state
    #     '''
    #     return self._state
        
    # @state.setter
    # def state(self, value):
    #     ''' Preserve the actual set object, instead of just replacing
    #     it.
    #     '''
    #     if self._state is None:
    #         self._state = value
    #     else:
    #         to_add = value - self._state
    #         to_remove = self._state - value
    #         self._state -= to_remove
    #         self._state |= to_add
    #         # self._state.clear()
    #         # self._state.update(value)
            
    # def __contains__(self, key):
    #     # Pass through to self._state
    #     return key in self._state
        
    # def __len__(self):
    #     # Pass through to self._state
    #     return len(self._state)
        
    # def __iter__(self):
    #     # Pass through to self._state
    #     return iter(self._state)
            
    # def add(self, elem):
    #     # Pass through to self._state
    #     self._state.add(elem)
                
    # def remove(self, elem):
    #     # Pass through to self._state
    #     self._state.remove(elem)
            
    # def discard(self, elem):
    #     # Pass through to self._state
    #     self._state.discard(elem)
            
    # def pop(self):
    #     # Pass through to self._state
    #     return self._state.pop()
            
    # def clear(self):
    #     # Pass through to self._state
    #     self._state.clear()
            
    # def isdisjoint(self, other):
    #     # Pass through to self._state
    #     return self._state.isdisjoint(other)
            
    # def issubset(self, other):
    #     # Pass through to self._state
    #     return self._state.issubset(other)
            
    # def issuperset(self, other):
    #     # Pass through to self._state
    #     return self._state.issuperset(other)
            
            
class GAOSet(_GAOPickleBase, _SetMixin):
    ''' Combine GAO sets with pickle serialization.
    '''
    pass
            
            
class _SetMapMixin(metaclass=Accountable):
    ''' A golix-aware setmap.
    '''
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state = SetMap()
        
    @property
    def state(self):
        ''' Pass through to self._state
        '''
        return self._state
        
    @state.setter
    def state(self, value):
        ''' Preserve the actual setmap object, instead of just
        replacing it.
        
        Don't use this to mutate things, or we'll defeat the entire
        point of delta tracking by constantly marking it as modified
        while pulling updates from upstream.
        '''
        self._state.clear_all()
        self._state.update_all(value)
        
    __len__ = PROXY_FUNC
    __iter__ = PROXY_FUNC
    __contains__ = PROXY_FUNC
    __bool__ = PROXY_FUNC
    get_any = PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    pop_any = MUTATING_PROXY_FUNC
    contains_within = PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    add = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    update = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    update_all = MUTATING_PROXY_FUNC
    remove = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    discard = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if empty?
    clear = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    clear_any = MUTATING_PROXY_FUNC
    # Note: this is lazy; what if nothing changes?
    clear_all = MUTATING_PROXY_FUNC
    combine = PROXY_FUNC
            
    # def __contains__(self, key):
    #     with self._statelock:
    #         return key in self._state
        
    # def __len__(self):
    #     # Straight pass-through
    #     return len(self._state)
        
    # def __iter__(self):
    #     for key in self._state:
    #         yield key
    
    # def __getitem__(self, key):
    #     ''' Pass-through to the core lookup. Will return a frozenset.
    #     Raises keyerror if missing.
    #     '''
    #     with self._statelock:
    #         return self._state[key]
        
    # def get_any(self, key):
    #     ''' Pass-through to the core lookup. Will return a frozenset.
    #     Will never raise a keyerror; if key not in self, returns empty
    #     frozenset.
    #     '''
    #     with self._statelock:
    #         return self._state.get_any(key)
                
    # def pop_any(self, key):
    #     with self._statelock:
    #         result = self._state.pop_any(key)
    #         if result:
    #             self.push()
    #         return result
        
    # def contains_within(self, key, value):
    #     ''' Check to see if the key exists, AND the value exists at key.
    #     '''
    #     with self._statelock:
    #         return self._state.contains_within(key, value)
        
    # def add(self, key, value):
    #     ''' Adds the value to the set at key. Creates a new set there if
    #     none already exists.
    #     '''
    #     with self._statelock:
    #         # Actually do some detection to figure out if we need to push.
    #         if not self._state.contains_within(key, value):
    #             self._state.add(key, value)
    #             self.push()
                
    # def update(self, key, value):
    #     ''' Updates the key with the value. Value must support being
    #     passed to set.update(), and the set constructor.
    #     '''
    #     # TODO: add some kind of detection of a delta to make sure this
    #     # really changed something
    #     with self._statelock:
    #         self._state.update(key, value)
    #         self.push()
        
    # def remove(self, key, value):
    #     ''' Removes the value from the set at key. Will raise KeyError
    #     if either the key is missing, or the value is not contained at
    #     the key.
    #     '''
    #     with self._statelock:
    #         # Note that this will raise a keyerror before push if nothing is
    #         # going to change.
    #         self._state.remove(key, value)
    #         self.push()
        
    # def discard(self, key, value):
    #     ''' Same as remove, but will never raise KeyError.
    #     '''
    #     with self._statelock:
    #         if self._state.contains_within(key, value):
    #             self._state.discard(key, value)
    #             self.push()
        
    # def clear(self, key):
    #     ''' Clears the specified key. Raises KeyError if key is not
    #     found.
    #     '''
    #     with self._statelock:
    #         # Note that keyerror will be raised if no delta
    #         self._state.clear(key)
    #         self.push()
            
    # def clear_any(self, key):
    #     ''' Clears the specified key, if it exists. If not, suppresses
    #     KeyError.
    #     '''
    #     with self._statelock:
    #         if key in self._state:
    #             self._state.clear_any(key)
    #             self.push()
        
    # def clear_all(self):
    #     ''' Clears the entire mapping.
    #     '''
    #     with self._statelock:
    #         self._state.clear_all()
    #         self.push()
            
            
class GAOSetMap(_GAOPickleBase, _SetMapMixin):
    ''' Combine GAO setmaps with pickle serialization.
    '''
    pass
