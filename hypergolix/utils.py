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

import logging
import collections
import threading
import abc
import weakref
import traceback
import asyncio
import warnings
import signal
import sys
import os
import time
import threading

from concurrent.futures import CancelledError

from golix import Ghid

# Utils may only import from .exceptions or .bases (except the latter doesn't
# yet exist)
from .exceptions import HandshakeError


# ###############################################
# Logging boilerplate
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports.
__all__ = [
    # 'StaticObject',
]


# ###############################################
# Lib
# ###############################################


class ApiID(Ghid):
    ''' Subclass Ghid in a way that makes the API ID seem like a normal
    64-byte string.
    
    Remind me again why the hell I'm subclassing ghid for this when I'm
    ending up removing just about everything that makes it a ghid?
    '''
    
    def __init__(self, address, algo=None):
        ''' Wrap the normal Ghid creation with a forced algo.
        '''
        if len(address) != 64:
            raise ValueError('Improper API ID length.')
        
        elif algo is None:
            algo = 0
            
        elif algo != 0:
            raise ValueError('Improper API ID format.')
        
        super().__init__(algo, address)
        
    def __repr__(self):
        ''' Hide that this is a ghid.
        '''
        return ''.join(
            type(self).__name__,
            '(',
            self.address,
            ')'
        )
        
    @property
    def algo(self):
        return self._algo
        
    @algo.setter
    def algo(self, value):
        # Currently, ensure algo is b'\x00'
        if value == 0:
            self._algo = value
        else:
            raise ValueError('Invalid address algorithm.')
            
    @property
    def address(self):
        return self._address
            
    @address.setter
    def address(self, address):
        self._address = address
        
        
class FiniteDict:
    ''' A dict-like bounded cache. Beyond max_size, object inserts will
    remove older elements. Element "freshness" is reset upon any and all
    operations specifically concerning that key:
    __getitem__
    __setitem__
    get (IFF the key existed)
    setdefault (IFF the key existed)
    '''
    
    def __init__(self, items=None, *args, maxlen, **kwargs):
        ''' Set us up with an internal data store and a max length.
        Don't directly subclass ordereddict, because there appear to be
        some inconsistencies re: documentation and implementation that
        are resulting in reentrancy-driven errors.
        '''
        super().__init__(*args, **kwargs)
        
        if items is None:
            self._data = collections.OrderedDict()
        else:
            self._data = collections.OrderedDict(items)
        
        self._maxlen = maxlen
        
    def _truncate(self):
        ''' Shrink the dict to, at most, they correct size.
        '''
        while len(self._data) > self._maxlen:
            self._data.popitem(last=True)
        
    def __getitem__(self, key):
        # Get the item, or raise KeyError if missing
        item = self._data[key]
        
        # Success! Bring the key to the front.
        self._data.move_to_end(key, last=False)
        
        # Now return the item.
        return item
        
    def __setitem__(self, key, value):
        # Set the item (or raise whatever if impossible)
        self._data[key] = value
        
        # Success! Bring the key to the front.
        self._data.move_to_end(key, last=False)
        
        # Truncate to the correct length.
        self._truncate()
        
    def __delitem__(self, key):
        del self._data[key]
        
    def get(self, key, default=None):
        # Only refresh if the key actually exists in self.
        try:
            item = self._data[key]
            
        except KeyError:
            item = default
            
        else:
            self._data.move_to_end(key, last=False)
            
        return item
        
    def setdefault(self, key, default=None):
        try:
            item = self._data[key]
            
        except KeyError:
            item = default
            self[key] = default
            # Let setting take care of truncation
            
        else:
            self._data.move_to_end(key, last=False)
            
        return item
        
    def update(self, *args, **kwargs):
        # Update with truncation.
        self._data.update(*args, **kwargs)
        self._truncate()
        
    def __eq__(self, other):
        if isinstance(other, FiniteDict):
            return self._data == other._data
        else:
            return self._data == other
            
    def __len__(self):
        return len(self._data)
        
    def __contains__(self, key):
        return key in self._data
        
    def __iter__(self):
        return iter(self._data)
        
    def __reversed__(self):
        return reversed(self._data)
        
    def clear(self):
        return self._data.clear()
        
    def items(self):
        return self._data.items()
        
    def keys(self):
        return self._data.keys()
        
    def values(self):
        return self._data.values()
        
    def pop(self, *args, **kwargs):
        return self._data.pop(*args, **kwargs)
        
    def popitem(self, *args, **kwargs):
        return self._data.popitem(*args, **kwargs)
        
        
class _WeakSet(set):
    ''' Re-write WeakSet to remove references ASAP, instead of lazily
    removing references upon access.
    
    Note: to bypass resolving-and-then-remaking weakrefs, could add
    a private iterator to iterate over self's refs directly instead of
    returning the item themselves.
    '''
    
    def __new__(cls, data=None, *args, **kwargs):
        ''' Native sets don't really use init. Do this instead.
        '''
        # We want to bypass the normal set() init, so that we don't
        # accidentally add a bunch of strong references through passing data.
        # Use *args and **kwargs to support coop multi inheritance
        self = super().__new__(cls, *args, **kwargs)
        # Now we'll start modifying self appropriately.
        
        # This defers removals until after iteration completes. It holds strong
        # references to the WEAK references of the objects themselves, IE:
        # strongref{weakref(object_to_remove), weakref(object_to_remove), ...}
        self._pending_removals = set()
        # This is the count of how many iterators are currently running
        self._iterators = 0
        # Updating the iterator count is not atomic. This makes it threadsafe.
        self._iterators_lock = threading.RLock()
        
        # This is added as a finalizer for all items added to the set. We need
        # the fancy construction so that if our contained objects outlive us,
        # they don't accidentally prevent self from deleting by holding strong
        # references. Therefore, we memoize a weakref to self.
        def _remove(item_weakref, self_weakref=weakref.ref(self)):
            # One of our contained objects is being GC'd. Attempt to resolve
            # the actual container reference.
            self = self_weakref()
            
            # In this case, the actual container object is already dead. Note
            # that using if-else may actually produce shorter bytecode. Not
            # that this matters particularly much, just worth noting.
            if self is None:
                return
                
            # The container is still live!
            else:
                # Protect ourselves against concurrent access to _iterators,
                # since operations with it are not atomic.
                with self._iterators_lock:
                    # HOLD UP! We have an iterator running.
                    if self._iterators > 0:
                        self._pending_removals.add(item_weakref)
                    
                    # No iterator is currently running, so discard the object
                    # directly.
                    else:
                        self._discard_ref(item_weakref)
        
        # And now add that to self as a normal function (NOT a bound/unbound
        # method!)
        self._remove = _remove
            
        # This is __new__, not __init__, so don't forget to return self.
        return self
        
    def __init__(self, data=None, *args, **kwargs):
        ''' This is here to prevent set's standard __init__ from adding
        data as a hard reference to self.
        '''
        super().__init__(*args, **kwargs)
        
        # If data was passed, use update (instead of super().__init__())
        if data is not None:
            self.update(data)

    def __iter__(self):
        ''' Override normal iteration to handle all pending removals at
        the conclusion of iteration. Otherwise, the size of the set will
        mutate during iteration, which is a critical error.
        
        NOTE THAT calling this will yield STRONG REFERENCES to objects,
        so iterating over the collection will necessary prevent
        contained objects from being removed -- but only the one that is
        CURRENTLY being used by the consumer of the iterator.
        '''
        # Incrementing is not atomic, so shield it. This also synchronizes with
        # any reference finalizing copies of self._remove.
        with self._iterators_lock:
            self._iterators += 1
            
        try:
            for item_weakref in super().__iter__():
                item = item_weakref()
                
                # We do need to check if the item is actually dead, which would
                # happen for every item that is sent to self._pending_removals.
                if item is not None:
                    yield item
                    
        # Finally, collect all removals and commit them. We DO need to shield
        # this within the _iterators lock, because otherwise, the self._remove
        # finalizer has a tiny race condition where it may add something to
        # pending_removals after iteration completes but before the iterators
        # count is decremented.
        finally:
            with self._iterators_lock:
                # ONLY do this on the conclusion of the last iterator,
                # though, lest we accidentally shaft some other iterator
                # by changing our size mid-iteration. Also, it should never be
                # less than 1, but just in case...
                if self._iterators <= 1:
                    try:
                        memoized_pending_removals = self._pending_removals
                        memoized_discard = self._discard_ref
                        
                        # This IS thread-safe, but only because it's using
                        # while, and because sets are themselves threadsafe.
                        while memoized_pending_removals:
                            memoized_discard(memoized_pending_removals.pop())
                    
                    # No matter what, be sure to clear the iterator count.
                    finally:
                        self._iterators = 0
                        
                else:
                    self._iterators -= 1

    def __len__(self):
        ''' Wrap __len__ to always return the correct size.
        '''
        with self._iterators_lock:
            return super().__len__() - len(self._pending_removals)

    def __contains__(self, item):
        ''' Wrap super.__contains__, since we need to know if the item's
        WEAK REFERENCE is in the container, not the item itself.
        '''
        try:
            # Don't automatically support comparison with weakrefs, because
            # otherwise we don't know for sure that the reference is live.
            item_weakref = weakref.ref(item)
        
        # Non-weakref-able objects obviously cannot be in the collection.
        except TypeError:
            return False
            
        # No errors -> check in super().
        else:
            return super().__contains__(item_weakref)

    def _has_ref(self, ref):
        ''' Just like the above, but checks for a raw reference. Only
        intended for debugging and testing.
        '''
        return super().__contains__(ref)

    def __reduce__(self):
        ''' Punt on pickle support.
        '''
        raise TypeError('_WeakSets do not currently support pickling.')
        # return (self.__class__, (list(self),),
        #         getattr(self, '__dict__', None))

    def add(self, item):
        ''' Add a weak reference to the item, not the item itself. Note
        that, just like normal sets, this will cause __iter__ to raise
        if invoked during iteration.
        '''
        # Call super...
        super().add(
            # On a weakref to the item, using self._remove as its finalizer.
            weakref.ref(item, self._remove)
        )

    def copy(self):
        ''' Modify default set behavior to support subclassing.
        '''
        # I heard you liked a self in your self so I selfed your self.
        return type(self)(item for item in self)

    def pop(self):
        ''' Pop stuff until a reference resolves.  Note that, just like
        normal sets, this will cause __iter__ to raise if invoked during
        iteration.
        '''
        # Run this until we KeyError or until we return.
        while True:
            try:
                item_weakref = super().pop()
            
            except KeyError:
                # Re-raise, and strip a level of context.
                raise KeyError('pop from empty WeakSet') from None
                
            else:
                item = item_weakref()
                # Check for live reference.
                if item is not None:
                    return item

    def remove(self, item):
        ''' Wrap super to remove the weakref instead of the item itself.
        '''
        super().remove(weakref.ref(item))

    def discard(self, item):
        ''' Wrap super to discard the weakref instead of the item
        itself.
        '''
        super().discard(weakref.ref(item))
        
    def _discard_ref(self, ref):
        ''' Discards a weakref directly.
        '''
        super().discard(ref)

    def update(self, other):
        ''' Wrap to add weakrefs instead of the items themselves.
        '''
        # Call super.add directly to avoid a stub function call to self.add.
        super().update(
            # As per above, call on a weakref to the item, and use
            # self._remove as its finalizer.
            weakref.ref(item, self._remove) for item in other
        )

    def __ior__(self, other):
        ''' Needs to actually return self, unlike update().
        '''
        self.update(other)
        return self

    def difference(self, other):
        ''' Return a copy of self, excluding anything contained in the
        other.
        '''
        return type(self)(item for item in self if item not in other)
    __sub__ = difference

    def difference_update(self, other):
        ''' Update, removing anything in the other set.
        '''
        # For performance reasons, short-circuit if identical. We could compare
        # __eq__ instead, but it would be equivalent performance wise, and also
        # potentially raise an error -- so avoid it.
        if self is other:
            self.clear()
        
        else:
            # Call super() on a generator expression for weakrefs of the items
            super().difference_update(weakref.ref(item) for item in other)
    
    def __isub__(self, other):
        self.difference_update(other)
        return self

    def intersection(self, other):
        ''' New set, with only elements common to both.
        '''
        return type(self)(item for item in other if item in self)
    __and__ = intersection

    def intersection_update(self, other):
        ''' Update in-place, leaving only what is common to both.
        '''
        super().intersection_update(weakref.ref(item) for item in other)
        
    def __iand__(self, other):
        self.intersection_update(other)
        return self

    def symmetric_difference(self, other):
        ''' One set, or the other set, but not both.
        '''
        # See other notes re: optimizing.
        if self is other:
            return type(self)()
        else:
            # Okay, this is an incredibly complicated generator function to
            # perform a symmetric difference. But, that's what it is.
            return type(self)(
                item for sset in (self, other) for item in sset if
                ((item not in self) ^ (item not in other))
            )
    __xor__ = symmetric_difference

    def symmetric_difference_update(self, other):
        ''' In-place XOR.
        '''
        if self is other:
            self.clear()
        else:
            super().symmetric_difference_update(
                # Don't forget the finalizer!
                weakref.ref(item, self._remove) for item in other
            )
    
    def __ixor__(self, other):
        self.symmetric_difference_update(other)
        return self

    def union(self, other):
        ''' New set, with all items common to both.
        '''
        # Fancy merge generators are fun!
        return type(self)(item for sset in (self, other) for item in sset)
    __or__ = union

    def isdisjoint(self, other):
        ''' Ensure that this set has no elements in common with other.
        '''
        return super().isdisjoint(weakref.ref(item) for item in other)

    def issubset(self, other):
        ''' Ensure every item in self exists in other.
        '''
        return super().issubset(weakref.ref(item) for item in other)
    __le__ = issubset

    def __lt__(self, other):
        ''' Test that every item in self exists in other, and that self
        is not identical to other (a proper subset).
        '''
        # Hmmm, is there a race condition between these two?
        return self.issubset(other) and len(self) != len(other)

    def issuperset(self, other):
        ''' Test if every element of other is contained in self.
        '''
        return super().issuperset(weakref.ref(item) for item in other)
    __ge__ = issuperset

    def __gt__(self, other):
        ''' Test that every item in other exists in self, and that self
        is not identical to other (a proper superset).
        '''
        # Hmmm, is there a race condition between these two?
        return self.issuperset(other) and len(self) != len(other)

    def __eq__(self, other):
        ''' Compare with both. Note that we can't much compare with any
        object, since that will be baaad (eg: dict == set). Instead,
        check that we're comparing against a set or subclass, and then
        do things accordingly.
        '''
        if not isinstance(other, collections.abc.Set) and \
           not isinstance(other, weakref.WeakSet):
                raise TypeError('Cannot compare to non-set-like objects.')

        # We're about to do an expensive comparison, so short-circuit if we can
        elif self is other:
            return True
        
        # We CAN compare.
        else:
            # Calculate the symmetric difference -- elements present in one
            # set XOR the other. If this is empty, we're equal.
            # Okay, this is an incredibly complicated generator function to
            # perform a symmetric difference. But, that's what it is.
            # Don't use self.symmetric_difference, because we want this to
            # always return a set, so that we don't have to worry about
            # subclasses changing the behavior of __init__ or symm_diff
            symdiff = set(
                item for sset in (self, other) for item in sset if
                ((item not in self) ^ (item not in other))
            )
            return len(symdiff) == 0
                
                
class _KeyedWeakSet(_WeakSet):
    ''' A set subclass intended to be used within a WeakSetMap. It holds
    all set members as weak references, and it holds a strong reference
    to self. When self is empty, it kills the strong reference to self.
    Assuming all other references to self are weak, the _WeakerSet will
    then be GC'd. Doesn't currently support any of the difference
    operators, so, uhhh, don't use them.
    '''
    
    def __new__(cls, data, *args, parent, key, **kwargs):
        ''' In this case, we must have data, parent, and key.
        '''
        return super().__new__(cls, data, *args, **kwargs)
    
    def __init__(self, data, *args, parent, key, **kwargs):
        ''' Modify __init__ to add an explicit, intentionally circular
        reference to self.
        '''
        super().__init__(data, *args, **kwargs)
        self._parent = weakref.ref(parent)
        self._key = key
        
    @property
    def _live(self):
        ''' Checks to see if self.__ref still exists. Intended strictly
        for debugging and testing.
        '''
        try:
            self.__ref
            
        except AttributeError:
            return False
            
        else:
            return True
        
    def _discard_ref(self, ref):
        ''' Discard the reference, and if we have nothing, delete
        self.__ref as well.
        '''
        super()._discard_ref(ref)
        if len(self) == 0:
            parent = self._parent()
            if parent is not None:
                parent.clear_any(self._key)
        
    def discard(self, *args, **kwargs):
        super().discard(*args, **kwargs)
        if len(self) == 0:
            parent = self._parent()
            if parent is not None:
                parent.clear_any(self._key)
        
    def remove(self, *args, **kwargs):
        super().remove(*args, **kwargs)
        if len(self) == 0:
            parent = self._parent()
            if parent is not None:
                parent.clear_any(self._key)
        
    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        if len(self) == 0:
            parent = self._parent()
            if parent is not None:
                parent.clear_any(self._key)
        return result
        
    def clear(self):
        # Equivalent to deletion.
        super().clear()
        parent = self._parent()
        if parent is not None:
            parent.clear_any(self._key)
        
    def difference_update(self, other):
        ''' Update, allowing for self to be nixxed.
        '''
        super().difference_update(other)
        if len(self) == 0:
            parent = self._parent()
            if parent is not None:
                parent.clear_any(self._key)

    def intersection_update(self, other):
        ''' Update, allowing for self to be nixxed.
        '''
        super().intersection_update(other)
        if len(self) == 0:
            parent = self._parent()
            if parent is not None:
                parent.clear_any(self._key)

    def symmetric_difference_update(self, other):
        ''' Update, allowing for self to be nixxed.
        '''
        super().symmetric_difference_update(other)
        if len(self) == 0:
            parent = self._parent()
            if parent is not None:
                parent.clear_any(self._key)
                
                
class _WeakerSet(_WeakSet):
    ''' A set subclass intended to be used within a WeakSetMap. It holds
    all set members as weak references, and it holds a strong reference
    to self. When self is empty, it kills the strong reference to self.
    Assuming all other references to self are weak, the _WeakerSet will
    then be GC'd. Doesn't currently support any of the difference
    operators, so, uhhh, don't use them.
    '''
    
    def __init__(self, *args, **kwargs):
        ''' Modify __init__ to add an explicit, intentionally circular
        reference to self.
        '''
        self.__ref = self
        super().__init__(*args, **kwargs)
        
    @property
    def _live(self):
        ''' Checks to see if self.__ref still exists. Intended strictly
        for debugging and testing.
        '''
        try:
            self.__ref
            
        except AttributeError:
            return False
            
        else:
            return True
        
    def _discard_ref(self, ref):
        ''' Discard the reference, and if we have nothing, delete
        self.__ref as well.
        '''
        super()._discard_ref(ref)
        if len(self) == 0:
            del self.__ref
        
    def discard(self, *args, **kwargs):
        super().discard(*args, **kwargs)
        if len(self) == 0:
            del self.__ref
        
    def remove(self, *args, **kwargs):
        super().remove(*args, **kwargs)
        if len(self) == 0:
            del self.__ref
        
    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        if len(self) == 0:
            del self.__ref
        return result
        
    def clear(self):
        # Equivalent to deletion.
        super().clear()
        del self.__ref
        
    def difference_update(self, other):
        ''' Update, allowing for self to be nixxed.
        '''
        super().difference_update(other)
        if len(self) == 0:
            del self.__ref

    def intersection_update(self, other):
        ''' Update, allowing for self to be nixxed.
        '''
        super().intersection_update(other)
        if len(self) == 0:
            del self.__ref

    def symmetric_difference_update(self, other):
        ''' Update, allowing for self to be nixxed.
        '''
        super().symmetric_difference_update(other)
        if len(self) == 0:
            del self.__ref
            
            
class SetMap:
    ''' Combines a mapping with a set. Threadsafe.
    '''
    
    def __init__(self):
        ''' Create a lookup!
        
        Currently does not support pre-population during __init__().
        '''
        self._mapping = {}
        self._lock = threading.RLock()
    
    def __getitem__(self, key):
        ''' Pass-through to the core lookup. Will return a frozenset.
        Raises keyerror if missing.
        '''
        with self._lock:
            return frozenset(self._mapping[key])
        
    def get_any(self, key):
        ''' Pass-through to the core lookup. Will return a frozenset.
        Will never raise a keyerror; if key not in self, returns empty
        frozenset.
        '''
        with self._lock:
            try:
                return frozenset(self._mapping[key])
            except KeyError:
                return frozenset()
                
    def pop_any(self, key):
        ''' Unlike other methods, pop_any returns the actual set,
        instead of a frozenset copy.
        '''
        with self._lock:
            try:
                return self._mapping.pop(key)
            except KeyError:
                return set()
        
    def __contains__(self, key):
        ''' Check to see if the key exists.
        '''
        with self._lock:
            return key in self._mapping
        
    def contains_within(self, key, value):
        ''' Check to see if the key exists, AND the value exists at key.
        '''
        with self._lock:
            try:
                return value in self._mapping[key]
            except KeyError:
                return False
        
    def add(self, key, value):
        ''' Adds the value to the set at key. Creates a new set there if
        none already exists.
        '''
        with self._lock:
            try:
                self._mapping[key].add(value)
            except KeyError:
                self._mapping[key] = {value}
                
    def update(self, key, value):
        ''' Updates the key with the value. Value must support being
        passed to set.update(), and the set constructor.
        '''
        with self._lock:
            try:
                self._mapping[key].update(value)
            except KeyError:
                self._mapping[key] = set(value)
                
    def update_all(self, other):
        ''' Updates all keys in other to self.
        '''
        with self._lock:
            for key in other:
                try:
                    self._mapping[key].update(other[key])
                except KeyError:
                    self._mapping[key] = set(other[key])
            
    def _remove_if_empty(self, key):
        ''' Removes a key entirely if it no longer has any values. Will
        suppress KeyError if the key is not found.
        '''
        try:
            if len(self._mapping[key]) == 0:
                del self._mapping[key]
        except KeyError:
            pass
        
    def remove(self, key, value):
        ''' Removes the value from the set at key. Will raise KeyError
        if either the key is missing, or the value is not contained at
        the key.
        '''
        with self._lock:
            try:
                self._mapping[key].remove(value)
            finally:
                self._remove_if_empty(key)
        
    def discard(self, key, value):
        ''' Same as remove, but will never raise KeyError.
        '''
        with self._lock:
            try:
                self._mapping[key].discard(value)
            except KeyError:
                pass
            finally:
                self._remove_if_empty(key)
        
    def clear(self, key):
        ''' Clears the specified key. Raises KeyError if key is not
        found.
        '''
        with self._lock:
            del self._mapping[key]
            
    def clear_any(self, key):
        ''' Clears the specified key, if it exists. If not, suppresses
        KeyError.
        '''
        with self._lock:
            try:
                del self._mapping[key]
            except KeyError:
                pass
        
    def clear_all(self):
        ''' Clears the entire mapping.
        '''
        with self._lock:
            self._mapping.clear()
            
    def __len__(self):
        ''' Returns the length of the mapping only.
        '''
        return len(self._mapping)
            
    def __iter__(self):
        ''' Send this through to the dict, bypassing the usual route,
        because otherwise we'll have a deadlock.
        '''
        with self._lock:
            for key in self._mapping:
                yield key
            
    def __eq__(self, other):
        ''' Expand comparison to search insides.
        '''
        with self._lock, other._lock:
            # First make sure both have same mapping keys.
            if set(self._mapping) != set(other._mapping):
                return False
            
            # Okay, now check each key has identical sets
            else:
                for this_key, this_set in self._mapping.items():
                    if other._mapping[this_key] != this_set:
                        return False
                        
        # If we successfully got through all of that, they are identical.
        return True
        
    def combine(self, other):
        ''' Returns a new SetMap with the union of both.
        '''
        new = type(self)()
        # Note that the iterator will take the lock, so we don't need to.
        for key in self:
            new.update(key, self._mapping[key])
        for key in other:
            new.update(key, other._mapping[key])
        return new

    def __getstate__(self):
        ''' Ignore self._lock to support pickling. Basically boilerplate
        copy from the pickle reference docs.
        '''
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['_lock']
        return state

    def __setstate__(self, state):
        ''' Ignore self._lock to support pickling. Basically boilerplate
        copy from the pickle reference docs.
        '''
        # Restore instance attributes (i.e., filename and lineno).
        self.__dict__.update(state)
        # Restore the lock.
        self._lock = threading.RLock()
        
    def __bool__(self):
        # Pass bool straight to mapping.
        return bool(self._mapping)
            
            
class WeakSetMap(SetMap):
    ''' SetMap that uses WeakerSets internally.
    '''
    
    def add(self, key, value):
        ''' Adds the value to the set at key. Creates a new set there if
        none already exists.
        '''
        with self._lock:
            try:
                self._mapping[key].add(value)
            except KeyError:
                self._mapping[key] = _KeyedWeakSet(
                    data = (value,),
                    parent = self,
                    key = key,
                )
                
    def update(self, key, value):
        ''' Updates the key with the value. Value must support being
        passed to set.update(), and the set constructor.
        '''
        with self._lock:
            try:
                self._mapping[key].update(value)
            except KeyError:
                self._mapping[key] = _KeyedWeakSet(
                    data = (item for item in value),
                    parent = self,
                    key = key,
                )
                
    def update_all(self, other):
        ''' Updates all keys in other to self.
        '''
        with self._lock:
            for key in other:
                try:
                    self._mapping[key].update(other[key])
                except KeyError:
                    self._mapping[key] = _KeyedWeakSet(
                        data = other[key],
                        parent = self,
                        key = key
                    )
                
    def __getitem__(self, key):
        ''' Resolves all of our references.
        '''
        result = super().__getitem__(key)
        return frozenset(ref() for ref in result)
        
    def get_any(self, key):
        ''' Resolve references.
        '''
        result = super().get_any(key)
        return frozenset(ref() for ref in result)
        
        
class WeakKeySetMap(SetMap):
    ''' SetMap with weak keys (but strong sets).
    '''
    
    def __init__(self, *args, **kwargs):
        ''' Override our mapping to be a weakref.WeakKeyDictionary.
        '''
        super().__init__(*args, **kwargs)
        self._mapping = weakref.WeakKeyDictionary()
        
        
class NoContext:
    ''' An empty context manager.
    '''
    
    def __enter__(*args, **kwargs):
        pass
        
    def __exit__(*args, **kwargs):
        pass


class _WeakProperty(property):
    ''' A weakly-referenced property.
    '''
    
    def __get__(self, obj, objtype=None):
        ''' Extend get to resolve the weakref.
        '''
        value_weakref = super().__get__(obj, objtype)
        value = value_weakref()
        if value is None:
            raise AttributeError('Attribute unavailable: weakref expired.')
        else:
            return value
        
    def __set__(self, obj, value):
        ''' Extend set to create the weakref.
        '''
        value_weakref = weakref.ref(value)
        return super().__set__(obj, value_weakref)
        
        
def weak_property(name):
    ''' Make a weakly-referenced property using name.
    '''
    @_WeakProperty
    def prop(self, name=name):
        return getattr(self, name)
        
    @prop.setter
    def prop(self, value, name=name):
        return setattr(self, name, value)
        
    return prop
        
    
def readonly_property(name):
    ''' Make a read-only property for the name.
    '''
    @property
    def prop(self, name=name):
        return getattr(self, name)
    return prop
    
    
def immutable_property(name):
    ''' Create a property that cannot be changed (nor deleted) once
    it has been defined.
    '''
    @property
    def prop(self, name=name):
        return getattr(self, name)
        
    @prop.setter
    def prop(self, value, name=name):
        try:
            existing = getattr(self, name)
        
        except AttributeError:
            existing = None
            
        if existing is not None:
            raise AttributeError('Cannot alter immutable property.')
        
        else:
            return setattr(self, name, value)
        
    return prop
    
    
def immortal_property(name):
    ''' Create a property that cannot be deleted.
    '''
    @property
    def prop(self, name=name):
        return getattr(self, name)
        
    @prop.setter
    def prop(self, value, name=name):
        return setattr(self, name, value)
        
    return prop


# ###############################################
# Older lib
# ###############################################
        
            
def _reap_wrapped_task(task):
    ''' Reap a task that was wrapped to never raise and then
    executed autonomously using ensure_future.
    '''
    task.result()
    
    
class _WeldedSet:
    __slots__ = ['_setviews']
    
    def __init__(self, *sets):
        # Some rudimentary type checking / forcing
        self._setviews = tuple(sets)
    
    def __contains__(self, item):
        for view in self._setviews:
            if item in view:
                return True
        return False
        
    def __len__(self):
        # This may not be efficient for large sets.
        union = set()
        union.update(*self._setviews)
        return len(union)
        
    def remove(self, elem):
        found = False
        for view in self._setviews:
            if elem in view:
                view.remove(elem)
                found = True
        if not found:
            raise KeyError(elem)
            
    def add_set_views(self, *sets):
        self._setviews += tuple(sets)
            
    def __repr__(self):
        c = type(self).__name__
        return (
            c + 
            '(' + 
                repr(self._setviews) + 
            ')'
        )
        

class _DeepDeleteChainMap(collections.ChainMap):
    ''' Chainmap variant to allow deletion of inner scopes. Used in 
    MemoryPersister.
    '''
    def __delitem__(self, key):
        found = False
        for mapping in self.maps:
            if key in mapping:
                found = True
                del mapping[key]
        if not found:
            raise KeyError(key)
    

class _WeldedSetDeepChainMap(collections.ChainMap):
    ''' Chainmap variant to combine mappings constructed exclusively of
    {
        key: set()
    }
    pairs. Used in MemoryPersister.
    '''
    def __getitem__(self, key):
        found = False
        result = _WeldedSet()
        for mapping in self.maps:
            if key in mapping:
                result.add_set_views(mapping[key])
                found = True
        if not found:
            raise KeyError(key)
        return result
    
    def __delitem__(self, key):
        found = False
        for mapping in self.maps:
            if key in mapping:
                del mapping[key]
                found = True
        if not found:
            raise KeyError(key)
    
    def remove_empty(self, key):
        found = False
        for mapping in self.maps:
            if key in mapping:
                found = True
                if len(mapping[key]) == 0:
                    del mapping[key]
        if not found:
            raise KeyError(key)
    
    
class _JitSetDict(dict):
    ''' Just-in-time set dictionary. A dictionary of sets. Attempting to
    access a value that does not exist will automatically create it as 
    an empty set.
    '''
    def __getitem__(self, key):
        if key not in self:
            self[key] = set()
        return super().__getitem__(key)
    
    
class _JitDictDict(dict):
    ''' Just-in-time dict dict. A dictionary of dictionaries. Attempting 
    to access a value that does not exist will automatically create it 
    as an empty dictionary.
    '''
    def __getitem__(self, key):
        if key not in self:
            self[key] = {}
        return super().__getitem__(key)
        
        
class _BijectDict:
    ''' A bijective dictionary. Aka, a dictionary where one key
    corresponds to exactly one value, and one value to exactly one key.
    
    Implemented as two dicts, with forward and backwards versions.
    
    Threadsafe.
    '''
    
    def __init__(self, *args, **kwargs):
        self._opslock = threading.RLock()
        self._fwd = dict(*args, **kwargs)
        # Make sure no values repeat and that all are hashable
        if len(list(self._fwd.values())) != len(set(self._fwd.values())):
            raise TypeError('_BijectDict values must be hashable and unique.')
        self._rev = {value: key for key, value in self._fwd.items()}
    
    def __getitem__(self, key):
        with self._opslock:
            try:
                return self._fwd[key]
            except KeyError:
                return self._rev[key]
    
    def __setitem__(self, key, value):
        with self._opslock:
            # Remove any previous connections with these values
            if value in self._fwd:
                raise ValueError('Value already exists as a forward key.')
            if key in self._rev:
                raise ValueError('Key already exists as a forward value.')
            # Note: this isn't perfectly atomic, as it won't restore a previous
            # value that we just failed to replace.
            try:
                self._fwd[key] = value
                self._rev[value] = key
            except Exception:
                # Default to None when popping to avoid KeyError
                self._fwd.pop(key, None)
                self._rev.pop(value, None)
                raise

    def __delitem__(self, key):
        with self._opslock:
            try:
                value = self._fwd.pop(key, None)
                del self._rev[value]
            except KeyError:
                value = self._rev.pop(key, None)
                del self._fwd[value]

    def __len__(self):
        return len(self._fwd)
        
    def __contains__(self, key):
        with self._opslock:
            return key in self._fwd or key in self._rev
            
    def __iter__(self):
        ''' Pass iter directly on to self._fwd
        '''
        return iter(self._fwd)
        
    def __reversed__(self):
        ''' Mappings don't have a strict order, so re-purpose reversed
        to look in the opposite direction (the self._rev dict).
        '''
        return iter(self._rev)


class TruthyLock:
    ''' Glues together a semaphore and an event, such that they can be
    used as a threadsafe blocking conditional.
    '''
    def __init__(self):
        self._opslock = threading.RLock()
        self._mutexlock = threading.RLock()
        self._cond = False
        
    def __bool__(self):
        with self._opslock:
            return self._cond
        
    def set(self):
        ''' Sets the internal flag to True. Indempotent.
        '''
        with self._opslock:
            self._cond = True
        
    def clear(self):
        ''' Sets the internal flag to False. Indempotent.
        '''
        with self._opslock:
            self._cond = False
            
    @property
    def mutex(self):
        ''' Returns the internal lock.
        Indended to be used with the context manager to provide mutually
        exclusive execution WITHOUT setting the condition.
        '''
        return self._mutexlock
            
    def __enter__(self):
        with self._opslock:
            self._mutexlock.acquire()
            self._cond = True
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._opslock:
            self._cond = False
            self._mutexlock.release()
        

class TraceLogger:
    ''' Log stack traces once per interval.
    '''
    
    def __init__(self, interval):
        """ Set up the logger.
        interval is in seconds.
        """
        if interval < 0.1:
            raise ValueError(
                'Interval too small. Will likely effect runtime behavior.'
            )
        
        self.interval = interval
        self.stop_requested = threading.Event()
        self.thread = threading.Thread(
            target = self.run,
            daemon = True,
            name = 'stacktracer'
        )
    
    def run(self):
        while not self.stop_requested.is_set():
            time.sleep(self.interval)
            traces = self.get_traces()
            logger.info(
                '#####################################################\n' +
                'TraceLogger frame:\n' +
                traces
            )
    
    def stop(self):
        self.stop_requested.set()
        self.thread.join()
            
    @classmethod
    def get_traces(cls):
        code = []
        for thread_id, stack in sys._current_frames().items():
            # Don't dump the trace for the TraceLogger!
            if thread_id != threading.get_ident():
                thread_name = cls.get_thread_name_from_id(thread_id)
                code.extend(cls._dump_thread(thread_id, thread_name, stack))
                    
        return '\n'.join(code)
        
    @staticmethod
    def get_thread_name_from_id(thread_id):
        threads = {}
        for thread in threading.enumerate():
            threads[thread.ident] = thread.name
            
        try:
            thread_name = threads[thread_id]
        except KeyError:
            thread_name = 'UNKNOWN'
        
        return thread_name
        
    @classmethod
    def dump_my_trace(cls):
        code = []
        for thread_id, stack in sys._current_frames().items():
            # Don't dump the trace for the TraceLogger!
            if thread_id == threading.get_ident():
                thread_name = cls.get_thread_name_from_id(thread_id)
                code.extend(cls._dump_thread(thread_id, thread_name, stack))
                    
        return '\n'.join(code)
        
    @classmethod
    def _dump_thread(cls, thread_id, thread_name, stack):
        code = []
        code.append('\n# Thread: ' + thread_name + ' w/ ID ' + str(thread_id))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
        return code
        
    def __enter__(self):
        self.thread.start()
        return self
        
    def __exit__(self, *args, **kwargs):
        self.stop()
    
    
def _generate_threadnames(*prefixes):
    ''' Generates a matching set of unique threadnames, of the form
    prefix[0] + '-1', prefix[1] + '-1', etc.
    '''
    ctr = 0
    names = []
    
    # Get existing thread NAMES (not the threads themselves!)
    existing_threadnames = set()
    for t in threading.enumerate():
        existing_threadnames.add(t.name)
        
    while len(names) != len(prefixes):
        candidates = [prefix + '-' + str(ctr) for prefix in prefixes]
        # Check the intersection of candidates and existing names
        if len(existing_threadnames & set(candidates)) > 0:
            ctr += 1
        else:
            names.extend(candidates)
            
    return names
        
        
def platform_specificker(linux_choice, win_choice, cygwin_choice, osx_choice, 
                        other_choice):
    ''' For the three choices, returns whichever is appropriate for this
    platform.
    
    "Other" means a non-linux Unix system, see python.sys docs: 
        
        For Unix systems, except on Linux, this is the lowercased OS 
        name as returned by uname -s with the first part of the version 
        as returned by uname -r appended, e.g. 'sunos5' or 'freebsd8', 
        at the time when Python was built.
    '''
    if sys.platform.startswith('linux'):
        return linux_choice
    elif sys.platform.startswith('win32'):
        return win_choice
    elif sys.platform.startswith('cygwin'):
        return cygwin_choice
    elif sys.platform.startswith('darwin'):
        return osx_choice
    else:
        return other_choice


def _default_to(check, default, comparator=None):
    ''' If check is None, apply default; else, return check.
    '''
    if comparator is None:
        if check is None:
            return default
        else:
            return check
    else:
        if check == comparator:
            return default
        else:
            return check


def ensure_equal_len(iterable, msg=''):
    ''' Ensures that all members of an iterable have the same length.
    Raises ValueError if not, using the passed msg. Returns the length.
    '''
    bulk_len = None
    for item in iterable:
        # Check length first, so that we only check bulk_len's definition once,
        # saving a few bytecode ops (just because it took me less time to think
        # of that than to write this comment explaining the ordering)
        if len(item) != bulk_len:
            
            # NOW make sure this isn't the first thing we're comparing to!
            if bulk_len is None:
                bulk_len = len(item)
                
            # If so, lengths don't match; raise.
            else:
                raise ValueError(msg)
                
    return bulk_len


# ###############################################
# Soon to be removed lib
# ###############################################
            

def _block_on_result(future):
    ''' Wait for the result of an asyncio future from synchronous code.
    Returns it as soon as available.
    '''
    event = threading.Event()
    
    # Create a callback to set the event and then set it for the future.
    def callback(fut, event=event):
        event.set()
    future.add_done_callback(callback)
    
    # Now wait for completion and return the exception or result.
    event.wait()
    
    exc = future.exception()
    if exc:
        raise exc
        
    return future.result()
        
        
def threading_autojoin():
    ''' Checks if this is the main thread. If so, registers interrupt
    mechanisms and then hangs indefinitely. Otherwise, returns 
    immediately.
    '''
    # SO BEGINS the "cross-platform signal wait workaround"
    if threading.current_thread() == threading.main_thread():
        signame_lookup = {
            signal.SIGINT: 'SIGINT',
            signal.SIGTERM: 'SIGTERM',
        }
        def sighandler(signum, sigframe):
            raise ZeroDivisionError('Caught ' + signame_lookup[signum])

        try:
            signal.signal(signal.SIGINT, sighandler)
            signal.signal(signal.SIGTERM, sighandler)
            
            # This is a little gross, but will be broken out of by the signal 
            # handlers erroring out.
            while True:
                time.sleep(600)
                
        except ZeroDivisionError as exc:
            logging.info(str(exc))


async def await_sync_future(fut):
    ''' Threadsafe, asyncsafe (ie non-loop-blocking) call to wait for a
    concurrent.Futures to finish, and then access the result.
    
    Must be awaited from the current 'context', ie event loop / thread.
    '''
    # Create an event on our source loop.
    source_loop = asyncio.get_event_loop()
    source_event = asyncio.Event(loop=source_loop)
    
    try:
        # Ignore the passed value and just set the flag.
        def callback(*args, **kwargs):
            source_loop.call_soon_threadsafe(source_event.set)
            
        # This will also be called if the fut is cancelled.
        fut.add_done_callback(callback)
        
        # Now we wait for the callback to run, and then handle the result.
        await source_event.wait()
    
    # Propagate any cancellation to the other event loop. Since the above await
    # call is the only point we pass execution control back to the loop, from
    # here on out we will never receive a CancelledError.
    except CancelledError:
        fut.cancel()
        raise
        
    else:
        # I don't know if/why this would ever be called (shutdown maybe?)
        if fut.cancelled():
            raise CancelledError()
        # Propagate any exception
        elif fut.exception():
            raise fut.exception()
        # Success!
        else:
            return fut.result()

        
async def run_coroutine_loopsafe(coro, target_loop):
    ''' Threadsafe, asyncsafe (ie non-loop-blocking) call to run a coro 
    in a different event loop and return the result. Wrap in an asyncio
    future (or await it) to access the result.
    
    Resolves the event loop for the current thread by calling 
    asyncio.get_event_loop(). Because of internal use of await, CANNOT
    be called explicitly from a third loop.
    '''
    # This returns a concurrent.futures.Future, so we need to wait for it, but
    # we cannot block our event loop, soooo...
    thread_future = asyncio.run_coroutine_threadsafe(coro, target_loop)
    return (await await_sync_future(thread_future))
            
            
def call_coroutine_threadsafe(coro, loop):
    ''' Wrapper on asyncio.run_coroutine_threadsafe that makes a coro
    behave as if it were called synchronously. In other words, instead
    of returning a future, it raises the exception or returns the coro's
    result.
    
    Leaving loop as default None will result in asyncio inferring the 
    loop from the default from the current context (aka usually thread).
    '''
    fut = asyncio.run_coroutine_threadsafe(
        coro = coro,
        loop = loop
    )
    
    # Block on completion of coroutine and then raise any created exception
    exc = fut.exception()
    if exc:
        raise exc
        
    return fut.result()
    
    
class LooperTrooper(metaclass=abc.ABCMeta):
    ''' Basically, the Arduino of event loops.
    Requires subclasses to define an async loop_init function and a 
    loop_run function. Loop_run is handled within a "while running" 
    construct.
    
    Optionally, async def loop_stop may be defined for cleanup.
    
    LooperTrooper handles threading, graceful loop exiting, etc.
    
    if threaded evaluates to False, must call LooperTrooper().start() to
    get the ball rolling.
    
    If aengel is not None, will immediately attempt to register self 
    with the aengel to guard against main thread completion causing an
    indefinite hang.
    
    *args and **kwargs are passed to the required async def loop_init.
    '''
    def __init__(self, threaded, thread_name=None, debug=False, aengel=None, *args, **kwargs):
        if aengel is not None:
            aengel.prepend_guardling(self)
        
        super().__init__(*args, **kwargs)
        
        self._startup_complete_flag = threading.Event()
        self._shutdown_init_flag = None
        self._shutdown_complete_flag = threading.Event()
        self._debug = debug
        self._death_timeout = 1
        
        if threaded:
            self._loop = asyncio.new_event_loop()
            # Set up a thread for the loop
            self._thread = threading.Thread(
                target = self.start,
                args = args,
                kwargs = kwargs,
                # This may result in errors during closing.
                # daemon = True,
                # This isn't currently stable enough to close properly.
                daemon = False,
                name = thread_name
            )
            self._thread.start()
            self._startup_complete_flag.wait()
            
        else:
            self._loop = asyncio.get_event_loop()
            # Declare the thread as nothing.
            self._thread = None
        
    async def loop_init(self, *args, **kwargs):
        ''' This will be passed any *args and **kwargs from self.start,
        either through __init__ if threaded is True, or when calling 
        self.start directly.
        '''
        pass
        
    @abc.abstractmethod
    async def loop_run(self):
        pass
        
    async def loop_stop(self):
        pass
        
    def start(self, *args, **kwargs):
        ''' Handles everything needed to start the loop within the 
        current context/thread/whatever. May be extended, but MUST be 
        called via super().
        '''
        try:
            self._loop.set_debug(self._debug)
            
            if self._thread is not None:
                asyncio.set_event_loop(self._loop)
            
            # Set up a shutdown event and then start the task
            self._shutdown_init_flag = asyncio.Event()
            self._looper_future = asyncio.ensure_future(
                self._execute_looper(*args, **kwargs)
            )
            self._loop.run_until_complete(self._looper_future)
            
        finally:
            self._loop.close()
            # stop_threadsafe could be waiting on this.
            self._shutdown_complete_flag.set()
        
    def halt(self):
        warnings.warn(DeprecationWarning(
            'Halt is deprecated. Use stop() or stop_threadsafe().'
        ))
        if self._thread is not None:
            self.stop_threadsafe()
        else:
            self.stop()
        
    def stop(self):
        ''' Stops the loop INTERNALLY.
        '''
        self._shutdown_init_flag.set()
    
    def stop_threadsafe(self):
        ''' Stops the loop EXTERNALLY.
        '''
        self.stop_threadsafe_nowait()
        self._shutdown_complete_flag.wait()
    
    def stop_threadsafe_nowait(self):
        ''' Stops the loop EXTERNALLY.
        '''
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._shutdown_init_flag.set)
        
    async def catch_interrupt(self):
        ''' Workaround for Windows not passing signals well for doing
        interrupts.
        
        Standard websockets stuff.
        
        Deprecated? Currently unused anyways.
        '''
        while not self._shutdown_init_flag.is_set():
            await asyncio.sleep(5)
            
    async def _execute_looper(self, *args, **kwargs):
        ''' Called by start(), and actually manages control flow for 
        everything.
        '''
        await self.loop_init(*args, **kwargs)
        
        try:
            while not self._shutdown_init_flag.is_set():
                await self._step_looper()
                
        except CancelledError:
            pass
            
        finally:
            # Prevent cancellation of the loop stop.
            await asyncio.shield(self.loop_stop())
            await self._kill_tasks()
            
    async def _step_looper(self):
        ''' Execute a single step of _execute_looper.
        '''
        task = asyncio.ensure_future(self.loop_run())
        interrupt = asyncio.ensure_future(self._shutdown_init_flag.wait())
        
        if not self._startup_complete_flag.is_set():
            self._loop.call_soon(self._startup_complete_flag.set)
            
        finished, pending = await asyncio.wait(
            fs = [task, interrupt],
            return_when = asyncio.FIRST_COMPLETED
        )
        
        # Note that we need to check both of these, or we have a race
        # condition where both may actually be done at the same time.
        if task in finished:
            # Raise any exception, ignore result, rinse, repeat
            self._raise_if_exc(task)
        else:
            task.cancel()
            
        if interrupt in finished:
            self._raise_if_exc(interrupt)
        else:
            interrupt.cancel()
            
    async def _kill_tasks(self):
        ''' Kill all remaining tasks. Call during shutdown. Will log any
        and all remaining tasks.
        '''
        all_tasks = asyncio.Task.all_tasks()
        
        for task in all_tasks:
            if task is not self._looper_future:
                logging.info('Task remains while closing loop: ' + repr(task))
                task.cancel()
        
        if len(all_tasks) > 0:
            await asyncio.wait(all_tasks, timeout=self._death_timeout)
            
    @staticmethod
    def _raise_if_exc(fut):
        if fut.exception():
            raise fut.exception()
            
            
class Aengel:
    ''' Watches for completion of the main thread and then automatically
    closes any other threaded objects (that have been registered with 
    the Aengel) by calling their close methods.
    '''
    def __init__(self, threadname='aengel', guardlings=None):
        ''' Creates an aengel.
        
        Uses threadname as the thread name.
        
        guardlings is an iterable of threaded objects to watch. Each 
        must have a stop_threadsafe() method, which will be invoked upon 
        completion of the main thread, from the aengel's own thread. The
        aengel WILL NOT prevent garbage collection of the guardling 
        objects; they are internally referenced weakly.
        
        They will be called **in the order that they were added.**
        '''
        # I would really prefer this to be an orderedset, but oh well.
        # That would actually break weakref proxies anyways.
        self._guardlings = collections.deque()
        self._dead = False
        self._stoplock = threading.RLock()
        
        if guardlings is not None:
            for guardling in guardlings:
                self.append_guardling(guardling)
            
        self._thread = threading.Thread(
            target = self._watcher,
            daemon = True,
            name = threadname,
        )
        self._thread.start()
        
    def append_guardling(self, guardling):
        if not isinstance(guardling, weakref.ProxyTypes):
            guardling = weakref.proxy(guardling)
            
        self._guardlings.append(guardling)
        
    def prepend_guardling(self, guardling):
        if not isinstance(guardling, weakref.ProxyTypes):
            guardling = weakref.proxy(guardling)
            
        self._guardlings.appendleft(guardling)
        
    def remove_guardling(self, guardling):
        ''' Attempts to remove the first occurrence of the guardling.
        Raises ValueError if guardling is unknown.
        '''
        try:
            self._guardlings.remove(guardling)
        except ValueError:
            logger.error('Missing guardling ' + repr(guardling))
            logger.error('State: ' + repr(self._guardlings))
            raise
    
    def _watcher(self):
        ''' Automatically watches for termination of the main thread and
        then closes the autoresponder and server gracefully.
        '''
        main = threading.main_thread()
        main.join()
        self.stop()
        
    def stop(self, *args, **kwargs):
        ''' Call stop_threadsafe on all guardlings.
        '''
        with self._stoplock:
            if not self._dead:
                for guardling in self._guardlings:
                    try:
                        guardling.stop_threadsafe_nowait()
                    except Exception:
                        # This is very precarious. Swallow all exceptions.
                        logger.error(
                            'Swallowed exception while closing ' + 
                            repr(guardling) + '.\n' + 
                            ''.join(traceback.format_exc())
                        )
                self._dead = True
