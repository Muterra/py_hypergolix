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

# Control * imports.
__all__ = [
    # 'StaticObject',
]


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################
        
        
class IPCPackerMixIn:
    ''' Mix-in class for packing objects for IPC usage.
    
    General format:
    version     1B      int16 unsigned
    address     65B     ghid
    author      65B     ghid
    private     1B      bool
    dynamic     1B      bool
    _legroom    1B      int8 unsigned
    api_id      65B     bytes
    is_link     1B      bool
    state       ?B      bytes (implicit length)
    '''
    def _pack_object_def(self, address, author, state, is_link, api_id, 
                        private, dynamic, _legroom):
        ''' Serializes an object definition.
        
        This is crude, but it's getting the job done for now. Also, for 
        the record, I was previously using msgpack, but good lord is it
        slow.
        '''
        version = b'\x00'
            
        if address is None:
            address = bytes(65)
        else:
            address = bytes(address)
        
        if author is None:
            author = bytes(65)
        else:
            author = bytes(author)
            
        private = bool(private).to_bytes(length=1, byteorder='big')
        dynamic = bool(dynamic).to_bytes(length=1, byteorder='big')
        if _legroom is None:
            _legroom = b'\x00'
        else:
            _legroom = int(_legroom).to_bytes(length=1, byteorder='big')
        if api_id is None:
            api_id = bytes(65)
        is_link = bool(is_link).to_bytes(length=1, byteorder='big')
        # State need not be modified
        
        return (version + 
                address + 
                author + 
                private + 
                dynamic + 
                _legroom + 
                api_id + 
                is_link +
                state)
        
    def _unpack_object_def(self, data):
        ''' Deserializes an object from bytes.
        '''
        try:
            version = data[0:1]
            address = data[1:66]
            author = data[66:131]
            private = data[131:132]
            dynamic = data[132:133]
            _legroom = data[133:134]
            api_id = data[134:199]
            is_link = data[199:200]
            state = data[200:]
            
        except Exception:
            logger.error(
                'Unable to unpack IPC object definition w/ traceback:\n'
                ''.join(traceback.format_exc())
            )
            raise
            
        # Version stays unmodified (unused)
        if address == bytes(65):
            address = None
        else:
            address = Ghid.from_bytes(address)
        if author == bytes(65):
            author = None
        else:
            author = Ghid.from_bytes(author)
        private = bool(int.from_bytes(private, 'big'))
        dynamic = bool(int.from_bytes(dynamic, 'big'))
        _legroom = int.from_bytes(_legroom, 'big')
        if _legroom == 0:
            _legroom = None
        if api_id == bytes(65):
            api_id = None
        is_link = bool(int.from_bytes(is_link, 'big'))
        # state also stays unmodified
        
        return (address,
                author,
                state,
                is_link,
                api_id,
                private,
                dynamic,
                _legroom)
    
    
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
        self._opslock = threading.Lock()
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
        self._stoplock = threading.Lock()
        
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


class TruthyLock:
    ''' Glues together a semaphore and an event, such that they can be
    used as a threadsafe blocking conditional.
    '''
    def __init__(self):
        self._opslock = threading.Lock()
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
            
            
class SetMap:
    ''' Combines a mapping with a set. Threadsafe.
    '''
    def __init__(self):
        ''' Create a lookup!
        
        Currently does not support pre-population during __init__().
        '''
        self._mapping = {}
        self._lock = threading.Lock()
    
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
        with self._lock:
            try:
                return frozenset(self._mapping.pop(key))
            except KeyError:
                return frozenset()
        
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
                self._mapping[key] = { value }
                
    def update(self, key, value):
        ''' Updates the key with the value. Value must support being
        passed to set.update(), and the set constructor.
        '''
        with self._lock:
            try:
                self._mapping[key].update(value)
            except KeyError:
                self._mapping[key] = set(value)
            
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
        self._lock = threading.Lock()
            
            
class _WeakerSet(weakref.WeakSet):
    ''' Used within a WeakSetMap to remove self when items are removed.
    '''
    def __init__(self, data=None, parent=None, key=None):
        super().__init__(data)
        
        if parent is None or key is None:
            raise TypeError('Must declare parent and key explicitly.')
            
        self.data = _SelfDestructingSet(self.data, parent, key)
                
                
class _SelfDestructingSet(set):
    def __init__(self, data, parent, key):
        super().__init__(data)
        self.__parent = weakref.proxy(parent)
        self.__key = key
        
    def discard(self, *args, **kwargs):
        super().discard(*args, **kwargs)
        if len(self) == 0:
            with self.__parent._lock:
                del self.__parent._mapping[self.__key]
        
    def remove(self, *args, **kwargs):
        super().remove(*args, **kwargs)
        if len(self) == 0:
            with self.__parent._lock:
                del self.__parent._mapping[self.__key]
        
    def pop(self, *args, **kwargs):
        result = super().pop(*args, **kwargs)
        if len(self) == 0:
            with self.__parent._lock:
                del self.__parent._mapping[self.__key]
        return result
            
            
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
                self._mapping[key] = _WeakerSet(
                    data = { value }, 
                    parent = self, 
                    key = key
                )
                
    def update(self, key, value):
        ''' Updates the key with the value. Value must support being
        passed to set.update(), and the set constructor.
        '''
        with self._lock:
            try:
                self._mapping[key].update(value)
            except KeyError:
                self._mapping[key] = _WeakerSet(
                    data = value, 
                    parent = self,
                    key = key
                )
        

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
            
# I think this is deprecated and unused?
import inspect
class _Proxy:
    ''' weakref.proxy doesn't support context managers? Balls to that!
    
    Actually this problem turns out to be very difficult and I've yet to
    solve it, but at least this is good practice for a pickle proxy.
    '''
    def __init__(self, obj):
        super().__setattr__('__hgxproxyref__', weakref.ref(obj))
        
        # Add some things to allow overrides
        overridable = { '__eq__', '__ge__', '__gt__', '__le__', '__lt__', 
            '__ne__', '__subclasshook__' }
            
        objdir = set(dir(obj))
        forbidden = set(dir(self)) - overridable
        potential_overrides = objdir - forbidden
        
        for candidate_name in potential_overrides:
            candidate = getattr(obj, candidate_name)
            if inspect.ismethod(candidate):
                super().__setattr__(candidate_name, 
                                    weakref.WeakMethod(candidate))
            elif inspect.isbuiltin(candidate):
                def weaker(*args, **kwargs):
                    proxy = super().__getattribute__('__hgxproxyref__')()
                    weakmethod = getattr(proxy, candidate_name)
                    return weakmethod(*args, **kwargs)
                
                super().__setattr__(candidate_name, weaker)
        
        
    # Explicitly un-support hashing
    __hash__ = None
        
    @property
    def _dereferenced(self):
        # Resolve the reference.
        return super().__getattribute__('__hgxproxyref__')()
        
    def __getattribute__(self, name, *args, **kwargs):
        try:
            return super().__getattribute__(name, *args, **kwargs)
        
        except AttributeError as outer_exc:
            try:
                proxy = super().__getattribute__('__hgxproxyref__')()
                return proxy.__getattribute__(name, *args, **kwargs)
            except Exception as inner_exc:
                raise inner_exc from outer_exc
        
    def __setattr__(self, *args, **kwargs):
        proxy = super().__getattribute__('__hgxproxyref__')()
        proxy.__setattr__(*args, **kwargs)
                
    def __delattr__(self, *args, **kwargs):
        proxy = super().__getattribute__('__hgxproxyref__')()
        proxy.__delattr__(*args, **kwargs)
        
        
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
