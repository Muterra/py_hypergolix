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

# Control * imports.
__all__ = [
    'NakError',
    'HandshakeError',
    'InaccessibleError',
    'UnknownPartyError',
    'PersistenceWarning',
    'StaticObject',
    'DynamicObject'
]


class NakError(RuntimeError):
    ''' Raised for most failed operations.
    '''
    pass


class HandshakeError(RuntimeError):
    ''' Raised when handshakes fail.
    '''
    pass


class InaccessibleError(RuntimeError):
    ''' Raised when an Agent does not have access to an object.
    '''
    pass
    
    
class UnknownPartyError(RuntimeError):
    ''' Raised when an Agent cannot find an identity definition for an
    author and therefore cannot verify anything.
    '''
    pass
    
    
class PersistenceWarning(RuntimeWarning):
    ''' Raised when a debinding did not result in the removal of its
    target -- for example, if another binding remains on the target
    object.
    '''
    pass
        

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
        
    def __hash__(self):
        return (
            hash(self.author) ^ 
            hash(self.address) ^ 
            hash(self.state)
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
            
        # This will only compare as far as we both have history.
        comp = zip(self.buffer, other.buffer)
        result = (
            self.author == other.author and
            self.address == other.address
        )
        for a, b in comp:
            result &= (a == b)
            
        return result
    
    
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
    return future.exception() or future.result()