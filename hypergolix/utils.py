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

# Control * imports.
__all__ = [
    'NakError',
    'InaccessibleError',
    'PersistenceWarning'
]


class InaccessibleError(RuntimeError):
    ''' Raised when an Agent does not have access to an object.
    '''
    pass


class NakError(RuntimeError):
    ''' Raised for most failed operations.
    '''
    pass
    
    
class PersistenceWarning(RuntimeWarning):
    ''' Raised when a debinding did not result in the removal of its
    target -- for example, if another binding remains on the target
    object.
    '''
    pass
    
    
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