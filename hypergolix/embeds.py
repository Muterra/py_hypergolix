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
# Embeds contains all of the application-side integrations. These will only
# be used by applications written in python.

# Control * imports.
__all__ = [
    # 'TestIntegration', 
    # 'LocalhostIntegration'
]

# External dependencies
import abc
import msgpack
import weakref

from golix import Guid

# Inter-package dependencies
# from .utils import AppDef
        

class _EmbedBase(metaclass=abc.ABCMeta):
    ''' Embeds are what you put in the actual application to communicate
    with the Hypergolix service.
    
    Note that each embed will have exactly one endpoint. However, for 
    some types of integrations (ex: filesystems), it may make sense for
    a single application to use multiple endpoints.
    '''
        
    def new_object(self, state=None, dynamic=False, callbacks=None):
        ''' Alternative constructor for AppObj that does not require 
        passing the embed explicitly.
        '''
        pass
        
    @abc.abstractmethod
    def register_api(self, api_id):
        ''' Registers the embed with the integration as supporting the
        passed api_id.
        
        May be called multiple times to denote that a single application 
        endpoint is capable of supporting multiple api_ids.
        
        Returns True.
        '''
        pass
        
    @abc.abstractmethod
    def share_object(self, obj, recipient_guid):
        ''' Shares an object with someone else.
        '''
        pass
        
    @abc.abstractmethod
    def load_object(self, guid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        pass
        
    @abc.abstractmethod
    def new_static(self, data):
        ''' Creates a new static object.
        '''
        pass
        
    @abc.abstractmethod
    def new_dynamic(self, data=None, link=None):
        ''' Creates a new dynamic object.
        '''
        pass
        
    @abc.abstractmethod
    def update_dynamic(self, obj, data=None, link=None):
        ''' Updates an existing dynamic object.
        '''
        pass
        
    @abc.abstractmethod
    def freeze_dynamic(self, obj):
        ''' Converts a dynamic object to a static object.
        '''
        pass
        
    @abc.abstractmethod
    def hold_object(self, obj):
        ''' Binds an object, preventing its deletion.
        '''
        pass
        
    @abc.abstractmethod
    def delete_object(self, obj):
        ''' Attempts to delete an object. May not succeed, if another 
        Agent has bound to it.
        '''
        pass


class AppObj:
    ''' A class for objects to be used by apps. Can be updated (if the 
    object was created by the connected Agent and is mutable) and have
    a state.
    
    Can be initiated directly using a reference to an embed. May also be
    constructed from _EmbedBase.new_object.
    '''
    __slots__ = [
        '_embed',
        '_is_dynamic',
        '_is_owned'
    ]
    
    def __init__(self, embed, state=None, dynamic=True, callbacks=None, _def=None, *args, **kwargs):
        ''' Create a new AppObj with:
        
        state isinstance bytes(like)
        dynamic isinstance bool(like) (optional)
        callbacks isinstance iterable of callables (optional)
        
        _def isinstance dict
        dynamic will be ignored if calling _def.
        
        NOTES re _def:
        ---------
        Must indicate ownership.
        '''
        super().__init__(*args, **kwargs)
        
        # This needs to be done first so we have access to object creation
        self._link_embed(embed)
        
        # _DEF xor state. Loading an existing object.
        if _def is not None and state is None:
            pass
            
        # STATE xor _def. Creating a new object.
        elif state is not None and _def is None:
            self._set_dynamic(dynamic)
            self._is_owned = True
            
        # Anything else is invalid
        else:
            raise TypeError('AppObj instances must be declared with a state.')
            
    def _set_dynamic(self, dynamic):
        ''' Sets whether or not we're dynamic based on dynamic.
        '''
        if dynamic:
            self._is_dynamic = True
        else:
            self._is_dynamic = False
            
    @property
    def mutable(self):
        ''' Returns true if and only if self is a dynamic object and is
        owned by the current agent.
        '''
        return self._is_dynamic and self._is_owned
            
    def delete(self):
        ''' Tells any persisters to delete. Clears local state. Future
        attempts to access will raise DeletedError, but does not (and 
        cannot) remove the object from memory.
        '''
        pass
        
    def share(self, recipient):
        ''' Shares the object with someone else.
        
        recipient isinstance Guid
        '''
        pass
        
    def hold(self):
        ''' Binds to the object, preventing its deletion.
        '''
        pass
        
    def freeze(self):
        ''' Creates a static snapshot of the dynamic object. Returns a 
        new static AppObj instance. Does NOT modify the existing object.
        May only be called on dynamic objects.
        '''
        pass
        
    def update(self, state):
        ''' Updates a mutable object to a new state.
        
        May only be called on a dynamic object that was created by the
        attached Agent.
        '''
        pass
            
    def _link_embed(self, embed):
        ''' Typechecks embed and them creates a weakref to it.
        '''
        if not isinstance(embed, _EmbedBase):
            raise TypeError('embed must subclass _EmbedBase.')
        
        self._embed = weakref.proxy(embed)
        
    @classmethod
    def from_guid(cls, embed, guid):
        '''
        embed isinstance _EmbedBase
        guid isinstance Guid
        '''
        pass