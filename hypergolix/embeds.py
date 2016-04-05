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
import collections

from golix import Guid

# Inter-package dependencies
# from .utils import AppDef
        

class _EmbedBase(metaclass=abc.ABCMeta):
    ''' Embeds are what you put in the actual application to communicate
    with the Hypergolix service.
    
    Note that each embed will have exactly one endpoint. However, for 
    some types of integrations (ex: filesystems), it may make sense for
    a single application to use multiple endpoints.
    
    Note that this API will be merged with AgentCore when DynamicObject
    and StaticObject are reconciled with AppObj.
    '''
        
    def new_object(self, state=None, dynamic=False, callbacks=None):
        ''' Alternative constructor for AppObj that does not require 
        passing the embed explicitly.
        '''
        pass
        
    @property
    @abc.abstractmethod
    def whoami(self):
        ''' Return the address of the currently active agent.
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
    def get_object(self, guid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        pass
        
    @abc.abstractmethod
    def new_object(self, state, dynamic=True):
        ''' Creates a new (static or dynamic) object.
        
        RETURNS AN AppObj INSTANCE.
        '''
        pass
        
    @abc.abstractmethod
    def update_object(self, obj, state):
        ''' Updates an existing dynamic object.
        '''
        pass
        
    @abc.abstractmethod
    def new_static(self, state):
        ''' Creates a new static object.
        
        DOES NOT RETURN AN AppObj INSTANCE!
        Returns the object's guid.
        '''
        pass
        
    @abc.abstractmethod
    def new_dynamic(self, state):
        ''' Creates a new dynamic object.
        
        DOES NOT RETURN AN AppObj INSTANCE!
        Returns the object's guid.
        '''
        pass
        
    @abc.abstractmethod
    def update_dynamic(self, obj, state):
        ''' Updates an existing dynamic object.
        '''
        pass
        
    @abc.abstractmethod
    def sync_dynamic(self, obj):
        ''' Checks for an update to a dynamic object, and if one is 
        available, performs an update.
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
        '_callbacks',
        '_deleted',
        '_author',
        '_address',
        '_state'
    ]
    
    def __init__(self, embed, state=None, dynamic=True, callbacks=None, _preexisting=None, _legroom=None, *args, **kwargs):
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
        self._deleted = False
        self._set_dynamic(dynamic)
        
        # _preexisting was set, so we're loading an existing object.
        # "Trust" anything using _preexisting to have passed a correct value
        # for state.
        if _preexisting is not None and state is None:
            state = _preexisting
            
        # _preexisting was not set, so we're creating a new object.
        elif state is not None and _preexisting is None:
            self._address = self._make_golix(state, dynamic)
            self._author = self._embed.whoami
            
        # Anything else is invalid
        else:
            raise TypeError('AppObj instances must be declared with a state.')
            
        # Now actually set the state.
        self._init_state(state, _legroom)
        # Finally, set the callbacks. Will error if inappropriate def (ex: 
        # attempt to register callbacks on static object)
        self._set_callbacks(callbacks)
        
    def _make_golix(self, state, dynamic):
        ''' Creates an object based on dynamic. Returns the guid for a
        static object, and the dynamic guid for a dynamic object.
        '''
        if dynamic:
            if isinstance(state, AppObj):
                guid = self._embed.new_dynamic(
                    state = state
                )
            else:
                guid = self._embed.new_dynamic(
                    state = state
                )
        else:
            guid = self._embed.new_static(
                state = state
            )
            
        return guid
        
    def _init_state(self, state, _legroom):
        ''' Makes the first state commit for the object, regardless of
        whether or not the object is new or loaded. Even dynamic objects
        are initially loaded with a single frame of history.
        '''
        if self.is_dynamic:
            self._state = collections.deque(
                iterable = (state,),
                maxlen = _legroom
            )
        else:
            self._state = state
        
    def _force_silent_update(self, value):
        ''' Silently updates self._state to value.
        '''
        if self.is_dynamic:
            if not isinstance(value, collections.deque):
                raise TypeError(
                    'Dynamic object state definitions must be '
                    'collections.deque or similar.'
                )
            if not value.maxlen:
                raise ValueError(
                    'Dynamic object states without a max length will grow to '
                    'infinity. Please declare a max length.'
                )
            
        self._state = value
    
    # This might be a little excessive, but I guess it's nice to have a
    # little extra protection against updates?
    def __setattr__(self, name, value):
        ''' Prevent rewriting declared attributes in slots. Does not
        prevent assignment using @property.
        '''
        if name in self.__slots__:
            try:
                __ = getattr(self, name)
            except AttributeError:
                pass
            else:
                raise AttributeError(
                    'AppObj internals cannot be changed once they have been '
                    'declared. They must be mutated instead.'
                )
                
        super().__setattr__(name, value)
            
    def __delattr__(self, name):
        ''' Prevent deleting declared attributes.
        '''
        raise AttributeError(
            'AppObj internals cannot be changed once they have been '
            'declared. They must be mutated instead.'
        )
        
    def __eq__(self, other):
        if not isinstance(other, AppObj):
            raise TypeError(
                'Cannot compare AppObj instances to incompatible types.'
            )
            
        # Short-circuit if dynamic mismatches
        if not self.is_dynamic == other.is_dynamic:
            return False
            
        meta_comparison = (
            # self.is_owned == other.is_owned and
            self.address == other.address and
            self.author == other.author
        )
        
        # If dynamic, state comparison looks at as many state shots as we share
        if self.is_dynamic:
            state_comparison = True
            comp = zip(self._state, other._state)
            for a, b in comp:
                state_comparison &= (a == b)
                
        # If static, state comparison simply looks at both states directly
        else:
            state_comparison = (self.state == other.state)
            
        # Return the result of the whole comparison
        return meta_comparison and state_comparison
        
    @property
    def author(self):
        ''' The guid address of the agent that created the object.
        '''
        return self._author
        
    @property
    def address(self):
        ''' The guid address of the object itself.
        '''
        return self._address
        
    @property
    def buffer(self):
        ''' Returns a tuple of the current history if dynamic. Raises
        TypeError if static.
        '''
        if self.is_dynamic:
            return tuple(self._state)
        else:
            raise TypeError('Static objects cannot have buffers.')
            
    def _set_callbacks(self, callbacks):
        ''' Initializes callbacks.
        '''
        if callbacks is None:
            callbacks = tuple()
        for callback in callbacks:
            self.add_callback(callback)
        
    @property
    def callbacks(self):
        if self.is_dynamic:
            return self._callbacks
        else:
            raise TypeError('Static objects cannot have callbacks.')
        
    def add_callback(self, callback):
        ''' Registers a callback to be called when the object receives
        an update.
        
        callback must be hashable and callable. Function definitions and
        lambdas are natively hashable; callable classes may not be.
        
        On update, callbacks are passed the object.
        '''
        if not self.is_dynamic:
            raise TypeError('Static objects cannot register callbacks.')
        if not callable(callback):
            raise TypeError('Callback must be callable.')
        self._callbacks.add(callback)
        
    def remove_callback(self, callback):
        ''' Removes a callback.
        
        Raises KeyError if the callback has not been registered.
        '''
        if self.is_dynamic:
            if callback in self._callbacks:
                self._callbacks.remove(callback)
            else:
                raise KeyError(
                    'Callback not found in dynamic obj callback set.'
                )
        else:
            raise TypeError('Static objects cannot have callbacks.')
        
    def clear_callbacks(self):
        ''' Resets all callbacks.
        '''
        if self.is_dynamic:
            self._callbacks.clear()
        else:
            raise TypeError('Static objects cannot have callbacks.')
            
    def _set_dynamic(self, dynamic):
        ''' Sets whether or not we're dynamic based on dynamic.
        '''
        if dynamic:
            self._is_dynamic = True
        else:
            self._is_dynamic = False
            
    @property
    def is_dynamic(self):
        ''' Indicates whether this object is dynamic.
        returns True/False.
        '''
        return self._is_dynamic
        
    @property
    def is_owned(self):
        ''' Indicates whether this object is owned by the associated 
        Agent.
        
        returns True/False.
        '''
        return self._embed.whoami == self.author
            
    @property
    def mutable(self):
        ''' Returns true if and only if self is a dynamic object and is
        owned by the current agent.
        '''
        return self.is_dynamic and self.is_owned
            
    def delete(self):
        ''' Tells any persisters to delete. Clears local state. Future
        attempts to access will raise ValueError, but does not (and 
        cannot) remove the object from memory.
        '''
        self.clear_callbacks()
        self._embed.delete_object(self)
        super().__setattr__('_deleted', True)
        super().__setattr__('_is_dynamic', None)
        super().__setattr__('_author', None)
        super().__setattr__('_address', None)
        super().__setattr__('_embed', None)
        
    @property
    def state(self):
        if self._deleted:
            raise ValueError('Object has already been deleted.')
        elif self.is_dynamic:
            current = self._state[0]
            
            # Resolve any nested/linked objects
            if isinstance(current, AppObj):
                current = current.state
                
            return current
        else:
            return self._state
            
    @state.setter
    def state(self, value):
        if self._deleted:
            raise ValueError('Object has already been deleted.')
        elif self.is_dynamic:
            self._state.appendleft(value)
        else:
            raise TypeError('Cannot update state of a static object.')
        
    def share(self, recipient):
        ''' Shares the object with someone else.
        
        recipient isinstance Guid
        '''
        self._embed.share_object(
            obj = self,
            recipient_guid = recipient
        )
        
    def hold(self):
        ''' Binds to the object, preventing its deletion.
        '''
        self._embed.hold_object(
            obj = self
        )
        
    def freeze(self):
        ''' Creates a static snapshot of the dynamic object. Returns a 
        new static AppObj instance. Does NOT modify the existing object.
        May only be called on dynamic objects.
        '''
        if self.is_dynamic:
            self._embed.freeze_dynamic(
                obj = self
            )
        else:
            raise TypeError(
                'Static objects cannot be frozen. If attempting to save them, '
                'call hold instead.'
            )
            
    def sync(self):
        ''' Checks the current agent for an updated copy of the object.
        '''
        self._embed.sync_dynamic(
            obj = self
        )
        return True
            
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