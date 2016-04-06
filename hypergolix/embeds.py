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
# Embeds contains all of the application-side IPC integrations. These will only
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
from .core import RawObj
        

class _EmbedBase(metaclass=abc.ABCMeta):
    ''' Embeds are what you put in the actual application to communicate
    with the Hypergolix service.
    
    Note that each embed will have exactly one endpoint. However, for 
    some types of IPC systems (ex: filesystems), it may make sense for
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
        ''' Registers the embed with the service as supporting the
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
    def update_dynamic(self, guid_dynamic, state):
        ''' Updates an existing dynamic object strictly at the embed.
        '''
        pass
        
    @abc.abstractmethod
    def update_object(self, obj, state):
        ''' Wrapper for obj.update.
        '''
        pass
        
    @abc.abstractmethod
    def sync_object(self, obj):
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
        
    @property
    @abc.abstractmethod
    def _legroom(self):
        ''' The history length to retain.
        '''
        pass
        
    @abc.abstractmethod
    def subscribe(self, guid, callback):
        ''' Subscribe to updates from guid. Inherited(ish) from 
        persister.
        '''
        pass


class AppObj:
    ''' A class for objects to be used by apps. Can be updated (if the 
    object was created by the connected Agent and is mutable) and have
    a state.
    
    Can be initiated directly using a reference to an embed. May also be
    constructed from _EmbedBase.new_object.
    
    Everything here is wrapped from the messagepack dispatch format, so
    state may be more than just bytes.
    '''
    # This should define *only* ADDITIONAL slots.
    __slots__ = []
    
    def __init__(self, embed, *args, **kwargs):
        super().__init__(dispatcher=embed, *args, **kwargs)
            
    def _link_dispatch(self, dispatch):
        ''' Typechecks dispatch and them creates a weakref to it.
        '''
        if not isinstance(dispatch, _EmbedBase):
            raise TypeError('dispatch must subclass _EmbedBase.')
        
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(dispatch, weakref.ProxyTypes):
            self._dispatch = dispatch
        else:
            self._dispatch = weakref.proxy(dispatch)