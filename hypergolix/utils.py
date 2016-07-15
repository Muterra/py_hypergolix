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
import msgpack
import traceback
import asyncio
import warnings
import signal

from concurrent.futures import CancelledError

from golix import Ghid

# Utils may only import from .exceptions or .bases (except the latter doesn't 
# yet exist)
from .exceptions import HandshakeError

# Control * imports.
__all__ = [
    'StaticObject',
    'DynamicObject',
    'RawObj',
    'AppObj',
]


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################


class RawObj:
    ''' A class for objects to be used by apps. Can be updated (if the 
    object was created by the connected Agent and is mutable) and have
    a state.
    
    Can be initiated directly using a reference to an dispatch. May also be
    constructed from DispatcherBase.new_object.
    
    RawObj instances do not wrap their state before creating golix 
    containers. As their name suggests, they very simply upload their
    raw state in a container.
    '''
    __slots__ = [
        '_dispatch',
        '_is_dynamic',
        '_callbacks',
        '_deleted',
        '_author',
        '_address',
        '_state'
    ]
    
    # Restore the original behavior of hash
    __hash__ = type.__hash__
    
    def __init__(self, dispatch, state, dynamic=True, callbacks=None, _preexisting=None, _legroom=None, *args, **kwargs):
        ''' Create a new RawObj with:
        
        state isinstance bytes(like)
        dynamic isinstance bool(like) (optional)
        callbacks isinstance iterable of callables (optional)
        
        _preexisting isinstance tuple(like):
            _preexisting[0] = address
            _preexisting[1] = author
        '''
        super().__init__(*args, **kwargs)
        
        # This needs to be done first so we have access to object creation
        self._link_dispatch(dispatch)
        self._deleted = False
        self._callbacks = set()
        self._set_dynamic(dynamic)
        
        # Legroom is None. Infer it from the dispatch.
        if _legroom is None:
            _legroom = self._dispatch._legroom
        
        # _preexisting was set, so we're loading an existing object.
        # "Trust" anything using _preexisting to have passed a correct value
        # for state and dynamic.
        if _preexisting is not None:
            self._address = _preexisting[0]
            self._author = _preexisting[1]
            # If we're dynamic, subscribe to any updates.
            if self.is_dynamic:
                # This feels gross. Also, it's not really un-sub-able
                # Also, it breaks if we use this in AppObj.
                self._dispatch.persister.subscribe(self.address, self.sync)
            state = self._unwrap_state(state)
        # _preexisting was not set, so we're creating a new object.
        else:
            self._address = self._make_golix(state, dynamic)
            self._author = self._dispatch.whoami
            # For now, only subscribe to objects that we didn't create.
            
        # Now actually set the state.
        self._init_state(state, _legroom)
        # Finally, set the callbacks. Will error if inappropriate def (ex: 
        # attempt to register callbacks on static object)
        self._set_callbacks(callbacks)
        
    def _make_golix(self, state, dynamic):
        ''' Creates an object based on dynamic. Returns the ghid for a
        static object, and the dynamic ghid for a dynamic object.
        '''
        if dynamic:
            if isinstance(state, RawObj):
                ghid = self._dispatch.new_dynamic(
                    state = state
                )
            else:
                ghid = self._dispatch.new_dynamic(
                    state = self._wrap_state(state)
                )
        else:
            ghid = self._dispatch.new_static(
                state = self._wrap_state(state)
            )
            
        return ghid
        
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
        
        Is this used? I don't think it is.
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
                    'RawObj internals cannot be changed once they have been '
                    'declared. They must be mutated instead.'
                )
                
        super().__setattr__(name, value)
            
    def __delattr__(self, name):
        ''' Prevent deleting declared attributes.
        '''
        raise AttributeError(
            'RawObj internals cannot be changed once they have been '
            'declared. They must be mutated instead.'
        )
        
    def __eq__(self, other):
        if not isinstance(other, RawObj):
            raise TypeError(
                'Cannot compare RawObj instances to incompatible types.'
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
        ''' The ghid address of the agent that created the object.
        '''
        return self._author
        
    @property
    def address(self):
        ''' The ghid address of the object itself.
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
        # It's meaningless to call this on a static object, but there's also 
        # no need to error out
            
    def _set_dynamic(self, dynamic):
        ''' Sets whether or not we're dynamic based on dynamic.
        '''
        if dynamic:
            self._is_dynamic = True
        else:
            self._is_dynamic = False
            
    @property
    def dispatch(self):
        return self._dispatch
            
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
        return self._dispatch.whoami == self.author
            
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
        self._dispatch.delete_ghid(self.address)
        self.clear_callbacks()
        super().__setattr__('_deleted', True)
        super().__setattr__('_is_dynamic', None)
        super().__setattr__('_author', None)
        super().__setattr__('_address', None)
        super().__setattr__('_dispatch', None)
        
    @property
    def is_link(self):
        if self.is_dynamic:
            if isinstance(self._state[0], RawObj):
                return True
            else:
                return False
        else:
            return None
            
    @property
    def link_address(self):
        ''' Only available when is_link is True. Otherwise, will return
        None.
        '''
        if self.is_dynamic and self.is_link:
            return self._state[0].address
        else:
            return None
        
    @property
    def state(self):
        if self._deleted:
            raise ValueError('Object has already been deleted.')
        elif self.is_dynamic:
            if self.is_link:
                # Recursively resolve any nested/linked objects
                return self._state[0].state
            else:
                return self._state[0]
        else:
            return self._state
            
    @state.setter
    def state(self, value):
        ''' Wraps update().
        '''
        self.update(value)
    
    def _wrap_state(self, state):
        ''' Wraps a state before calling external update.
        
        For RawObj instances, this does nothing.
        '''
        return state
    
    def _unwrap_state(self, state):
        ''' Unwraps a state before calling internal update.
        
        For RawObj instances, this does nothing.
        '''
        return state
        
    def share(self, recipient):
        ''' Shares the object with someone else.
        
        recipient isinstance Ghid
        '''
        self._dispatch.share_object(
            obj = self,
            recipient = recipient
        )
        
    def hold(self):
        ''' Binds to the object, preventing its deletion.
        '''
        self._dispatch.hold_ghid(
            obj = self
        )
        
    def freeze(self):
        ''' Creates a static snapshot of the dynamic object. Returns a 
        new static RawObj instance. Does NOT modify the existing object.
        May only be called on dynamic objects. 
        
        Note: should really be reimplemented as a recursive resolution
        of the current container object, and then a hold on that plus a
        return of a static RawObj version of that. This is pretty buggy.
        
        Note: does not currently traverse nested dynamic bindings, and
        will probably error out if you attempt to freeze one.
        '''
        if self.is_dynamic:
            ghid = self._dispatch.freeze_dynamic(
                ghid_dynamic = self.address
            )
        else:
            raise TypeError(
                'Static objects cannot be frozen. If attempting to save them, '
                'call hold instead.'
            )
        
        # If we traverse, this will need to pick the author out from the 
        # original binding.
        # Also note that this will break in subclasses that have more 
        # required arguments.
        return type(self)(
            dispatch = self._dispatch,
            state = self.state,
            dynamic = False,
            _preexisting = (ghid, self.author)
        )
            
    def sync(self, *args):
        ''' Checks the current state matches the state at the connected
        Agent. If this is a dynamic and an update is available, do that.
        If it's a static and the state mismatches, raise error.
        '''
        if self.is_dynamic:
            self._dispatch.sync_dynamic(obj=self)
        else:
            self._dispatch.sync_static(obj=self)
        return True
            
    def update(self, state, _preexisting=False):
        ''' Updates a mutable object to a new state.
        
        May only be called on a dynamic object that was created by the
        attached Agent.
        
        If _preexisting is True, this is an update coming down from a
        persister, and we will NOT push it upstream.
        '''
        if self._deleted:
            raise ValueError('Object has already been deleted.')
            
        if not self.is_dynamic:
            raise TypeError('Cannot update a static RawObj.')
            
        # _preexisting has not been set, so this is a local request. Check if
        # we actually can update, and then test validity by pushing an update.
        if not _preexisting:
            if not self.is_owned:
                raise TypeError(
                    'Cannot update an object that was not created by the '
                    'attached Agent.'
                )
            else:
                self._dispatch.update_dynamic(
                    self.address, 
                    state = self._wrap_state(state)
                )
                
        # _preexisting has been set, so we may need to unwrap state before 
        # using it
        else:
            state = self._unwrap_state(state)
        
        # Regardless, now we need to update local state.
        self._state.appendleft(state)
        for callback in self.callbacks:
            callback(self)
            
    def _link_dispatch(self, dispatch):
        ''' Typechecks dispatch and them creates a weakref to it.
        '''
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(dispatch, weakref.ProxyTypes):
            self._dispatch = dispatch
        else:
            self._dispatch = weakref.proxy(dispatch)


class DispatchObj(RawObj):
    ''' A class for objects to be used by dispatchers.
    
    Has an api_id and POTENTIALLY an app token. If app token is defined,
    this is a private object for that application only. In that case,
    api_id is ignored during dispatching.
    
    Unlike RawObj instances, AppObj instances are meant to be used with
    a specific API definition, and/or a specific token. Tokens can be 
    used to provide a consistent PRIVATE, freeform application state,
    whilst any objects that are being communicated to a different agent
    must use api ids.
    
    Basically, token-based AppObj instances without an api_id represent
    data that will not be shared. It can, however, be any unstructured
    data.
    
    _private isinstance bool
        if True, dispatch by token.
        if False, dispatch by api id.
    
    DispatchObj instances will wrap their state in a dispatch structure 
    before updating golix containers. 
    '''
    # This should define *only* ADDITIONAL slots.
    __slots__ = [
        '_api_id',
        '_app_token'
    ]
    
    def __init__(self, dispatch, state, api_id=None, app_token=None, 
    dynamic=True, callbacks=None, _preexisting=None, *args, **kwargs):
        ''' NOTE: If app_token is not None, this will be considered a 
        private object. Though DispatchObjs do not explicitly disallow
        sharing app-private objects (unlike AppObjs, which do), sharing
        tokens is suuuuper bad practice.
        '''
        # Do this first to prevent extraneous creation of golix objects
        if _preexisting is None:
            
            # Make sure we were passed one or the other.
            if api_id is None and app_token is None:
                raise TypeError(
                    'api_id or app_token must be defined for a new object.'
                )
                
            # Now set them both, and let @property handle Nones
            else:
                self.app_token2 = app_token
                self.api_id2 = api_id
        
        super().__init__(
            dispatch = dispatch, 
            state = state, 
            dynamic = dynamic,
            callbacks = callbacks,
            _preexisting = _preexisting,
            *args, **kwargs
        )
        
        # Make sure to call this or we may be prematurely GC'd
        self.dispatch.register_object(self)
                
    @property
    def api_id(self):
        return self._api_id
        
    @property
    def api_id2(self):
        '''
        This version is predominantly intended for packing/unpacking and
        safely performing internal operations when the underlying _attr
        is in an unknown state.
        '''
        try:
            if self._api_id is None:
                return bytes(65)
            else:
                return self._api_id
        except AttributeError:
            return None
            
    @api_id2.setter
    def api_id2(self, value):
        ''' Sets the api_id. Note that this cannot be changed once it's
        been created, so we don't need to worry about accidental update
        shenanigans.
        
        This version is predominantly intended for packing/unpacking and
        safely performing internal operations when the underlying _attr
        is in an unknown state.
        '''
        # Only do this if we lack an existing value.
        if self.api_id2 is None:
            # This might be a smart place for some type checking...
            if value == bytes(65):
                self._api_id = None
            else:
                self._api_id = value
                
        # If this is an attempted update, just check to make sure they match.
        elif self.api_id2 != value:
            print(self.api_id2)
            print(value)
            raise RuntimeError(
                'Attempting to change api_id during update is not allowed.'
            )
            
    @property
    def app_token(self):
        return self._app_token
        
    @property
    def app_token2(self):
        '''
        This version is predominantly intended for packing/unpacking and
        safely performing internal operations when the underlying _attr
        is in an unknown state.
        '''
        try:
            if self._app_token is None:
                return bytes(4)
            else:
                return self._app_token
        except AttributeError:
            return None
            
    @app_token2.setter
    def app_token2(self, value):
        ''' Sets the token. Note that this cannot be changed once it's
        been created, so we don't need to worry about accidental update
        shenanigans.
        
        This version is predominantly intended for packing/unpacking and
        safely performing internal operations when the underlying _attr
        is in an unknown state.
        '''
        # Only do this if we lack an existing value.
        if self.app_token2 is None:
            # This might be a smart place for some type checking...
            if value == bytes(4):
                self._app_token = None
            else:
                self._app_token = value
                
        # If this is an attempted update, just check to make sure they match.
        elif self.app_token2 != value:
            raise RuntimeError(
                'Attempting to change app_token during update is not allowed.'
            )
            
    def _link_dispatch(self, dispatch):
        ''' Typechecks dispatch and them creates a weakref to it.
        '''
        # This will require moving DispatcherBase to a bases module
        # if not isinstance(dispatch, DispatcherBase):
        #     raise TypeError('dispatch must subclass DispatcherBase.')
        
        super()._link_dispatch(dispatch)
        
    def _wrap_state(self, state):
        ''' Wraps the passed state into a format that can be dispatched.
        
        This should probably use something other than messagepack, which
        is massive, massive overkill.
        '''
        msg = {
            'api_id': self.api_id2,
            'app_token': self.app_token2,
            'body': state,
        }
        try:
            packed_msg = msgpack.packb(msg, use_bin_type=True)
            
        except (
            msgpack.exceptions.BufferFull,
            msgpack.exceptions.ExtraData,
            msgpack.exceptions.OutOfData,
            msgpack.exceptions.PackException,
            msgpack.exceptions.PackValueError
        ) as e:
            raise ValueError(
                'Couldn\'t wrap state. Incompatible data format?'
            ) from e
            
        return packed_msg
        
    def _unwrap_state(self, state):
        ''' Wraps the object state into a format that can be dispatched.
        
        Note that this is also called during updates.
        '''
        try:
            unpacked_msg = msgpack.unpackb(state, encoding='utf-8')
            api_id = unpacked_msg['api_id']
            app_token = unpacked_msg['app_token']
            state = unpacked_msg['body']
            
        # MsgPack errors mean that we don't have a properly formatted handshake
        except (
            msgpack.exceptions.BufferFull,
            msgpack.exceptions.ExtraData,
            msgpack.exceptions.OutOfData,
            msgpack.exceptions.UnpackException,
            msgpack.exceptions.UnpackValueError,
            KeyError
        ) as e:
            # print(repr(e))
            # traceback.print_tb(e.__traceback__)
            # print(state)
            raise HandshakeError(
                'Handshake does not appear to conform to the hypergolix '
                'handshake procedure.'
            ) from e
            
        # The setters handle all of the error catching here.
        self.api_id2 = api_id
        self.app_token2 = app_token
            
        return state
        
    def share(*args, **kwargs):
        ''' Override normal share behavior to force use of IPC host to
        pass in objects.
        
        Note that sharing is (currently) always handled by the embed, so
        it's not an issue right now that we're changing its behavior.
        '''
        raise NotImplementedError(
            'DispatchObj cannot be shared from the obj; it must be shared '
            'from the IPC host.'
        )
        
        
class IPCPackerMixIn:
    ''' Mix-in class for packing objects for IPC usage.
    '''
    def _pack_object_def(self, address, author, state, is_link, api_id, app_token, private, dynamic, _legroom):
        ''' Serializes an object definition.
        '''
        if author is not None:
            author = bytes(author)
            
        if address is not None:
            address = bytes(address)
        
        msg = {
            'author': author,
            'address': address,
            'state': state,
            'is_link': is_link,
            'api_id': api_id,
            'app_token': app_token,
            'private': private,
            'dynamic': dynamic,
            '_legroom': _legroom,
        }
        try:
            packed_msg = msgpack.packb(msg, use_bin_type=True)
            
        except (
            msgpack.exceptions.BufferFull,
            msgpack.exceptions.ExtraData,
            msgpack.exceptions.OutOfData,
            msgpack.exceptions.PackException,
            msgpack.exceptions.PackValueError
        ) as e:
            raise ValueError(
                'Couldn\'t pack object definition. Incompatible data format?'
            ) from e
            
        return packed_msg
        
    def _unpack_object_def(self, data):
        ''' Deserializes an object from bytes.
        '''
        try:
            unpacked_msg = msgpack.unpackb(data, encoding='utf-8')
            
        # MsgPack errors mean that we don't have a properly formatted handshake
        except (
            msgpack.exceptions.BufferFull,
            msgpack.exceptions.ExtraData,
            msgpack.exceptions.OutOfData,
            msgpack.exceptions.UnpackException,
            msgpack.exceptions.UnpackValueError,
        ) as e:
            print(repr(e))
            traceback.print_tb(e.__traceback__)
            # print(state)
            raise ValueError(
                'Unable to unpack object definition. Different serialization?'
            ) from e
            
        if unpacked_msg['address'] is not None:
            unpacked_msg['address'] = Ghid.from_bytes(unpacked_msg['address'])
            
        if unpacked_msg['author'] is not None:
            unpacked_msg['author'] = Ghid.from_bytes(unpacked_msg['author'])
            
        # We may have already set the attributes.
        # Wrap this in a try/catch in case we've have.
        try:
            return (
                unpacked_msg['address'], 
                unpacked_msg['author'], 
                unpacked_msg['state'], 
                unpacked_msg['is_link'],
                unpacked_msg['api_id'], 
                unpacked_msg['app_token'],
                unpacked_msg['private'], 
                unpacked_msg['dynamic'], 
                unpacked_msg['_legroom']
            )
        except KeyError as e:
            raise ValueError(
                'Insufficient keys for object definition unpacking.'
            ) from e
        

class _ObjectBase:
    ''' DEPRECATED. UNUSED?
    
    Hypergolix objects cannot be directly updated. They must be 
    passed to Agents for modification (if applicable). They do not (and, 
    if you subclass, for security reasons they should not) reference a
    parent Agent.
    
    Objects provide a simple interface to the arbitrary binary data 
    contained within Golix containers. They track both the plaintext, 
    and the associated GHID. They do NOT expose the secret key material
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
        ''' Creates a new object. Address is the dynamic ghid. State is
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
    ''' DEPRECATED. UNUSED?
    
    An immutable object. Can be produced directly, or by freezing a
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
    ''' DEPRECATED. UNUSED?
    
    A mutable object. Updatable by Agents.
    Interestingly, this could also do the whole __setattr__/__delattr__
    thing from above, since we're overriding state, and buffer updating
    is handled by the internal deque.
    '''
    __slots__ = [
        '_author',
        '_address',
        '_buffer',
        '_callbacks'
    ]
    
    _REPROS = ['author', 'address', 'callbacks', '_buffer']
    
    def __init__(self, author, address, _buffer, callbacks=None):
        ''' Callbacks isinstance iter(callbacks)
        '''
        super().__init__(author, address)
        
        if not isinstance(_buffer, collections.deque):
            raise TypeError('Buffer must be collections.deque or similar.')
        if not _buffer.maxlen:
            raise ValueError(
                'Buffers without a max length will grow to infinity. Please '
                'declare a max length.'
            )
            
        self._callbacks = set()
        self._buffer = _buffer
        
        if callbacks is None:
            callbacks = tuple()
        for callback in callbacks:
            self.add_callback(callback)
        
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
    def callbacks(self):
        return self._callbacks
        
    def add_callback(self, callback):
        ''' Registers a callback to be called when the object receives
        an update.
        
        callback must be hashable and callable. Function definitions and
        lambdas are natively hashable; callable classes may not be.
        
        On update, callbacks are passed the object.
        '''
        if not callable(callback):
            raise TypeError('Callback must be callable.')
        self._callbacks.add(callback)
        
    def remove_callback(self, callback):
        ''' Removes a callback.
        
        Raises KeyError if the callback has not been registered.
        '''
        if callback in self._callbacks:
            self._callbacks.remove(callback)
        else:
            raise KeyError('Callback not found in dynamic obj callback set.')
        
    def clear_callbacks(self):
        ''' Resets all callbacks.
        '''
        self._callbacks = set()
        
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
            except:
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
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._shutdown_init_flag.set)
        self._shutdown_complete_flag.wait()
        
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
        for guardling in self._guardlings:
            try:
                guardling.stop_threadsafe()
            except:
                # This is very precarious. Swallow all exceptions.
                logger.error(
                    'Swallowed exception while closing ' + repr(guardling) + 
                    '.\n' + ''.join(traceback.format_exc())
                )



        
        
import sys
import traceback
import os
import time
import threading

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
            logger.info(traces)
    
    def stop(self):
        self.stop_requested.set()
        self.thread.join()
            
    def get_traces(self):
        code = []
        for thread_id, stack in sys._current_frames().items():
            # Don't dump the trace for the TraceLogger!
            if thread_id != threading.get_ident():
                code.extend(self._dump_thread(thread_id, stack))
                    
        return '\n'.join(code)
        
    def _dump_thread(self, thread_id, stack):
        code = []
        code.append("\n# Thread ID: %s" % thread_id)
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