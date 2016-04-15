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
        ''' Creates an object based on dynamic. Returns the guid for a
        static object, and the dynamic guid for a dynamic object.
        '''
        if dynamic:
            if isinstance(state, RawObj):
                guid = self._dispatch.new_dynamic(
                    state = state
                )
            else:
                guid = self._dispatch.new_dynamic(
                    state = self._wrap_state(state)
                )
        else:
            guid = self._dispatch.new_static(
                state = self._wrap_state(state)
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
        self._dispatch.delete_guid(self.address)
        self.clear_callbacks()
        super().__setattr__('_deleted', True)
        super().__setattr__('_is_dynamic', None)
        super().__setattr__('_author', None)
        super().__setattr__('_address', None)
        super().__setattr__('_dispatch', None)
        
    @property
    def state(self):
        if self._deleted:
            raise ValueError('Object has already been deleted.')
        elif self.is_dynamic:
            current = self._state[0]
            
            # Recursively resolve any nested/linked objects
            if isinstance(current, RawObj):
                current = current.state
                
            return current
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
        
        recipient isinstance Guid
        '''
        self._dispatch.share_object(
            obj = self,
            recipient = recipient
        )
        
    def hold(self):
        ''' Binds to the object, preventing its deletion.
        '''
        self._dispatch.hold_guid(
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
            guid = self._dispatch.freeze_dynamic(
                guid_dynamic = self.address
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
            _preexisting = (guid, self.author)
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


class AppObj(RawObj):
    ''' A class for objects to be used by apps. Can be updated (if the 
    object was created by the connected Agent and is mutable) and have
    a state.
    
    AppObj instances will wrap their state in a dispatch structure 
    before updating golix containers.
    
    Can be initiated directly using a reference to an embed. May also be
    constructed from _EmbedBase.new_object.
    
    Everything here is wrapped from the messagepack dispatch format, so
    state may be more than just bytes.
    
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
        
    Note: AppObj should be set up such that there is exactly one AppObj
    per application. As such, they should probably be aware of their 
    endpoints.
    '''
    # This should define *only* ADDITIONAL slots.
    __slots__ = [
        '_api_id',
        '_app_token',
        '_private',
        '_raw'
    ]
    
    def __init__(self, embed, state, api_id=None, 
    private=False, dynamic=True, callbacks=None, _preexisting=None, 
    *args, **kwargs):
        self._raw = collections.deque(maxlen=1)
        # Why the fuck are objects even using this? This should only be in the
        # embed. Grrrrrr
        self._app_token = embed.app_token
        
        super().__init__(
            dispatch = embed, 
            state = state, 
            dynamic = dynamic, 
            callbacks = callbacks,
            _preexisting = _preexisting,
            *args, **kwargs
        )
        
        if _preexisting is not None:
            # Note that this is now covered by super().
            # state = self._unwrap_state(state)
            pass
        elif api_id is None:
            raise TypeError('api_id must be defined for a new object.')
        else:
            self._private = private
            self._api_id = api_id
            
    def _link_dispatch(self, dispatch):
        ''' Typechecks dispatch and them creates a weakref to it.
        '''
        # if not isinstance(dispatch, _EmbedBase):
        #     raise TypeError('dispatch must subclass _EmbedBase.')
        
        super()._link_dispatch(dispatch)
            
    def share(self, recipient):
        ''' Extends super() share behavior to disallow sharing of 
        private objects. Overriding this behavior will cause security
        risks for users/agents.
        '''
        if self.private:
            raise TypeError('Private application objects cannot be shared.')
        else:
            super().share(recipient)
            
    @property
    def private(self):
        ''' Return the (immutable) property describing whether this is
        a private application object, or a sharable api-id-dispatched
        object.
        '''
        return self._private
        
    @property
    def api_id(self):
        return self._api_id
        
    @property
    def app_token(self):
        return self._app_token
        
    @property
    def raw(self):
        ''' The raw packed bytes. Maybe useful for IPC?
        '''
        return self._raw[0]
        
    def _wrap_state(self, state):
        ''' Wraps the passed state into a format that can be dispatched.
        '''
        if self.private:
            app_token = self.app_token
        else:
            app_token = None
        
        msg = {
            'api_id': self.api_id,
            'app_token': app_token,
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
            
        self._raw.append(packed_msg)
        return packed_msg
        
    def _unwrap_state(self, state):
        ''' Wraps the object state into a format that can be dispatched.
        '''
        raw = state
        
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
            
        # We may have already set the attributes.
        # Wrap this in a try/catch in case we've have.
        try:
            # print('---- app_token: ', app_token)
            self._unwrap_api_id(api_id)
            self._unwrap_app_token(app_token)
            # print('---- self.app_token: ', self.app_token)
                
        # So, we've already set one or more of the attributes.
        except AttributeError as e:
            print(repr(e))
            traceback.print_tb(e.__traceback__)
            # # Should we double-check the consistency of _private?
            # pass
            
        self._raw.append(raw)
        return state
        
    def _unwrap_api_id(self, api_id):
        ''' Checks to see if api_id has already been defined. If so, 
        compares between them. If not, sets it.
        '''
        try:
            if self.api_id != api_id:
                raise RuntimeError('Mismatched api_ids across update.')
        except AttributeError:
            self._api_id = api_id
        
    def _unwrap_app_token(self, app_token):
        ''' Checks app token for None. If None, asks dispatch for the 
        appropriate token. Also assigns _private appropriately.
        '''
        try:
            # Compare to self.app_token FIRST to force attributeerror.
            if app_token != self.app_token and app_token is not None:
                raise RuntimeError('Mistmatched app tokens across update.')
            
        # There is no existing app token.
        except AttributeError:
            # If we have an app_token, we know this is a private object.
            if app_token is not None:
                self._app_token = app_token
                self._private = True
                
            # Otherwise, don't set the app token, and look it up after we've
            # completed the rest of object initialization.
            else:
                # self._app_token = self._dispatch.get_token(self.api_id)
                self._private = False
        

class _ObjectBase:
    ''' DEPRECATED. UNUSED?
    
    Hypergolix objects cannot be directly updated. They must be 
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