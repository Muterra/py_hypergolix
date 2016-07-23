'''
Scratchpad for test-based development.

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

import IPython
import unittest
import warnings
import collections
import logging
import weakref

from hypergolix.persisters import MemoryPersister

from hypergolix.dispatch import DispatcherBase

from hypergolix.privateer import Privateer

from hypergolix import HGXCore
from hypergolix.core import Oracle
from hypergolix.core import _GAO

from hypergolix.exceptions import NakError
from hypergolix.exceptions import PersistenceWarning

# This is a semi-normal import
from golix.utils import _dummy_ghid

# These are abnormal imports
from golix import Ghid
from golix import ThirdParty
from golix import SecondParty
from golix import FirstParty


# ###############################################
# Really shitty test fixtures
# ###############################################
        
        
class _TestDispatcher(DispatcherBase):
    ''' An dispatcher that ignores all dispatching for test purposes.
    '''
    def __init__(self, core, oracle, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._core = core
        
        self._orphan_shares_pending = []
        self._orphan_shares_incoming = []
        self._orphan_shares_outgoing = []
        self._orphan_shares_failed = []
        
    def await_handshake_response(self, target, request):
        self._orphan_shares_pending.append((target, request))
        
    def dispatch_handshake(self, target):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        self._orphan_shares_incoming.append(target)
        
    def dispatch_handshake_ack(self, target):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        self._orphan_shares_outgoing.append(target)
    
    def dispatch_handshake_nak(self, target):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        self._orphan_shares_failed.append(target)
        
    def retrieve_recent_handshake(self):
        return self._orphan_shares_incoming.pop()
        
    def retrieve_recent_ack(self):
        return self._orphan_shares_outgoing.pop()
        
    def retrieve_recent_nak(self):
        return self._orphan_shares_failed.pop()


class GAOFix(_GAO):
    def apply_state(self, state):
        self._state = state
        
    def extract_state(self):
        return self._state
        
        
# JFC seriously? In HERE?! This is so ridiculously out of scope. This is just
# disgraceful and gross. TODO: make real actual test fixtures and suite.
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
            _legroom = self._dispatch._core._legroom
        
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
                self._dispatch._core.persister.subscribe(self.address, self.sync)
            state = self._unwrap_state(state)
        # _preexisting was not set, so we're creating a new object.
        else:
            self._address = self._make_golix(state, dynamic)
            self._author = self._dispatch._core.whoami
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
                ghid = self._dispatch._core.new_dynamic(
                    state = state
                )
            else:
                ghid = self._dispatch._core.new_dynamic(
                    state = self._wrap_state(state)
                )
        else:
            ghid = self._dispatch._core.new_static(
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
        return self._dispatch._core.whoami == self.author
            
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
        self._dispatch._core.delete_ghid(self.address)
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
        self._dispatch._core.hold_ghid(
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
            ghid = self._dispatch._core.freeze_dynamic(
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
                self._dispatch._core.update_dynamic(
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
            

class _TestEmbed:
    def register_api(self, api_id, object_handler=None):
        ''' Just here to silence errors from ABC.
        '''
        if object_handler is None:
            object_handler = lambda *args, **kwargs: None
            
        self._object_handlers[api_id] = object_handler
    
    def get_object(self, ghid):
        ''' Wraps RawObj.__init__  and get_ghid for preexisting objects.
        '''
        author, is_dynamic, state = self.get_ghid(ghid)
            
        return RawObj(
            # Todo: make the dispatch more intelligent
            dispatch = self,
            state = state,
            dynamic = is_dynamic,
            _preexisting = (ghid, author)
        )
        
    def new_object(self, state, dynamic=True, _legroom=None):
        ''' Creates a new object. Wrapper for RawObj.__init__.
        '''
        return RawObj(
            # Todo: update dispatch intelligently
            dispatch = self,
            state = state,
            dynamic = dynamic,
            _legroom = _legroom
        )
        
    def update_object(self, obj, state):
        ''' Updates a dynamic object. May link to a static (or dynamic) 
        object's address. Must pass either data or link, but not both.
        
        Wraps RawObj.update and modifies the dynamic object in place.
        
        Could add a way to update the legroom parameter while we're at
        it. That would need to update the maxlen of both the obj._buffer
        and the self._historian.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Obj must be an RawObj.'
            )
            
        obj.update(state)
        
    def sync_object(self, obj):
        ''' Wraps RawObj.sync.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError('Must pass RawObj or subclass to sync_object.')
            
        return obj.sync()
        
    def hand_object(self, obj, recipient):
        ''' DEPRECATED.
        
        Initiates a handshake request with the recipient to share 
        the object.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Obj must be a RawObj or similar.'
            )
    
        # This is, shall we say, suboptimal, for dynamic objects.
        # frame_ghid = self._historian[obj.address][0]
        # target = self._dynamic_targets[obj.address]
        target = obj.address
        self._core.hand_ghid(target, recipient)
        
    def share_object(self, obj, recipient):
        ''' Currently, this is just calling hand_object. In the future,
        this will have a devoted key exchange subprotocol.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Only RawObj may be shared.'
            )
        return self._core.hand_ghid(obj.address, recipient)
        
    def freeze_object(self, obj):
        ''' Wraps RawObj.freeze. Note: does not currently traverse 
        nested dynamic bindings.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Only RawObj may be frozen.'
            )
        return obj.freeze()
        
    def hold_object(self, obj):
        ''' Wraps RawObj.hold.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError('Only RawObj may be held by hold_object.')
        obj.hold()
        
    def delete_object(self, obj):
        ''' Wraps RawObj.delete. 
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Obj must be RawObj or similar.'
            )
            
        obj.delete()
        
    def _get_object(self, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        pass
        
    def _new_object(self, obj):
        ''' Handles only the creation of a new object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        
        return address, author
        '''
        pass
        
    def _update_object(self, obj, state):
        ''' Handles only the updating of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _sync_object(self, obj):
        ''' Handles only the syncing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _share_object(self, obj, recipient):
        ''' Handles only the sharing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _freeze_object(self, obj):
        ''' Handles only the freezing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _hold_object(self, obj):
        ''' Handles only the holding of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass

    def _delete_object(self, obj):
        ''' Handles only the deleting of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        pass
        
        
class _TD(_TestEmbed, _TestDispatcher):
    # Combine the embed and the dispatcher. FFS.
    def __init__(self, *args, **kwargs):
        self._legroom = 3
        super().__init__(*args, **kwargs)
        

class _TestClient(HGXCore):
    def __init__(self):
        oracle = Oracle(
            core = self,
            # gao_class = GAOFix,
        )
        super().__init__(persister=MemoryPersister(), oracle=oracle)
        self.link_privateer(Privateer(core=self))
        self.link_dispatch(_TD(core=self, oracle=oracle))
        
    def _discard_object(*args, **kwargs):
        pass


class _TestAgent_SharedPersistence(HGXCore):
    def __init__(self, *args, **kwargs):
        oracle = Oracle(
            core = self,
            # gao_class = GAOFix,
        )
        super().__init__(oracle=oracle, *args, **kwargs)
        self.link_privateer(Privateer(core=self))
        self.link_dispatch(_TD(core=self, oracle=oracle))
        
    def subscribe(self, ghid, callback):
        ''' We're not testing this right now but we need to suppress 
        warnings about it for EmbedBase. Pass everything straight to the
        persister.
        '''
        self.persister.subscribe(ghid, callback)
        
    def _discard_object(*args, **kwargs):
        pass


# ###############################################
# Testing
# ###############################################
        
        
class AgentTrashTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.persister = MemoryPersister()
        cls.agent1 = _TestAgent_SharedPersistence(
            persister = cls.persister
        )
        cls.agent2 = _TestAgent_SharedPersistence(
            persister = cls.persister
        )
        cls.dispatcher1 = cls.agent1._dispatcher
        cls.dispatcher2 = cls.agent2._dispatcher
        
    def test_alone(self):
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'
        
        # Create, test, and delete a static object
        obj1 = self.agent1._dispatcher.new_object(pt1, dynamic=False)
        self.assertEqual(obj1.state, pt1)
        
        self.agent1._dispatcher.delete_object(obj1)
        with self.assertRaises(NakError, msg='Agent failed to delete.'):
            self.persister.get(obj1.address)
        
        # Create, test, update, test, and delete a dynamic object
        obj2 = self.agent1._dispatcher.new_object(pt1, dynamic=True)
        self.assertEqual(obj2.state, pt1)
        
        self.agent1._dispatcher.update_object(obj2, state=pt2)
        self.assertEqual(obj2.state, pt2)
        
        self.agent1._dispatcher.delete_object(obj2)
        with self.assertRaises(NakError, msg='Agent failed to delete.'):
            self.persister.get(obj2.address)
        
        # Test dynamic linking
        obj3 = self.agent1._dispatcher.new_object(pt3, dynamic=False)
        obj4 = self.agent1._dispatcher.new_object(state=obj3.address, dynamic=True)
        obj5 = self.agent1._dispatcher.new_object(state=obj4.address, dynamic=True)
        
        # self.assertEqual(obj3.state, pt3)
        # self.assertEqual(obj4.state, pt3)
        # self.assertEqual(obj5.state, pt3)
        
        obj6 = self.agent1._dispatcher.freeze_object(obj4)
        # Note: at some point that was producing incorrect bindings and didn't
        # error out at the persister. Is that still a problem, or has it been 
        # resolved?
        self.agent1._dispatcher.update_object(obj4, pt4)
        
        # self.assertEqual(obj6.state, pt3)
        # self.assertEqual(obj4.state, pt4)
        # self.assertEqual(obj5.state, pt4)
        
    def test_together(self):
        contact1 = self.agent1.whoami
        contact2 = self.agent2.whoami
        
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        
        obj1 = self.agent1._dispatcher.new_object(pt1, dynamic=True)
        obj1s1 = self.agent1._dispatcher.freeze_object(obj1)
        
        self.agent1._dispatcher.hand_object(obj1s1, contact2)
        # This is still using the old object-based stuff so ignore it for now
        # self.assertIn(obj1s1.address, self.agent2._secrets)
        # self.assertEqual(
        #     self.agent1._secrets[obj1s1.address], 
        #     self.agent2._secrets[obj1s1.address]
        # )
        obj1s1_shared = self.dispatcher2.retrieve_recent_handshake()
        self.assertEqual(
            obj1s1_shared, obj1s1.address
        )
        
        # Test handshakes using dynamic objects
        self.agent1._dispatcher.hand_object(obj1, contact2)
        # Same note as above.
        # self.assertIn(obj1.address, self.agent2._historian)
        # # Make sure this doesn't error, checking that it's in secrets
        # self.agent2._get_secret(obj1.address)
        obj1_shared = self.dispatcher2.retrieve_recent_handshake()
        self.assertEqual(
            obj1.address,
            obj1_shared
        )
        
        # And one more time...
        # # Awesome, now let's check updating them -- including subscriptions, 
        # # and that the recipient correctly ratchets the key and updates state.
        # self.agent1.update_object(obj1, pt2)
        # self.assertEqual(
        #     obj1.state,
        #     pt2
        # )
        # self.assertEqual(
        #     obj1,
        #     obj1_shared
        # )
            
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        
        
class ClientTrashTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agent1 = _TestClient()
        cls.agent2 = _TestClient()
        
    def test_alone(self):
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'
        
        # Create, test, and delete a static object
        obj1 = self.agent1._dispatcher.new_object(pt1, dynamic=False)
        self.agent1._dispatcher.delete_object(obj1)
        obj2 = self.agent1._dispatcher.new_object(pt1, dynamic=True)
        self.agent1._dispatcher.update_object(obj2, state=pt2)
        self.agent1._dispatcher.delete_object(obj2)
        # Test dynamic linking
        obj3 = self.agent1._dispatcher.new_object(pt3, dynamic=False)
        obj4 = self.agent1._dispatcher.new_object(state=obj3.address, dynamic=True)
        obj5 = self.agent1._dispatcher.new_object(state=obj4.address, dynamic=True)
        obj6 = self.agent1._dispatcher.freeze_object(obj4)
        self.agent1._dispatcher.update_object(obj4, pt4)
            
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        

if __name__ == "__main__":
    logging.basicConfig(filename='logs/core.log', level=logging.DEBUG)
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()