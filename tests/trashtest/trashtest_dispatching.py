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
import threading
import time
import logging

from golix import Ghid
from hypergolix import HGXCore

from hypergolix.privateer import Privateer
from hypergolix.persisters import MemoryPersister
from hypergolix.dispatch import Dispatcher


# ###############################################
# Cheap, trashy test fixtures
# ###############################################
        
        
from hypergolix.utils import RawObj
# This is actually almost semi-normal here. Except it needs to be rewritten.
class _TEFixture:
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
        
    def _discard_object(*args, **kwargs):
        pass
        
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
        self.hand_ghid(target, recipient)
        
    def share_object(self, obj, recipient):
        ''' Currently, this is just calling hand_object. In the future,
        this will have a devoted key exchange subprotocol.
        '''
        if not isinstance(obj, RawObj):
            raise TypeError(
                'Only RawObj may be shared.'
            )
        return self.hand_ghid(obj.address, recipient)
        
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
        
    # def _get_object(self, ghid):
    #     ''' Loads an object into local memory from the hypergolix 
    #     service.
    #     '''
    #     pass
        
    # def _new_object(self, obj):
    #     ''' Handles only the creation of a new object via the hypergolix
    #     service. Does not manage anything to do with the AppObj itself.
        
    #     return address, author
    #     '''
    #     pass
        
    # def _update_object(self, obj, state):
    #     ''' Handles only the updating of an object via the hypergolix
    #     service. Does not manage anything to do with the AppObj itself.
    #     '''
    #     pass

    # def _sync_object(self, obj):
    #     ''' Handles only the syncing of an object via the hypergolix
    #     service. Does not manage anything to do with the AppObj itself.
    #     '''
    #     pass

    # def _share_object(self, obj, recipient):
    #     ''' Handles only the sharing of an object via the hypergolix
    #     service. Does not manage anything to do with the AppObj itself.
    #     '''
    #     pass

    # def _freeze_object(self, obj):
    #     ''' Handles only the freezing of an object via the hypergolix
    #     service. Does not manage anything to do with the AppObj itself.
    #     '''
    #     pass

    # def _hold_object(self, obj):
    #     ''' Handles only the holding of an object via the hypergolix
    #     service. Does not manage anything to do with the AppObj itself.
    #     '''
    #     pass

    # def _delete_object(self, obj):
    #     ''' Handles only the deleting of an object via the hypergolix
    #     service. Does not manage anything to do with the AppObj itself.
    #     '''
    #     pass
    
    
class _TestEmbed(Dispatcher, _TEFixture):
    # This is a little gross, but we gotta combine 'em somehow
    pass


class _TestDispatch(HGXCore):
    def __init__(self, *args, **kwargs):
        super().__init__(
            # dispatcher = self, 
            # privateer = Privateer(),
            *args, **kwargs
        )
        self.link_dispatch(_TestEmbed(core=self))
        self.link_privateer(Privateer(core=self))


# class _TestEndpoint(_EndpointBase):
class _TestEndpoint:
    def __init__(self, name, dispatch, apis, app_token=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__name = name
        self._assigned_objs = []
        self._failed_objs = []
        self.dispatch = dispatch
        self.app_token = self.dispatch.new_token()
        self.apis = set(apis)
        
    def notify_object_threadsafe(self, obj, state=None):
        self._assigned_objs.append(obj)
        print('Endpoint ', self.__name, ' incoming: ', obj)
        
    # def send_update(self, obj, state=None):
    #     self._assigned_objs.append(obj)
    #     print('Endpoint ', self.__name, ' updated: ', obj)
        
    def send_delete_threadsafe(self, ghid):
        ''' Notifies the endpoint that the object has been deleted 
        upstream.
        '''
        print('Endpoint ', self.__name, ' received delete: ', obj)
        
    def notify_share_failure_threadsafe(self, obj, recipient):
        self._failed_objs.append(obj)
        print('Endpoint ', self.__name, ' failed: ', obj)
        
    def notify_share_success_threadsafe(self, obj, recipient):
        self._assigned_objs.append(obj)
        print('Endpoint ', self.__name, ' success: ', obj)


# ###############################################
# Testing
# ###############################################
        
        
class TestDispatching(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.persister = MemoryPersister()
        cls.__api_id = bytes(64) + b'1'
        
        cls.agent1 = _TestDispatch(persister=cls.persister)
        cls.dispatch1 = cls.agent1._dispatcher
        cls.endpoint1 = _TestEndpoint(
            dispatch = cls.dispatch1,
            apis = [cls.__api_id],
            # app_token = b'1234',
            name = 'Agent1, ep1',
        )
        cls.dispatch1.register_endpoint(cls.endpoint1)
        
        cls.agent2 = _TestDispatch(persister=cls.persister)
        cls.dispatch2 = cls.agent2._dispatcher
        cls.endpoint2 = _TestEndpoint(
            dispatch = cls.dispatch2,
            apis = [cls.__api_id],
            # app_token = b'5678',
            name = 'Agent2, ep1',
        )
        cls.endpoint3 = _TestEndpoint(
            dispatch = cls.dispatch2,
            apis = [cls.__api_id],
            # app_token = b'9101',
            name = 'Agent2, ep2',
        )
        cls.dispatch2.register_endpoint(cls.endpoint2)
        cls.dispatch2.register_endpoint(cls.endpoint3)
        
    def test_trash(self):
        pt0 = b'I am a sexy stagnant beast.'
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'

        address1 = self.dispatch1.new_object(
            asking_token = self.endpoint1.app_token,
            state = pt0,
            # NOTE THAT APP_TOKEN SHOULD ONLY EVER BE DEFINED for non-private
            # objects!
            app_token = self.endpoint1.app_token,
            api_id = self.__api_id,
            dynamic = False
        )

        address2 = self.dispatch1.new_object(
            asking_token = self.endpoint1.app_token,
            state = pt1,
            api_id = self.__api_id,
            app_token = None,
            dynamic = True
        )
        
        self.dispatch1.share_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2, 
            recipient = self.agent2.whoami, 
        )
        
        self.assertIn(address2, self.dispatch1._oracle)
        self.assertIn(address2, self.dispatch2._oracle)
        
        self.dispatch1.update_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2,
            state = pt2
        )
        
        frozen2 = self.dispatch1.freeze_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2
        )
        
        self.assertIn(address2, self.dispatch1._oracle)
        self.assertEqual(
            self.dispatch1._oracle[frozen2].state, 
            self.dispatch1._oracle[address2].state
        )
        
        address3 = self.dispatch2.new_object(
            asking_token = self.endpoint2.app_token,
            state = pt0,
            app_token = self.endpoint2.app_token,
            api_id = self.__api_id,
            dynamic = False
        )

        address4 = self.dispatch2.new_object(
            asking_token = self.endpoint2.app_token,
            state = pt1,
            api_id = self.__api_id,
            app_token = None,
            dynamic = True
        )
        
        self.dispatch2.hold_object(
            asking_token = self.endpoint2.app_token,
            ghid = address2
        )
        
        self.dispatch1.delete_object(
            asking_token = self.endpoint1.app_token,
            ghid = address1
        )
        
        self.assertNotIn(address1, self.dispatch1._oracle)
        
        self.dispatch1.discard_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2
        )
        
        # Agent1 has only one endpoint following this object so it should be
        # completely deregistered
        self.assertNotIn(address2, self.dispatch1._oracle)
        
        self.dispatch2.discard_object(
            asking_token = self.endpoint2.app_token,
            ghid = address2
        )
        
        # Agent2 has two endpoints following this object so it should persist
        self.assertIn(address2, self.dispatch2._oracle)

        # obj1 = self.dispatch1.new_object(pt0, dynamic=False)
        # obj2 = self.dispatch1.new_object(pt1, dynamic=True)
        
        
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        
    
    # def tearDown(self):
    #     self.server._halt()
        

if __name__ == "__main__":
    logging.basicConfig(filename='logs/dispatch.log', level=logging.DEBUG)
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()