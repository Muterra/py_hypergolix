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
from hypergolix import AgentBase

from hypergolix.persisters import MemoryPersister
from hypergolix.core import Dispatcher
from hypergolix.utils import DispatchObj
from hypergolix.ipc import _TestEmbed
# from hypergolix.ipc import _EndpointBase


# ###############################################
# Fixtures
# ###############################################


class _TestDispatch(AgentBase, Dispatcher, _TestEmbed):
    def __init__(self, *args, **kwargs):
        super().__init__(dispatcher=self, *args, **kwargs)
        
    def _discard_object(*args, **kwargs):
        pass


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
        cls.endpoint1 = _TestEndpoint(
            dispatch = cls.agent1,
            apis = [cls.__api_id],
            # app_token = b'1234',
            name = 'Agent1, ep1',
        )
        cls.agent1.register_endpoint(cls.endpoint1)
        
        cls.agent2 = _TestDispatch(persister=cls.persister)
        cls.endpoint2 = _TestEndpoint(
            dispatch = cls.agent2,
            apis = [cls.__api_id],
            # app_token = b'5678',
            name = 'Agent2, ep1',
        )
        cls.endpoint3 = _TestEndpoint(
            dispatch = cls.agent2,
            apis = [cls.__api_id],
            # app_token = b'9101',
            name = 'Agent2, ep2',
        )
        cls.agent2.register_endpoint(cls.endpoint2)
        cls.agent2.register_endpoint(cls.endpoint3)
        
    def test_trash(self):
        pt0 = b'I am a sexy stagnant beast.'
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'

        address1 = self.agent1.new_object(
            asking_token = self.endpoint1.app_token,
            state = pt0,
            # NOTE THAT APP_TOKEN SHOULD ONLY EVER BE DEFINED for non-private
            # objects!
            app_token = self.endpoint1.app_token,
            api_id = self.__api_id,
            dynamic = False
        )

        address2 = self.agent1.new_object(
            asking_token = self.endpoint1.app_token,
            state = pt1,
            api_id = self.__api_id,
            app_token = None,
            dynamic = True
        )
        
        self.agent1.share_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2, 
            recipient = self.agent2.whoami, 
        )
        
        self.assertIn(address2, self.agent1._oracle)
        self.assertIn(address2, self.agent2._oracle)
        
        self.agent1.update_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2,
            state = pt2
        )
        
        frozen2 = self.agent1.freeze_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2
        )
        
        self.assertIn(address2, self.agent1._oracle)
        self.assertEqual(
            self.agent1._oracle[frozen2].state, 
            self.agent1._oracle[address2].state
        )
        
        address3 = self.agent2.new_object(
            asking_token = self.endpoint2.app_token,
            state = pt0,
            app_token = self.endpoint2.app_token,
            api_id = self.__api_id,
            dynamic = False
        )

        address4 = self.agent2.new_object(
            asking_token = self.endpoint2.app_token,
            state = pt1,
            api_id = self.__api_id,
            app_token = None,
            dynamic = True
        )
        
        self.agent2.hold_object(
            asking_token = self.endpoint2.app_token,
            ghid = address2
        )
        
        self.agent1.delete_object(
            asking_token = self.endpoint1.app_token,
            ghid = address1
        )
        
        self.assertNotIn(address1, self.agent1._oracle)
        
        self.agent1.discard_object(
            asking_token = self.endpoint1.app_token,
            ghid = address2
        )
        
        # Agent1 has only one endpoint following this object so it should be
        # completely deregistered
        self.assertNotIn(address2, self.agent1._oracle)
        
        self.agent2.discard_object(
            asking_token = self.endpoint2.app_token,
            ghid = address2
        )
        
        # Agent2 has two endpoints following this object so it should persist
        self.assertIn(address2, self.agent2._oracle)

        # obj1 = self.agent1.new_object(pt0, dynamic=False)
        # obj2 = self.agent1.new_object(pt1, dynamic=True)
        
        
        
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