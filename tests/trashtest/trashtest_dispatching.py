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

from golix import Guid

from hypergolix import AgentBase

# from hypergolix.persisters import LocalhostClient
# from hypergolix.persisters import LocalhostServer
from hypergolix.persisters import MemoryPersister

from hypergolix.core import Dispatcher

from hypergolix.utils import DispatchObj
# from hypergolix.utils import AppObj
# from hypergolix.utils import RawObj

from hypergolix.embeds import _TestEmbed

from hypergolix.ipc_hosts import _TestEndpoint

# from hypergolix.ipc_hosts import _EmbeddedIPC


class _TestDispatch(AgentBase, Dispatcher, _TestEmbed):
    def __init__(self, *args, **kwargs):
        super().__init__(dispatcher=self, *args, **kwargs)


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
            name = 'Agent1, ep1'
        )
        cls.agent1.register_endpoint(cls.endpoint1)
        
        cls.agent2 = _TestDispatch(persister=cls.persister)
        cls.endpoint2 = _TestEndpoint(
            dispatch = cls.agent2,
            apis = [cls.__api_id],
            name = 'Agent2, ep1'
        )
        cls.endpoint3 = _TestEndpoint(
            dispatch = cls.agent2,
            apis = [cls.__api_id],
            name = 'Agent2, ep2'
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
            guid = address2, 
            recipient = self.agent2.whoami, 
        )
        
        self.assertIn(address2, self.agent1._dynamic_by_guid)
        self.assertIn(address2, self.agent1._state_by_guid)
        self.assertIn(address2, self.agent1._author_by_guid)
        self.assertIn(address2, self.agent1._token_by_guid)
        self.assertIn(address2, self.agent1._api_by_guid)
        
        self.assertIn(address2, self.agent2._dynamic_by_guid)
        self.assertIn(address2, self.agent2._state_by_guid)
        self.assertIn(address2, self.agent2._author_by_guid)
        self.assertIn(address2, self.agent2._token_by_guid)
        self.assertIn(address2, self.agent2._api_by_guid)
        
        self.agent1.update_object(
            asking_token = self.endpoint1.app_token,
            guid = address2,
            state = pt2
        )
        
        frozen2 = self.agent1.freeze_object(
            asking_token = self.endpoint1.app_token,
            guid = address2
        )
        
        self.assertIn(address2, self.agent1._dynamic_by_guid)
        self.assertIn(address2, self.agent1._state_by_guid)
        self.assertIn(address2, self.agent1._author_by_guid)
        self.assertIn(address2, self.agent1._token_by_guid)
        self.assertIn(address2, self.agent1._api_by_guid)
        self.assertEqual(self.agent1._state_by_guid[frozen2], self.agent1._state_by_guid[address2])
        
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
            guid = address2
        )

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
    unittest.main()