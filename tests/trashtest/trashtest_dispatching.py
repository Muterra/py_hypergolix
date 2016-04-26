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
        
        
class TestAppObj(unittest.TestCase):
    def setUp(self):
        self.persister = MemoryPersister()
        self.__api_id = bytes(64) + b'1'
        
        self.agent1 = _TestDispatch(persister=self.persister)
        self.endpoint1 = _TestEndpoint(
            dispatch = self.agent1,
            apis = [self.__api_id],
            name = 'Agent1, ep1'
        )
        self.agent1.register_endpoint(self.endpoint1)
        # This is fucking gross. Oh well, that's why it's a trashtest.
        self.agent1.app_token = self.endpoint1.app_token
        
        self.agent2 = _TestDispatch(persister=self.persister)
        self.endpoint2 = _TestEndpoint(
            dispatch = self.agent2,
            apis = [self.__api_id],
            name = 'Agent2, ep1'
        )
        self.endpoint3 = _TestEndpoint(
            dispatch = self.agent2,
            apis = [self.__api_id],
            name = 'Agent2, ep2'
        )
        self.agent2.register_endpoint(self.endpoint2)
        self.agent2.register_endpoint(self.endpoint3)
        # This is fucking gross. See above.
        self.agent2.app_token = self.endpoint2.app_token
        
    def test_appobj(self):
        pt0 = b'I am a sexy stagnant beast.'
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'

        obj1 = DispatchObj(
            dispatch = self.agent1,
            state = pt0,
            app_token = self.agent1.app_token,
            api_id = self.__api_id,
            dynamic = False
        )

        obj2 = DispatchObj(
            dispatch = self.agent1,
            state = pt1,
            api_id = self.__api_id,
            dynamic = True
        )
        
        # Manually calling share here to bypass things. Technically this should
        # be called from the endpoint, but let's just do it like this as a test
        # fixture.
        self.agent1.share_object(
            obj = obj2, 
            recipient = self.agent2.whoami, 
            requesting_token = self.agent1.app_token
        )
        obj2.update(pt2)

        obj3 = DispatchObj(
            dispatch = self.agent2,
            state = pt0,
            app_token = self.agent2.app_token,
            api_id = self.__api_id,
            dynamic = False
        )

        obj2 = DispatchObj(
            dispatch = self.agent2,
            state = pt1,
            api_id = self.__api_id,
            dynamic = True
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