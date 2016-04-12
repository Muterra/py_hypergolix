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

from hypergolix.core import _TestDispatcher

from hypergolix.persisters import LocalhostClient
from hypergolix.persisters import LocalhostServer

from hypergolix.embeds import _TestEmbed

# from hypergolix.embeds import _EmbedBase


class _TestAgent(AgentBase, LocalhostClient, _TestEmbed, _TestDispatcher):
    def __init__(self, *args, **kwargs):
        super().__init__(persister=self, dispatcher=self, *args, **kwargs)


# ###############################################
# Testing
# ###############################################
        
        
class WebsocketsTrashTest(unittest.TestCase):
    def setUp(self):
        self.server = LocalhostServer(port=5926)
        self.server_thread = threading.Thread(
            target = self.server.run,
            daemon = True,
            name = 'server_thread'
        )
        self.server_thread.start()
        
        self.agent1 = _TestAgent(port=5926)
        self.agent2 = _TestAgent(port=5926)
        
    def test_comms(self):
        pt0 = b'I am a sexy stagnant beast.'
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'

        obj1 = self.agent1.new_object(pt0, dynamic=False)
        obj2 = self.agent1.new_object(pt1, dynamic=True)
        
        self.agent1.hand_object(obj1, self.agent2.whoami)
        # As it turns out, the length of this delay is important.
        time.sleep(1)
        obj1_shared = self.agent2.retrieve_recent_handshake()
        self.assertEqual(
            obj1_shared,
            obj1
        )
        
        self.agent1.hand_object(obj2, self.agent2.whoami)
        # As it turns out, the length of this delay is important.
        time.sleep(1)
        obj2_shared = self.agent2.retrieve_recent_handshake()
        self.assertEqual(
            obj2_shared,
            obj2
        )
        
        self.agent1.update_object(obj2, pt2)
        # As it turns out, the length of this delay is important.
        time.sleep(1)
        self.assertEqual(
            obj2_shared,
            obj2
        )            
        print('Successful tests.')
        # self.server._halt()
        
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