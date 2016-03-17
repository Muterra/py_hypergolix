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

# These are normal imports
from hypergolix.persisters import MemoryPersister
from hypergolix import DynamicObject
from hypergolix import StaticObject
from hypergolix import NakError
from hypergolix import PersistenceWarning
from hypergolix import Agent

# This is a semi-normal import
from golix.utils import _dummy_guid

# These are abnormal imports
from golix import Guid
from golix import ThirdParty
from golix import SecondParty
from golix import FirstParty

# ###############################################
# Testing
# ###############################################

    
class ObjectTrashtest(unittest.TestCase):
    def setUp(self):
        pass
        
    def test_trash(self):
        dummy_state = b'0'
        
        stat1 = StaticObject(
            address = _dummy_guid,
            author = _dummy_guid,
            state = dummy_state,
        )
        dyn1 = DynamicObject(
            address = _dummy_guid,
            author = _dummy_guid,
            _buffer = collections.deque([dummy_state], maxlen=7),
        )
        
        with self.assertRaises(AttributeError, 
            msg='Failed to prevent private attr assignment in static obj.'):
                stat1._state = 5
        
        with self.assertRaises(AttributeError, 
            msg='Failed to prevent public attr assignment in static obj.'):
                stat1.state = 5
                stat1.author = _dummy_guid
                stat1.address = _dummy_guid
        
        with self.assertRaises(AttributeError, 
            msg='Failed to prevent public attr assignment in dynamic obj.'):
                stat1.state = 5
                stat1.author = _dummy_guid
                stat1.address = _dummy_guid
                stat1.buffer = [_dummy_guid]
                
        self.assertEqual(stat1.author, _dummy_guid)
        self.assertEqual(dyn1.author, _dummy_guid)
        self.assertEqual(stat1.address, _dummy_guid)
        self.assertEqual(dyn1.address, _dummy_guid)
        self.assertEqual(stat1.state, dummy_state)
        self.assertEqual(dyn1.state, dummy_state)
        self.assertEqual(dyn1.buffer, (dummy_state,))
                
        repr(stat1)
        repr(dyn1)
            
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        
        
class AgentTrashTest(unittest.TestCase):
    def setUp(self):
        self.persister = MemoryPersister()
        self.agent = Agent(persister=self.persister)
        
    def test_trash(self):
        obj1 = self.agent.new_static(b'Hello, world?')
        self.agent.delete_object(obj1)
        obj2 = self.agent.new_dynamic(b'Hello, world?')
        self.agent.update_dynamic(obj2, data=b'Hiyaback!')
        

if __name__ == "__main__":
    unittest.main()