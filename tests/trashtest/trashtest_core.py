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
from hypergolix.core import _TestDispatcher

from hypergolix import AgentBase
from hypergolix import StaticObject
from hypergolix import DynamicObject

from hypergolix.exceptions import NakError
from hypergolix.exceptions import PersistenceWarning

from hypergolix.embeds import _TestEmbed

# from hypergolix.embeds import _EmbedBase

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
        

class _TestClient(AgentBase, MemoryPersister, _TestEmbed, _TestDispatcher):
    def __init__(self):
        super().__init__(persister=self, dispatcher=self)


class _TestAgent_SharedPersistence(AgentBase, _TestEmbed, _TestDispatcher):
    def __init__(self, *args, **kwargs):
        super().__init__(dispatcher=self, *args, **kwargs)
        
    def subscribe(self, guid, callback):
        ''' We're not testing this right now but we need to suppress 
        warnings about it for EmbedBase. Pass everything straight to the
        persister.
        '''
        self.persister.subscribe(guid, callback)

    
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
        self.agent1 = _TestAgent_SharedPersistence(
            persister = self.persister
        )
        self.agent2 = _TestAgent_SharedPersistence(
            persister = self.persister
        )
        self.dispatcher1 = self.agent1
        self.dispatcher2 = self.agent2
        
    def test_alone(self):
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'
        
        # Create, test, and delete a static object
        obj1 = self.agent1.new_object(pt1, dynamic=False)
        self.assertEqual(obj1.state, pt1)
        
        self.agent1.delete_object(obj1)
        with self.assertRaises(NakError, msg='Agent failed to delete.'):
            self.persister.get(obj1.address)
        
        # Create, test, update, test, and delete a dynamic object
        obj2 = self.agent1.new_object(pt1, dynamic=True)
        self.assertEqual(obj2.state, pt1)
        
        self.agent1.update_object(obj2, state=pt2)
        self.assertEqual(obj2.state, pt2)
        
        self.agent1.delete_object(obj2)
        with self.assertRaises(NakError, msg='Agent failed to delete.'):
            self.persister.get(obj2.address)
        
        # Test dynamic linking
        obj3 = self.agent1.new_object(pt3, dynamic=False)
        obj4 = self.agent1.new_object(state=obj3, dynamic=True)
        obj5 = self.agent1.new_object(state=obj4, dynamic=True)
        
        self.assertEqual(obj3.state, pt3)
        self.assertEqual(obj4.state, pt3)
        self.assertEqual(obj5.state, pt3)
        
        obj6 = self.agent1.freeze_object(obj4)
        # Note: at some point that was producing incorrect bindings and didn't
        # error out at the persister. Is that still a problem, or has it been 
        # resolved?
        self.agent1.update_object(obj4, pt4)
        
        self.assertEqual(obj6.state, pt3)
        self.assertEqual(obj4.state, pt4)
        self.assertEqual(obj5.state, pt4)
        
    def test_together(self):
        contact1 = self.agent1.whoami
        contact2 = self.agent2.whoami
        
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        
        obj1 = self.agent1.new_object(pt1, dynamic=True)
        obj1s1 = self.agent1.freeze_object(obj1)
        
        self.agent1.hand_object(obj1s1, contact2)
        self.assertIn(obj1s1.address, self.agent2._secrets)
        self.assertEqual(
            self.agent1._secrets[obj1s1.address], 
            self.agent2._secrets[obj1s1.address]
        )
        obj1s1_shared = self.dispatcher2.retrieve_recent_handshake()
        self.assertEqual(
            obj1s1_shared, obj1s1
        )
        
        # Test handshakes using dynamic objects
        self.agent1.hand_object(obj1, contact2)
        self.assertIn(obj1.address, self.agent2._historian)
        # Make sure this doesn't error, checking that it's in secrets
        self.agent2._get_secret(obj1.address)
        obj1_shared = self.dispatcher2.retrieve_recent_handshake()
        self.assertEqual(
            obj1,
            obj1_shared
        )
        
        # Awesome, now let's check updating them -- including subscriptions, 
        # and that the recipient correctly ratchets the key and updates state.
        self.agent1.update_object(obj1, pt2)
        self.assertEqual(
            obj1.state,
            pt2
        )
        self.assertEqual(
            obj1,
            obj1_shared
        )
            
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        
        
class ClientTrashTest(unittest.TestCase):
    def setUp(self):
        self.agent1 = _TestClient()
        self.agent2 = _TestClient()
        
    def test_alone(self):
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'
        
        # Create, test, and delete a static object
        obj1 = self.agent1.new_object(pt1, dynamic=False)
        self.agent1.delete_object(obj1)
        obj2 = self.agent1.new_object(pt1, dynamic=True)
        self.agent1.update_object(obj2, state=pt2)
        self.agent1.delete_object(obj2)
        # Test dynamic linking
        obj3 = self.agent1.new_object(pt3, dynamic=False)
        obj4 = self.agent1.new_object(state=obj3, dynamic=True)
        obj5 = self.agent1.new_object(state=obj4, dynamic=True)
        obj6 = self.agent1.freeze_object(obj4)
        self.agent1.update_object(obj4, pt4)
            
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        

if __name__ == "__main__":
    unittest.main()