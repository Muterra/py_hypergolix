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

from hypergolix.persisters import MemoryPersister

from hypergolix.dispatch import _TestDispatcher

from hypergolix.privateer import Privateer

from hypergolix import HGXCore

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
        
        
# JFC seriously? In HERE?! This is so ridiculously out of scope. This is just
# disgraceful and gross. TODO: make real actual test fixtures and suite.
from hypergolix.utils import RawObj
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
        

class _TestClient(HGXCore, MemoryPersister, _TestEmbed, _TestDispatcher):
    def __init__(self):
        super().__init__(persister=self, privateer=Privateer())
        self.link_dispatch(self)
        
    def _discard_object(*args, **kwargs):
        pass


class _TestAgent_SharedPersistence(HGXCore, _TestEmbed, _TestDispatcher):
    def __init__(self, *args, **kwargs):
        super().__init__(privateer=Privateer(), *args, **kwargs)
        self.link_dispatch(self)
        
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
        cls.dispatcher1 = cls.agent1
        cls.dispatcher2 = cls.agent2
        
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
        obj4 = self.agent1.new_object(state=obj3.address, dynamic=True)
        obj5 = self.agent1.new_object(state=obj4.address, dynamic=True)
        
        # self.assertEqual(obj3.state, pt3)
        # self.assertEqual(obj4.state, pt3)
        # self.assertEqual(obj5.state, pt3)
        
        obj6 = self.agent1.freeze_object(obj4)
        # Note: at some point that was producing incorrect bindings and didn't
        # error out at the persister. Is that still a problem, or has it been 
        # resolved?
        self.agent1.update_object(obj4, pt4)
        
        # self.assertEqual(obj6.state, pt3)
        # self.assertEqual(obj4.state, pt4)
        # self.assertEqual(obj5.state, pt4)
        
    def test_together(self):
        contact1 = self.agent1.whoami
        contact2 = self.agent2.whoami
        
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        
        obj1 = self.agent1.new_object(pt1, dynamic=True)
        obj1s1 = self.agent1.freeze_object(obj1)
        
        self.agent1.hand_object(obj1s1, contact2)
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
        self.agent1.hand_object(obj1, contact2)
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
        obj1 = self.agent1.new_object(pt1, dynamic=False)
        self.agent1.delete_object(obj1)
        obj2 = self.agent1.new_object(pt1, dynamic=True)
        self.agent1.update_object(obj2, state=pt2)
        self.agent1.delete_object(obj2)
        # Test dynamic linking
        obj3 = self.agent1.new_object(pt3, dynamic=False)
        obj4 = self.agent1.new_object(state=obj3.address, dynamic=True)
        obj5 = self.agent1.new_object(state=obj4.address, dynamic=True)
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
    logging.basicConfig(filename='logs/core.log', level=logging.DEBUG)
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()