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
import queue
import time
import logging
import weakref
import asyncio
import random

from queue import Empty

from golix import Ghid

from hypergolix.utils import LooperTrooper
from hypergolix.utils import Aengel
from hypergolix.utils import SetMap

from hypergolix.dispatch import Dispatcher
from hypergolix.dispatch import _Dispatchable
from hypergolix.dispatch import _AppDef

from hypergolix.ipc import IPCCore
from hypergolix.persistence import SalmonatorNoop

from hypergolix.exceptions import UnknownToken


# ###############################################
# Fixturing
# ###############################################


from _fixtures.ghidutils import make_random_ghid
        
        
class MockGolcore:
    def __init__(self, whoami):
        self.whoami = whoami.ghid
        
        
class MockOracle:
    def __init__(self, whoami):
        self.objs = {}
        self.whoami = whoami.ghid
        
    def get_object(self, gaoclass, ghid, *args, **kwargs):
        return self.objs[ghid]
        
    def new_object(self, gaoclass, *args, **kwargs):
        # Make a random address for the ghid
        obj = _Dispatchable(*args, **kwargs)
        
        # Shit, need to make fixtures for everything for a _GAO to get the
        # dispatchable to work right, and I need to be able to test the 
        # dispatchable. Shit.
        
        MockDispatchable(
            author = self.whoami, 
            dynamic = dynamic, 
            api_id = api_id, 
            state = state,
            frozen = False,
            held = False,
            deleted = False,
            oracle = self,
            *args, **kwargs)
        self.objs[obj.ghid] = obj
        return obj
        
        
class MockRolodex:
    def __init__(self):
        self.shared_objects = {}
        
    def share_object(self, ghid, recipient, requesting_token):
        self.shared_objects[ghid] = recipient, requesting_token
        
        
class MockIPCCore(LooperTrooper):
    def __init__(self, *args, **kwargs):
        self.endpoints = frozenset(random.sample(range(10000000), 10))
        self.callsheet_buffer = collections.deque()
        self.caller_buffer = collections.deque()
        self.arg_buffer = collections.deque()
        
        super().__init__(*args, **kwargs)
    
    async def loop_run(self):
        ''' Just await closure forever.
        '''
        await self._shutdown_init_flag.wait()
        
    async def make_callsheet(self, target):
        ''' Mocking callsheet.
        '''
        return self.endpoints
        
    async def distribute_to_endpoints(self, caller, callsheet, *args):
        ''' Mocks stuff and stuff.
        '''
        self.callsheet_buffer.appendleft(callsheet)
        self.caller_buffer.appendleft(caller)
        self.arg_buffer.appendleft(args)
        
    async def send_delete(self):
        pass
    
    async def send_update(self):
        pass


# ###############################################
# Testing
# ###############################################
        
        
class TestDispatcher(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.token_iters = 500
        super().__init__(*args, **kwargs)
    
    @classmethod
    def setUpClass(cls):
        cls.aengel = Aengel()
        cls.ipccore = MockIPCCore(threaded=True, aengel=cls.aengel)
        
    def setUp(self):
        self.dispatch = Dispatcher()
        self.dispatch.assemble(self.ipccore)
        self.dispatch.bootstrap(
            all_tokens = set(),
            startup_objs = SetMap(),
            private_by_ghid = {}
        )
        
    def test_new_token(self):
        for __ in range(self.token_iters):
            token = self.dispatch.new_token()
            self.assertEqual(len(token), 4)
            self.assertNotIn(token, self.dispatch._all_known_tokens)
            self.dispatch._all_known_tokens.add(token)
            
    def test_reg_app(self):
        for __ in range(self.token_iters):
            appdef = self.dispatch.register_application()
            self.assertIn(appdef.app_token, self.dispatch._all_known_tokens)
            
    def test_start_app(self):
        # These should succeed
        for __ in range(self.token_iters):
            token = self.dispatch.new_token()
            self.dispatch._all_known_tokens.add(token)
            self.dispatch.start_application((token,))
            self.dispatch.start_application(_AppDef(token))
            
        # These should fail
        for __ in range(self.token_iters):
            token = self.dispatch.new_token()
            with self.assertRaises(UnknownToken, 
                                    msg='Failed to block unknown app'):
                self.dispatch.start_application(_AppDef(token))
                
    def test_reg_startup(self):
        ghid = make_random_ghid()
        token = self.dispatch.new_token()
        with self.assertRaises(UnknownToken, 
                                msg='Failed to block unknown app.'):
            self.dispatch.register_startup(token, ghid)
        self.assertNotIn(token, self.dispatch._startup_by_token)
        self.assertNotIn(ghid, self.dispatch._startup_by_token.get_any(token))
        
        token = self.dispatch.new_token()
        ghid = make_random_ghid()
        self.dispatch._all_known_tokens.add(token)
        self.dispatch.register_startup(token, ghid)
        self.assertIn(token, self.dispatch._startup_by_token)
        self.assertIn(ghid, self.dispatch._startup_by_token.get_any(token))
        
        ghids = self.dispatch.get_startup_objs(token)
        self.assertIn(ghid, ghids)
        self.assertEqual(len(ghids), 1)
        
        ghid = make_random_ghid()
        self.dispatch.register_startup(token, ghid)
        
        ghids = self.dispatch.get_startup_objs(token)
        self.assertIn(ghid, ghids)
        self.assertEqual(len(ghids), 2)
        
        self.dispatch.deregister_startup(token, ghid)
        ghids = self.dispatch.get_startup_objs(token)
        self.assertNotIn(ghid, ghids)
        self.assertEqual(len(ghids), 1)
        
    def test_reg_private(self):
        ghid = make_random_ghid()
        token = self.dispatch.new_token()
        with self.assertRaises(UnknownToken, 
                                msg='Failed to block unknown app.'):
            self.dispatch.register_private(token, ghid)
        self.assertNotIn(ghid, self.dispatch._private_by_ghid)
        
        token = self.dispatch.new_token()
        ghid = make_random_ghid()
        self.dispatch._all_known_tokens.add(token)
        self.dispatch.register_private(token, ghid)
        self.assertIn(ghid, self.dispatch._private_by_ghid)
        self.assertEqual(self.dispatch._private_by_ghid[ghid], token)
        
        self.assertEqual(self.dispatch.get_parent_token(ghid), token)
        self.assertEqual(
            self.dispatch.get_parent_token(make_random_ghid()), 
            None
        )
        
        self.dispatch.make_public(ghid)
        self.assertNotIn(ghid, self.dispatch._private_by_ghid)
        self.assertEqual(self.dispatch.get_parent_token(ghid), None)
        
        with self.assertRaises(ValueError, msg='Made an unknown ghid public.'):
            self.dispatch.make_public(make_random_ghid())
            
    def test_notify(self):
        # Test update notification
        ghid = make_random_ghid()
        self.dispatch.notify(ghid)
        argbuffer = self.ipccore.arg_buffer.popleft()
        self.assertEqual(ghid, argbuffer[0])
        callsheetbuffer = self.ipccore.callsheet_buffer.popleft()
        self.assertEqual(self.ipccore.endpoints, callsheetbuffer)
        callerbuffer = self.ipccore.caller_buffer.popleft()
        self.assertEqual(callerbuffer, self.ipccore.send_update)
        
        # Test deletion notification
        ghid = make_random_ghid()
        self.dispatch.notify(ghid, deleted=True)
        argbuffer = self.ipccore.arg_buffer.popleft()
        self.assertEqual(ghid, argbuffer[0])
        callsheetbuffer = self.ipccore.callsheet_buffer.popleft()
        self.assertEqual(self.ipccore.endpoints, callsheetbuffer)
        callerbuffer = self.ipccore.caller_buffer.popleft()
        self.assertEqual(callerbuffer, self.ipccore.send_delete)
        
        
class TestDispatchable(unittest.TestCase):
    @unittest.expectedFailure
    def test_dispatchable(self):
        raise NotImplementedError()
        

if __name__ == "__main__":
    from _fixtures import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()