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

from hypergolix.exceptions import UnknownToken


# ###############################################
# Fixturing
# ###############################################


from _fixtures.ghidutils import make_random_ghid


# ###############################################
# Testing
# ###############################################
        
        
@unittest.skip('Deprecated.')
class TestDispatcher(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.token_iters = 500
        super().__init__(*args, **kwargs)
    
    @classmethod
    def setUpClass(cls):
        cls.aengel = Aengel()
        
    def setUp(self):
        self.dispatch = Dispatcher()
        self.dispatch.assemble()
        self.dispatch.bootstrap(
            all_tokens = set(),
            startup_objs = {},
            private_by_ghid = {},
            token_lock = threading.Lock()
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
        
        token = self.dispatch.new_token()
        ghid = make_random_ghid()
        self.dispatch._all_known_tokens.add(token)
        self.dispatch.register_startup(token, ghid)
        self.assertIn(token, self.dispatch._startup_by_token)
        self.assertEqual(ghid, self.dispatch._startup_by_token[token])
        
        ghid2 = self.dispatch.get_startup_obj(token)
        self.assertEqual(ghid, ghid2)
        
        self.dispatch.deregister_startup(token)
        self.assertEqual(self.dispatch.get_startup_obj(token), None)
        
        ghid = make_random_ghid()
        self.dispatch.register_startup(token, ghid)
        
        ghid2 = self.dispatch.get_startup_obj(token)
        self.assertEqual(ghid, ghid2)
        
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
        
        
@unittest.skip('Deprecated.')
class TestDispatchable(unittest.TestCase):
    @unittest.expectedFailure
    def test_dispatchable(self):
        raise NotImplementedError()
            
    @unittest.expectedFailure
    def test_notify(self):
        raise NotImplementedError()
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
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
