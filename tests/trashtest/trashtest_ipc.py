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
import asyncio
import random
import traceback
import logging
import os
import copy

from hypergolix.dispatch import _Dispatchable
from hypergolix.dispatch import Dispatcher
from hypergolix.dispatch import _AppDef

from hypergolix.persistence import SalmonatorNoop

from hypergolix.utils import Aengel
from hypergolix.utils import SetMap

from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicServer
from hypergolix.comms import WSBasicClient

from hypergolix.ipc import IPCCore
from hypergolix.ipc import IPCEmbed

from hypergolix.objproxy import ProxyBase

from golix import Ghid

# from hypergolix.embeds import WebsocketsEmbed


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
        
        
class MockGolcore:
    def __init__(self, whoami):
        self.whoami = whoami.ghid
        
        
class MockOracle:
    def __init__(self, whoami):
        self.objs = {}
        self.whoami = whoami.ghid
        
    def get_object(self, gaoclass, ghid, *args, **kwargs):
        return self.objs[ghid]
        
    def new_object(self, state, dynamic, api_id, *args, **kwargs):
        # Make a random address for the ghid
        obj = MockDispatchable(
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
        
        
class MockDispatch:
    def __init__(self):
        self.startups = {}
        self.parents = {}
        self.tokens = set()
        
    def get_parent_token(self, ghid):
        if ghid in self.parents:
            return self.parents[ghid]
        else:
            return None
        
    def get_startup_objs(self, token):
        return frozenset([self.startups[token]])
        
    def register_startup(self, token, ghid):
        self.startups[token] = ghid
        
    def register_private(self, token, ghid):
        self.parents[ghid] = token
        
    def start_application(self, appdef):
        if appdef.app_token not in self.tokens:
            raise RuntimeError()
        else:
            return True
        
    def register_application(self):
        token = os.urandom(4)
        self.tokens.add(token)
        return _AppDef(token)


class MockDispatchable:
    def __init__(self, dispatch, ipc_core, author, dynamic, api_id, frozen, held, deleted, state, oracle, *args, **kwargs):
        self.ghid = Ghid.from_bytes(b'\x01' + os.urandom(64))
        self.author = author
        self.dynamic = dynamic
        self.api_id = api_id
        # Don't forget that dispatchables assign state as a _DispatchableState
        self.state = state[1]
        self.frozen = frozen
        self.held = held
        self.deleted = deleted
        self.ipccore = ipc_core
        
        self.oracle = oracle
        self.dispatch = dispatch
        
    def __eq__(self, other):
        comp = True
        
        try:
            comp &= (self.ghid == other.ghid)
            comp &= (self.author == other.author)
            comp &= (self.dynamic == other.dynamic)
            comp &= (self.api_id == other.api_id)
            comp &= (self.state == other.state)
            comp &= (self.private == other.private)
            comp &= (self.frozen == other.frozen)
            comp &= (self.held == other.held)
            comp &= (self.deleted == other.deleted)
            
        except:
            comp = False
            
        return comp
        
    @property
    def private(self):
        return self.ghid in self.dispatch.parents
        
    def freeze(self):
        frozen = type(self)(
            dispatch = self.dispatch, 
            ipc_core = self.ipccore,
            oracle = self.oracle, 
            deleted = self.deleted,
            held = self.held,
            frozen = self.frozen,
            state = (None, self.state),
            api_id = self.api_id,
            dynamic = self.dynamic,
            author = self.author,
        )
        frozen.frozen = True
        self.oracle.objs[frozen.ghid] = frozen
        return frozen.ghid
        
    def update(self, state):
        self.state = state
        
    def hold(self):
        self.held = True
        
    def delete(self):
        self.deleted = True


# ###############################################
# Testing
# ###############################################
        
        
class WebsocketsIPCTrashTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._notifier_1 = threading.Event()
        self._notifier_2 = threading.Event()
        self._timeout = 1
        
    def _objhandler_1(self, obj):
        # Quick and dirty.
        self._notifier_11.set()
        
    def notification_checker_1(self, timeout=None):
        if timeout is None:
            timeout = self._timeout
            
        result = self._notifier_1.wait(timeout)
        self._notifier_1.clear()
        return result
        
    def _objhandler_2(self, obj):
        # Quick and dirty.
        self._notifier_2.set()
        
    def notification_checker_2(self, timeout=None):
        if timeout is None:
            timeout = self._timeout
            
        result = self._notifier_2.wait(timeout)
        self._notifier_2.clear()
        return result
        
    def setUp(self):
        # SO BEGINS SERVER SETUP!
        self.aengel = Aengel()
        self.ipccore = IPCCore(
            aengel = self.aengel,
            debug = True,
            threaded = True,
            thread_name = 'IPCCore'
        )
        
        self.golcore = MockGolcore(TEST_AGENT1)
        self.oracle = MockOracle(TEST_AGENT1)
        self.dispatch = MockDispatch()
        self.rolodex = MockRolodex()
        self.salmonator = SalmonatorNoop()
        
        self.ipccore.assemble(
            self.golcore, self.oracle, self.dispatch, self.rolodex, 
            self.salmonator
        )
        self.ipccore.bootstrap(
            incoming_shares = set(),
            orphan_acks = SetMap(),
            orphan_naks = SetMap()
        )
        
        self.ipccore.add_ipc_server(
            'websockets', 
            WSBasicServer, 
            host = 'localhost',
            port = 4628,
            aengel = self.aengel,
            debug = True,
            threaded = True,
            tls = False,
            thread_name = 'IPC server'
        )
        
        # AND THUS BEGINS CLIENT/APPLICATION SETUP!
        self.app1 = IPCEmbed(
            aengel = self.aengel,
            debug = True,
            threaded = True,
            thread_name = 'app1em'
        )
        self.app1.add_ipc_threadsafe(
            WSBasicClient,
            host = 'localhost',
            port = 4628,
            debug = True,
            tls = False,
            aengel = self.aengel,
            threaded = True,
            thread_name = 'app1WS',
        )
        # self.app1endpoint = self.ipccore.any_session
        
        self.app2 = IPCEmbed(
            aengel = self.aengel,
            debug = True,
            threaded = True,
            thread_name = 'app2em'
        )
        self.app2.add_ipc_threadsafe(
            WSBasicClient,
            host = 'localhost',
            port = 4628,
            debug = True,
            tls = False,
            aengel = self.aengel,
            threaded = True,
            thread_name = 'app2WS',
        )
        # endpoints = set(self.ipccore.sessions)
        # self.app2endpoint = list(endpoints - {self.app1endpoint})[0]
        
        self.__api_id = bytes(64) + b'1'
        
    def test_client1(self):
        # Test app tokens.
        # -----------
        token1 = self.app1.get_new_token_threadsafe()
        self.assertIn(token1, self.dispatch.tokens)
        self.assertIn(token1, self.ipccore._endpoint_from_token)
        self.assertTrue(isinstance(token1, bytes))
        
        with self.assertRaises(RuntimeError, 
            msg='IPC allowed concurrent token re-registration.'):
                self.app2.set_existing_token_threadsafe(token1)
                
        # Good, that didn't work
        self.app2.get_new_token_threadsafe()
        token2 = self.app2.app_token
        self.assertIn(token2, self.dispatch.tokens)
        self.assertIn(token2, self.ipccore._endpoint_from_token)
        self.assertTrue(isinstance(token2, bytes))
        
        # Okay, now I'm satisfied about tokens. Get on with it already!
        pt0 = b'I am a sexy stagnant beast.'
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'
        
        # Test whoami
        # -----------
        whoami = self.app1.whoami
        self.assertEqual(whoami, TEST_AGENT1.ghid)
        
        # Test registering an api_id
        # -----------
        self.app1.register_share_handler_threadsafe(
            self.__api_id, 
            ProxyBase,
            self._objhandler_1
        )
        self.app2.register_share_handler_threadsafe(
            self.__api_id, 
            ProxyBase,
            self._objhandler_2
        )
        registered_apis = \
            self.ipccore._endpoints_from_api.get_any(self.__api_id)
        self.assertEqual(len(registered_apis), 2)
        
        # Test creating a private, static new object with that api_id
        # -----------
        obj1 = self.app1.new_threadsafe(
            cls = ProxyBase,
            state = pt0,
            api_id = self.__api_id,
            dynamic = False,
            private = True
        )
        self.assertIn(obj1.hgx_ghid, self.oracle.objs)
        # Private registration = app2 should not get a notification.
        self.assertFalse(self.notification_checker_2(.25))
        # Nor should app1, who created it.
        self.assertFalse(self.notification_checker_1(.05))
        self.assertNotIn(obj1.hgx_ghid, self.dispatch.startups)
        # Private, so we should see it.
        self.assertIn(obj1.hgx_ghid, self.dispatch.parents)
        
        # And again, but dynamic, and not private.
        # -----------
        obj2 = self.app1.new_threadsafe(
            cls = ProxyBase,
            state = pt1,
            api_id = self.__api_id,
            dynamic = True,
        )
        self.assertTrue(obj2.hgx_dynamic)
        self.assertIn(obj2.hgx_ghid, self.oracle.objs)
        # Since app2 also registered this API, it should get a notification.
        self.assertTrue(self.notification_checker_2())
        # But app1 should not, because it created the object.
        self.assertFalse(self.notification_checker_1(.05))
        self.assertNotIn(obj2.hgx_ghid, self.dispatch.startups)
        self.assertNotIn(obj2.hgx_ghid, self.dispatch.parents)
        # Also make sure we have listeners for it
        dispatchable2 = self.oracle.objs[obj2.hgx_ghid]
        # Note that currently, as we're immediately sending the whole object to
        # apps, they are getting added as listeners immediately.
        # self.assertEqual(
        #     len(self.ipccore._update_listeners.get_any(dispatchable2.ghid)), 
        #     1
        # )
        self.assertEqual(
            len(self.ipccore._update_listeners.get_any(dispatchable2.ghid)), 
            2
        )
        
        # Test object retrieval from app2
        # -----------
        # Huh, interestingly this shouldn't fail, if the second app is able to
        # directly guess the right address for the private object.
        joint1 = self.app2.get_threadsafe(ProxyBase, obj1.hgx_ghid)
        self.assertEqual(obj1, joint1)
        
        joint2 = self.app2.get_threadsafe(ProxyBase, obj2.hgx_ghid)
        self.assertEqual(obj2, joint2)
        self.assertEqual(
            len(self.ipccore._update_listeners.get_any(dispatchable2.ghid)), 
            2
        )
        
        # Test object updates
        # -----------
        obj2.hgx_state = pt2
        obj2.hgx_push_threadsafe()
        # Note that we have to wait for the callback background process to 
        # complete
        time.sleep(.1)
        self.assertEqual(obj2, joint2)
        
        # Test object sharing
        # -----------
        obj2.hgx_share_threadsafe(TEST_AGENT2.ghid)
        self.assertIn(obj2.hgx_ghid, self.rolodex.shared_objects)
        recipient, requesting_token = self.rolodex.shared_objects[obj2.hgx_ghid]
        self.assertEqual(recipient, TEST_AGENT2.ghid)
        self.assertEqual(requesting_token, token1)
        
        # Test object freezing
        # -----------
        frozen2 = obj2.hgx_freeze_threadsafe()
        self.assertEqual(frozen2.hgx_state, obj2.hgx_state)
        self.assertIn(frozen2.hgx_ghid, self.oracle.objs)
        
        # Test object holding
        # -----------
        frozen2.hgx_hold_threadsafe()
        dispatchable3 = self.oracle.objs[frozen2.hgx_ghid]
        self.assertTrue(dispatchable3.held)
        
        # Test object discarding
        # -----------
        joint2.hgx_discard_threadsafe()
        self.assertFalse(joint2._isalive_3141592)
        self.assertEqual(
            len(self.ipccore._update_listeners.get_any(dispatchable2.ghid)), 
            1
        )
        self.assertFalse(dispatchable2.deleted)
        
        # Test object discarding
        # -----------
        dispatchable1 = self.oracle.objs[obj1.hgx_ghid]
        obj1.hgx_delete_threadsafe()
        self.assertTrue(dispatchable1.deleted)
        self.assertFalse(obj1._isalive_3141592)
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig('debug')
    
    unittest.main()