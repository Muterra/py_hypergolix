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

import unittest
import threading
import time
import os
import warnings
import collections
import IPython
import asyncio
import random
import traceback
import logging
import copy
import weakref

from loopa import TaskCommander
from loopa.utils import await_coroutine_threadsafe

from hypergolix.comms import BasicServer
from hypergolix.comms import WSConnection
from hypergolix.comms import ConnectionManager

from hypergolix.core import GolixCore
from hypergolix.core import Oracle

from hypergolix.dispatch import _Dispatchable
from hypergolix.dispatch import Dispatcher
from hypergolix.dispatch import _AppDef

from hypergolix.remotes import Salmonator
from hypergolix.rolodex import Rolodex

from hypergolix.utils import Aengel
from hypergolix.utils import SetMap
from hypergolix.utils import WeakSetMap
from hypergolix.utils import ApiID

# from hypergolix.objproxy import ProxyBase

from hypergolix.embed import HGXLink

from hypergolix.objproxy import ObjCore

from golix import Ghid

from hypergolix.exceptions import HypergolixException
from hypergolix.exceptions import IPCError
from hypergolix.exceptions import HGXLinkError

# Imports within the scope of tests

from hypergolix.ipc import IPCServerProtocol
from hypergolix.ipc import IPCClientProtocol


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid
from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2


# ###############################################
# Testing
# ###############################################


@unittest.skipIf(True, 'skip')
class WSIPCTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Set up the IPC server.
        cls.server_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            name = 'server'
        )
        cls.server_protocol = IPCServerProtocol()
        cls.server = BasicServer(connection_cls=WSConnection)
        cls.server_commander.register_task(
            cls.server,
            msg_handler = cls.server_protocol,
            host = 'localhost',
            port = 4628,
            # debug = True
        )
        cls.golcore = GolixCore.__fixture__(TEST_AGENT1)
        cls.oracle = Oracle.__fixture__()
        cls.dispatch = Dispatcher.__fixture__()
        cls.rolodex = Rolodex.__fixture__()
        cls.salmonator = Salmonator.__fixture__()
        cls.server_protocol.assemble(cls.golcore, cls.oracle, cls.dispatch,
                                     cls.rolodex, cls.salmonator)
        
        # Set up the first IPC client.
        cls.client1_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            name = 'client'
        )
        cls.client1_protocol = IPCClientProtocol()
        cls.client1 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = cls.client1_protocol
        )
        cls.client1_commander.register_task(
            cls.client1,
            host = 'localhost',
            port = 4628,
            tls = False
        )
        cls.hgxlink1 = HGXLink.__fixture__()
        cls.client1_protocol.assemble(cls.hgxlink1)
        
        # Set up the second IPC client.
        cls.client2_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            name = 'client'
        )
        cls.client2_protocol = IPCClientProtocol()
        cls.client2 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = cls.client2_protocol
        )
        cls.client2_commander.register_task(
            cls.client2,
            host = 'localhost',
            port = 4628,
            tls = False
        )
        cls.hgxlink2 = HGXLink.__fixture__()
        cls.client2_protocol.assemble(cls.hgxlink2)
        
        # Finally, start both.
        cls.server_commander.start()
        cls.client1_commander.start()
        cls.client2_commander.start()
        
    @classmethod
    def tearDownClass(cls):
        cls.client1_commander.stop_threadsafe_nowait()
        cls.client2_commander.stop_threadsafe_nowait()
        cls.server_commander.stop_threadsafe_nowait()
        
    def get_client_conn(self, client, loop):
        ''' Used to retrieve the server connection associated with
        client1.
        '''
        # Create an arbitrary, unique API id and register it
        apiid = ApiID(bytes([random.randint(0, 255) for i in range(0, 64)]))
        await_coroutine_threadsafe(
            coro = client.register_api(apiid, timeout=1),
            loop = loop
        )
        # Convert the frozenset we'll get from getitem into a set and then pop
        # the only member of it, thereby getting the connection
        connection = set(self.dispatch._endpoints_from_api[apiid]).pop()
        
        # Restore the dispatch to its previous state
        await_coroutine_threadsafe(
            coro = client.deregister_api(apiid, timeout=1),
            loop = loop
        )
        
        return connection
        
    def test_token(self):
        ''' Test everything in set_token.
        '''
        # Test creating a new token
        self.dispatch.RESET()
        token = await_coroutine_threadsafe(
            coro = self.client1.set_token(None, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertIn(token, self.dispatch._all_known_tokens)
        
        # Test re-creating the same token concurrently from a different conn
        with self.assertRaises(IPCError):
            await_coroutine_threadsafe(
                coro = self.client2.set_token(token, timeout=1),
                loop = self.client2_commander._loop
            )
        self.assertIn(token, self.dispatch._all_known_tokens)
        
        # Test re-creating the same token concurrently from the same conn
        with self.assertRaises(IPCError):
            await_coroutine_threadsafe(
                coro = self.client1.set_token(token, timeout=1),
                loop = self.client1_commander._loop
            )
        self.assertIn(token, self.dispatch._all_known_tokens)
        
        # Remove the token and test setting the token.
        self.dispatch.RESET()
        token2 = await_coroutine_threadsafe(
            coro = self.client1.set_token(token, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertEqual(token2, token)
        self.assertIn(token, self.dispatch._all_known_tokens)
        
    def test_register_api(self):
        ''' Test registration and deregistration of api_ids.
        '''
        # Generate some pseudorandom api ids
        apiid_1 = ApiID(bytes([random.randint(0, 255) for i in range(0, 64)]))
        apiid_2 = ApiID(bytes([random.randint(0, 255) for i in range(0, 64)]))
        
        # Test registering a new api id
        self.dispatch.RESET()
        await_coroutine_threadsafe(
            coro = self.client1.register_api(apiid_1, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertIn(apiid_1, self.dispatch._endpoints_from_api)
        
        # Test adding a second
        await_coroutine_threadsafe(
            coro = self.client1.register_api(apiid_2, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertIn(apiid_2, self.dispatch._endpoints_from_api)
        
        # Now test removing the first
        await_coroutine_threadsafe(
            coro = self.client1.deregister_api(apiid_1, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertNotIn(apiid_1, self.dispatch._endpoints_from_api)
        self.assertIn(apiid_2, self.dispatch._endpoints_from_api)
        
    def test_whoami(self):
        whoami = await_coroutine_threadsafe(
            coro = self.client1.get_whoami(timeout=1),
            loop = self.client1_commander._loop
        )
        
        self.assertEqual(whoami, self.golcore.whoami)
        
    def test_startup_obj(self):
        ''' Test getting and setting startup objects.
        '''
        self.dispatch.RESET()
        
        # We need a token to do anything with startup objects, but we don't
        # need to do anything WITH the token.
        await_coroutine_threadsafe(
            coro = self.client1.set_token(None, timeout=1),
            loop = self.client1_commander._loop
        )
        
        ghid = await_coroutine_threadsafe(
            coro = self.client1.get_startup_obj(timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertIsNone(ghid)
        
        obj = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.client1.register_startup_obj(obj, timeout=1),
            loop = self.client1_commander._loop
        )
        ghid = await_coroutine_threadsafe(
            coro = self.client1.get_startup_obj(timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertEqual(obj, ghid)
        
        await_coroutine_threadsafe(
            coro = self.client1.deregister_startup_obj(timeout=1),
            loop = self.client1_commander._loop
        )
        
    def test_obj_get(self):
        ''' Test getting and making objects.
        '''
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        seed_state = bytes([random.randint(0, 255) for i in range(0, 20)])
        obj = MockDispatchable(
            author = self.golcore.whoami,
            dynamic = True,
            api_id = ApiID(bytes(64)),
            state = seed_state,
            frozen = False,
            held = False,
            deleted = False,
            oracle = self.oracle,
            dispatch = self.dispatch,
            ipc_core = self.server
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # Test getting that object
        ghid, author, state, is_link, api_id, private, dynamic, _legroom =\
            await_coroutine_threadsafe(
                coro = self.client1.get_ghid(obj.ghid),
                loop = self.client1_commander._loop
            )
        self.assertEqual(ghid, obj.ghid)
        self.assertEqual(state, seed_state)
        self.assertEqual(author, self.golcore.whoami)
        
    def test_obj_new(self):
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        seed_state = bytes([random.randint(0, 255) for i in range(0, 20)])
        obj = MockDispatchable(
            author = self.golcore.whoami,
            dynamic = True,
            api_id = ApiID(bytes(64)),
            state = seed_state,
            frozen = False,
            held = False,
            deleted = False,
            oracle = self.oracle,
            dispatch = self.dispatch,
            ipc_core = self.server
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # Test "creating" a "new" object
        ghid = await_coroutine_threadsafe(
            coro = self.client1.new_ghid(
                b'hello world',
                ApiID(bytes(64)),
                True,
                False,
                7
            ),
            loop = self.client1_commander._loop
        )
        self.assertEqual(ghid, obj.ghid)
        
    def test_obj_update(self):
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        seed_state = bytes([random.randint(0, 255) for i in range(0, 20)])
        obj = MockDispatchable(
            author = self.golcore.whoami,
            dynamic = True,
            api_id = ApiID(bytes(64)),
            state = seed_state,
            frozen = False,
            held = False,
            deleted = False,
            oracle = self.oracle,
            dispatch = self.dispatch,
            ipc_core = self.server
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # Test updating an existing object from client
        update_state = bytes([random.randint(0, 255) for i in range(0, 20)])
        await_coroutine_threadsafe(
            coro = self.client1.update_ghid(
                obj.ghid,
                update_state,
                False,
                7
            ),
            loop = self.client1_commander._loop
        )
        self.assertEqual(obj.state, update_state)
        
        # Test updating an existing object from server
        conn = self.get_client_conn(
            self.client1,
            self.client1_commander._loop
        )
        obj.update(bytes([random.randint(0, 255) for i in range(0, 20)]))
        await_coroutine_threadsafe(
            coro = self.server_protocol.update_obj(conn, obj.ghid),
            loop = self.server_commander._loop
        )
        self.assertEqual(obj.state, self.hgxlink1.state_lookup[obj.ghid])
        
    def test_obj_sync(self):
        # Test forceful sync from client
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        
        await_coroutine_threadsafe(
            coro = self.client1.sync_ghid(make_random_ghid()),
            loop = self.client1_commander._loop
        )
        # This doesn't have a success test (yet)
        
    def test_obj_share(self):
        ''' Test sharing. Bidirectional.
        '''
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        self.rolodex.RESET()
        
        # Test sharing object from client
        recipient = make_random_ghid()
        ghid = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.client1.share_ghid(ghid, recipient),
            loop = self.client1_commander._loop
        )
        self.assertEqual(self.rolodex.shared[ghid], recipient)
        
        # Test updating an existing object from server
        conn = self.get_client_conn(
            self.client1,
            self.client1_commander._loop
        )
        
        origin = make_random_ghid()
        ghid = make_random_ghid()
        api_id = ApiID(bytes([random.randint(0, 255) for i in range(0, 64)]))
        await_coroutine_threadsafe(
            coro = self.server_protocol.share_obj(conn, ghid, origin, api_id),
            loop = self.server_commander._loop
        )
        self.assertEqual(self.hgxlink1.share_lookup[ghid], origin)
        self.assertEqual(self.hgxlink1.api_lookup[ghid], api_id)
        
    def test_share_response(self):
        ''' Test server sending share success and failure. Doesn't do
        much at the moment beyond just test the arbitrary response
        sending.
        '''
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        self.rolodex.RESET()
        
        conn = self.get_client_conn(
            self.client1,
            self.client1_commander._loop
        )
        
        recipient = make_random_ghid()
        ghid = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.server_protocol.notify_share_success(
                conn,
                ghid,
                recipient
            ),
            loop = self.server_commander._loop
        )
        await_coroutine_threadsafe(
            coro = self.server_protocol.notify_share_failure(
                conn,
                ghid,
                recipient
            ),
            loop = self.server_commander._loop
        )
        
    def test_obj_freeze(self):
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        self.rolodex.RESET()
        seed_state = bytes([random.randint(0, 255) for i in range(0, 20)])
        obj = MockDispatchable(
            author = self.golcore.whoami,
            dynamic = True,
            api_id = ApiID(bytes(64)),
            state = seed_state,
            frozen = False,
            held = False,
            deleted = False,
            oracle = self.oracle,
            dispatch = self.dispatch,
            ipc_core = self.server
        )
        self.oracle.add_object(obj.ghid, obj)
        
        frozen_ghid = await_coroutine_threadsafe(
            coro = self.client1.freeze_ghid(obj.ghid),
            loop = self.client1_commander._loop
        )
        frozen = self.oracle.get_object(None, frozen_ghid)
        self.assertEqual(frozen.state[1], seed_state)
        
    def test_obj_hold(self):
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        self.rolodex.RESET()
        seed_state = bytes([random.randint(0, 255) for i in range(0, 20)])
        obj = MockDispatchable(
            author = self.golcore.whoami,
            dynamic = True,
            api_id = ApiID(bytes(64)),
            state = seed_state,
            frozen = False,
            held = False,
            deleted = False,
            oracle = self.oracle,
            dispatch = self.dispatch,
            ipc_core = self.server
        )
        self.oracle.add_object(obj.ghid, obj)
        
        await_coroutine_threadsafe(
            coro = self.client1.hold_ghid(obj.ghid),
            loop = self.client1_commander._loop
        )
        
        self.assertTrue(obj.held)
        
    def test_obj_discard(self):
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        self.rolodex.RESET()
        
        # There's not currently anything to verify this.
        ghid = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.client1.discard_ghid(ghid),
            loop = self.client1_commander._loop
        )
        
    def test_obj_delete(self):
        ''' Bidirectional deletion test.
        '''
        # Test setup
        self.oracle.RESET()
        self.dispatch.RESET()
        self.rolodex.RESET()
        seed_state = bytes([random.randint(0, 255) for i in range(0, 20)])
        obj = MockDispatchable(
            author = self.golcore.whoami,
            dynamic = True,
            api_id = ApiID(bytes(64)),
            state = seed_state,
            frozen = False,
            held = False,
            deleted = False,
            oracle = self.oracle,
            dispatch = self.dispatch,
            ipc_core = self.server
        )
        self.oracle.add_object(obj.ghid, obj)
        
        await_coroutine_threadsafe(
            coro = self.client1.delete_ghid(obj.ghid),
            loop = self.client1_commander._loop
        )
        self.assertTrue(obj.deleted)
        
        # Test updating an existing object from server
        conn = self.get_client_conn(
            self.client1,
            self.client1_commander._loop
        )
        
        ghid = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.server_protocol.delete_obj(conn, ghid),
            loop = self.server_commander._loop
        )
        self.assertIn(ghid, self.hgxlink1.deleted)
        

class HGXLinkTrashtest(unittest.TestCase):
    ''' Test HGXLink.
    
    TODO: figure out how hgxlink should run get_whoami!
    '''
    
    @classmethod
    def setUpClass(cls):
        # Set up the IPC fixture
        whoami = make_random_ghid()
        cls.ipc_fixture = IPCClientProtocol.__fixture__(whoami)
        # Set up HGXLink (and, because of autostart, start its event loop)
        cls.hgxlink = HGXLink(
            debug = True,
            threaded = True,
            autostart = True,
            ipc_fixture = cls.ipc_fixture
        )
        # Normally this is handled by the connection startup. Since we're
        # fixturing the connection manager, we have to do it manually.
        cls.hgxlink.whoami = whoami
        
    @classmethod
    def tearDownClass(cls):
        # We just need to kill the hgxlink
        cls.hgxlink.stop_threadsafe_nowait()
        
    def make_dummy_object(self):
        return ObjCore.__fixture__(
            hgxlink = self.hgxlink,
            ipc_manager = self.ipc_fixture,
            state = bytes([random.randint(0, 255) for i in range(0, 25)]),
            api_id = ApiID(
                bytes([random.randint(0, 255) for i in range(0, 64)])
            ),
            dynamic = True,
            private = False,
            ghid = make_random_ghid(),
            binder = self.hgxlink.whoami,
            _legroom = random.randint(5, 15)
        )
        
    def test_whoami(self):
        # Trivial pass-through.
        self.assertEqual(self.hgxlink.whoami, self.ipc_fixture.whoami)
        
    def test_token(self):
        ''' Test token operations.
        '''
        self.ipc_fixture.RESET()
        
        with self.assertRaises(HGXLinkError):
            self.hgxlink.token
            
        # Set a token
        token = bytes([random.randint(0, 255) for i in range(0, 4)])
        startup = self.hgxlink.register_token_threadsafe(token)
        self.assertEqual(token, self.hgxlink.token)
        self.assertIsNone(startup)
        
        # Ensure re-setting fails
        with self.assertRaises(HGXLinkError):
            self.hgxlink.register_token_threadsafe(bytes(4))
        
        # Call again with nothing as token
        self.hgxlink._token = None
        startup = self.hgxlink.register_token_threadsafe()
        self.assertIsNone(startup)
        
        # Now set the startup object and run again
        self.hgxlink._token = None
        token = bytes([random.randint(0, 255) for i in range(0, 4)])
        set_startup = make_random_ghid()
        self.ipc_fixture.startup = set_startup
        startup = self.hgxlink.register_token_threadsafe(token)
        self.assertEqual(set_startup, startup)
        
    def test_startup(self):
        ''' Test startup registration and de-registration.
        '''
        self.ipc_fixture.RESET()
        dummy_obj = self.make_dummy_object()
        self.hgxlink.register_startup_obj_threadsafe(dummy_obj)
        self.assertEqual(dummy_obj._hgx_ghid, self.ipc_fixture.startup)
        
        self.hgxlink.deregister_startup_obj_threadsafe()
        self.assertIsNone(self.ipc_fixture.startup)
        
    def test_get(self):
        ''' Test getting an object.
        '''
        self.ipc_fixture.RESET()
        dummy_obj = self.make_dummy_object()
        self.ipc_fixture.prep_obj(dummy_obj)
        
        # Get that object normally.
        obj = self.hgxlink.get_threadsafe(
            ObjCore.__fixture__,
            dummy_obj._hgx_ghid
        )
        self.assertEqual(dummy_obj._hgx_state, obj._hgx_state)
        self.assertEqual(dummy_obj._hgx_ghid, obj._hgx_ghid)
        self.assertIn(dummy_obj._hgx_ghid, self.hgxlink._objs_by_ghid)
        
        # Now re-get that object, as if we're calling again from somewhere else
        self.ipc_fixture.RESET()
        obj2 = self.hgxlink.get_threadsafe(
            ObjCore.__fixture__,
            dummy_obj._hgx_ghid
        )
        
        # Now reset and get with an object def, like we're recasting.
        self.ipc_fixture.RESET()
        dummy_obj = self.make_dummy_object()
        self.ipc_fixture.prep_obj(dummy_obj)
        
        # Get that object normally.
        obj = self.hgxlink.get_threadsafe(
            ObjCore.__fixture__,
            dummy_obj._hgx_ghid,
            obj_def = (
                dummy_obj._hgx_ghid,
                dummy_obj._hgx_binder,
                dummy_obj._hgx_state,
                dummy_obj._hgx_linked,
                dummy_obj._hgx_api_id,
                dummy_obj._hgx_private,
                dummy_obj._hgx_dynamic,
                dummy_obj._hgx_legroom
            )
        )
        self.assertEqual(dummy_obj._hgx_state, obj._hgx_state)
        self.assertEqual(dummy_obj._hgx_ghid, obj._hgx_ghid)
        self.assertIn(dummy_obj._hgx_ghid, self.hgxlink._objs_by_ghid)
        
    ###################################################################
    # Junk follows.
    ###################################################################
        
    @unittest.skipIf(True, 'skip')
    def test_client1(self):
        
        # Okay, now I'm satisfied about tokens. Get on with it already!
        pt0 = b'I am a sexy stagnant beast.'
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'
        
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


if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    unittest.main()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
