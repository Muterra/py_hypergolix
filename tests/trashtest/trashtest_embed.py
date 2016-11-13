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
import queue
import random
import inspect
import asyncio

from loopa import TaskLooper
from loopa.utils import await_coroutine_threadsafe

from hypergolix.utils import ApiID
from hypergolix.embed import HGXLink
from hypergolix.objproxy import ObjCore
from hypergolix.exceptions import HGXLinkError
from hypergolix.ipc import IPCClientProtocol


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid


class IPCFixture(TaskLooper, IPCClientProtocol.__fixture__):
    ''' Adds an event loop to the IPC fixture to use for testing.
    '''
    
    async def loop_run(self, *args, **kwargs):
        ''' Just busy loop forever for the fixture.
        '''
        await asyncio.sleep(.1)


# ###############################################
# Testing
# ###############################################
        

class HGXLinkTrashtest(unittest.TestCase):
    ''' Test HGXLink.
    
    TODO: figure out how hgxlink should run get_whoami!
    '''
    
    @classmethod
    def setUpClass(cls):
        # Set up the IPC fixture
        whoami = make_random_ghid()
        cls.ipc_fixture = IPCFixture(whoami)
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
        self.assertEqual(obj2._hgx_state, obj._hgx_state)
        self.assertEqual(obj2._hgx_ghid, obj._hgx_ghid)
        self.assertIn(obj2._hgx_ghid, self.hgxlink._objs_by_ghid)
        
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
        
    def test_new(self):
        ''' Create a new object.
        '''
        self.ipc_fixture.RESET()
        dummy_obj = self.make_dummy_object()
        self.ipc_fixture.pending_ghid = dummy_obj._hgx_ghid
        
        obj = self.hgxlink.new_threadsafe(
            ObjCore.__fixture__,
            dummy_obj._hgx_state,
            dummy_obj._hgx_api_id,
            dummy_obj._hgx_dynamic,
            dummy_obj._hgx_private,
            dummy_obj._hgx_legroom
        )
        self.assertEqual(dummy_obj._hgx_state, obj._hgx_state)
        self.assertEqual(dummy_obj._hgx_ghid, obj._hgx_ghid)
        self.assertIn(dummy_obj._hgx_ghid, self.hgxlink._objs_by_ghid)
        
    def test_upstream_pull(self):
        ''' Test updates and deletion coming in from upstream.
        '''
        self.ipc_fixture.RESET()
        # Manually make and load a dummy object
        dummy_obj = self.make_dummy_object()
        self.hgxlink._objs_by_ghid[dummy_obj._hgx_ghid] = dummy_obj
        
        # Test updates
        new_state = bytes([random.randint(0, 255) for i in range(0, 25)])
        await_coroutine_threadsafe(
            coro = self.hgxlink._pull_state(dummy_obj._hgx_ghid, new_state),
            loop = self.hgxlink._loop
        )
        self.assertEqual(dummy_obj._hgx_state, new_state)
        
        # Test deletion
        await_coroutine_threadsafe(
            coro = self.hgxlink.handle_delete(dummy_obj._hgx_ghid),
            loop = self.hgxlink._loop
        )
        
    def test_share_stuff(self):
        ''' Test share handlers registration, function wrapping, and
        actual handling of upstream shares. For convenience reasons,
        also validate triplicate loopsafe stuff.
        '''
        self.ipc_fixture.RESET()
        
        class DummyLoop(TaskLooper):
            ''' Make a dummy event loop for manipulation of stuff.
            '''
            
            async def loop_run(self):
                await asyncio.sleep(.1)
                
        looper = DummyLoop(threaded=True, debug=True, reusable_loop=False)
        
        # Test manual wrapping
        def dummy_func(*args, **kwargs):
            pass
        dummy_func = self.hgxlink.wrap_threadsafe(dummy_func)
        self.assertTrue(inspect.iscoroutinefunction(dummy_func))
        
        async def dummy_coro(*args, **kwargs):
            pass
        dummy_coro = self.hgxlink.wrap_loopsafe(
            dummy_coro,
            target_loop = looper._loop
        )
        self.assertTrue(inspect.iscoroutinefunction(dummy_coro))
        
        # Test decorator wrapping
        deliveries = queue.Queue()
        
        # This is the actual loopsafe handler we'll test
        @self.hgxlink.wrap_loopsafe(looper._loop)
        async def loopsafe_handler(ghid, origin, api_id):
            await asyncio.sleep(.01)
            deliveries.put((ghid, origin, api_id))
        self.assertTrue(inspect.iscoroutinefunction(loopsafe_handler))
        
        # Note this is the actual threaded handler we'll test
        @self.hgxlink.wrap_threadsafe
        def handler(ghid, origin, api_id):
            deliveries.put((ghid, origin, api_id))
        self.assertTrue(inspect.iscoroutinefunction(handler))
            
        # Now register the actual handler
        apiid = ApiID(bytes([random.randint(0, 255) for i in range(0, 64)]))
        self.hgxlink.register_share_handler_threadsafe(apiid, handler)
        self.assertIn(apiid, self.ipc_fixture.apis)
        
        # Test running an incoming share
        ghid = make_random_ghid()
        origin = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.hgxlink.handle_share(ghid, origin, apiid),
            loop = self.hgxlink._loop
        )
        ghid2, origin2, apiid2 = deliveries.get(timeout=1)
        self.assertEqual(ghid, ghid2)
        self.assertEqual(origin2, origin)
        self.assertEqual(apiid2, apiid)
        
        # Test removing the handler
        self.hgxlink.deregister_share_handler_threadsafe(apiid)
        self.assertNotIn(apiid, self.ipc_fixture.apis)
        
        # Now test loopsafe stuff with a new handler.
        self.hgxlink.register_share_handler_threadsafe(apiid, loopsafe_handler)
        
        try:
            looper.start()
            await_coroutine_threadsafe(
                coro = self.hgxlink.handle_share(ghid, origin, apiid),
                loop = self.hgxlink._loop
            )
            ghid2, origin2, apiid2 = deliveries.get(timeout=1)
            self.assertEqual(ghid, ghid2)
            self.assertEqual(origin2, origin)
            self.assertEqual(apiid2, apiid)
            self.hgxlink.deregister_share_handler_threadsafe(apiid)
            
        finally:
            looper.stop_threadsafe_nowait()


if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    unittest.main()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
