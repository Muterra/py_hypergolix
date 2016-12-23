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
import loopa
import asyncio
import random
import threading

from loopa.utils import await_coroutine_threadsafe
from loopa import NoopLoop as DummyLoop

from hypergolix.utils import ApiID
from hypergolix.embed import HGXLink
from hypergolix.exceptions import HGXLinkError
from hypergolix.ipc import IPCClientProtocol

from hypergolix.objproxy import ObjCore
from hypergolix.objproxy import Proxy
from hypergolix.objproxy import Obj
from hypergolix.objproxy import PickleObj
from hypergolix.objproxy import JsonObj

from golix import Ghid


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid


class MockProgrammerObject:
    ''' Simulate an arbitrary programmer's object.
    '''
    pass


# ###############################################
# Testing
# ###############################################


class GenericObjTest:
    ''' Runs a handful of generic object tests.
    '''
    
    def __init__(self, *args, name_convention, use_cls, **kwargs):
        ''' Sets up our naming convention.
        '''
        super().__init__(*args, **kwargs)
        
        self.name_prefix = name_convention
        self.use_cls = use_cls
    
    @classmethod
    def setUpClass(cls):
        # Set up the IPC and hgxlink fixtures
        whoami = make_random_ghid()
        cls.ipc_fixture = IPCClientProtocol.__fixture__(whoami)
        cls.hgxlink = HGXLink.__fixture__(whoami, cls.ipc_fixture)
        cls.spinner = DummyLoop()
        cls.hgxlink.register_task(cls.spinner)
        
        # We also need to START the event loop.
        cls.hgxlink.start()
        
    @classmethod
    def tearDownClass(cls):
        # We just need to kill the hgxlink
        cls.hgxlink.stop_threadsafe_nowait()
        
    def make_dummy_object(self, cls, state=None):
        if state is None:
            state = bytes([random.randint(0, 255) for i in range(0, 25)])
        
        return cls(
            hgxlink = self.hgxlink,
            ipc_manager = self.ipc_fixture,
            state = state,
            api_id = ApiID(
                bytes([random.randint(0, 255) for i in range(0, 64)])
            ),
            dynamic = True,
            private = False,
            ghid = make_random_ghid(),
            binder = self.hgxlink.whoami,
            _legroom = random.randint(5, 15)
        )
        
    def test_recasting(self):
        ''' Test upcasting and downcasting the objects.
        '''
        recast_str = self.name_prefix + 'recast_threadsafe'
        obj = self.make_dummy_object(self.use_cls)
        state = obj._hgx_state
        redobj = getattr(obj, recast_str)(ObjCore)
        
        self.assertEqual(redobj._hgx_state, state)
        self.assertEqual(redobj._hgx_ghid, obj._hgx_ghid)
        
        reredobj = redobj._hgx_recast_threadsafe(self.use_cls)
        
        self.assertEqual(reredobj._hgx_state, state)
        self.assertEqual(reredobj._hgx_ghid, redobj._hgx_ghid)
        
    def test_state(self):
        # Test basic object creation.
        obj = self.make_dummy_object(self.use_cls, b'hello world')
        self.assertEqual(obj._hgx_state, b'hello world')
        
        state_str = self.name_prefix + 'state'
        state = getattr(obj, state_str)
        self.assertEqual(state, b'hello world')
        
    def test_ghid(self):
        ''' Test getting ghid from the type-specific property.
        '''
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'ghid'
        test_val = getattr(obj, test_str)
        self.assertEqual(test_val, obj._hgx_ghid)
        
    def test_api(self):
        ''' Test getting ghid from the type-specific property.
        '''
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'api_id'
        test_val = getattr(obj, test_str)
        self.assertEqual(test_val, obj._hgx_api_id)
        
    def test_private(self):
        ''' Test getting ghid from the type-specific property.
        '''
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'private'
        test_val = getattr(obj, test_str)
        self.assertEqual(test_val, obj._hgx_private)
        
    def test_dynamic(self):
        ''' Test getting ghid from the type-specific property.
        '''
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'dynamic'
        test_val = getattr(obj, test_str)
        self.assertEqual(test_val, obj._hgx_dynamic)
        
    def test_binder(self):
        ''' Test getting ghid from the type-specific property.
        '''
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'binder'
        test_val = getattr(obj, test_str)
        self.assertEqual(test_val, obj._hgx_binder)
        
    def test_isalive(self):
        ''' Test getting ghid from the type-specific property.
        '''
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'isalive'
        test_val = getattr(obj, test_str)
        self.assertEqual(test_val, obj._hgx_isalive)
        
    def test_callback(self):
        ''' Test getting ghid from the type-specific property.
        '''
        async def test_coro(*args, **kwargs):
            pass
            
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'callback'
        setattr(obj, test_str, test_coro)
        test_val = getattr(obj, test_str)
        self.assertEqual(test_val, obj._hgx_callback)
        
    def test_push(self):
        ''' Test getting ghid from the type-specific property.
        '''
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'push_threadsafe'
        test_meth = getattr(obj, test_str)
        state = obj._hgx_state
        test_meth()
        update = self.ipc_fixture.updates[0]
        self.assertIn(obj._hgx_ghid, update)
        self.assertEqual(update[obj._hgx_ghid][0], state)
        
    def test_sync(self):
        ''' Yep, test the sync.
        '''
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'sync_threadsafe'
        test_meth = getattr(obj, test_str)
        test_meth()
        self.assertIn(obj._hgx_ghid, self.ipc_fixture.syncs)
        
    def test_share(self):
        ''' Test the share.
        '''
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'share_threadsafe'
        test_meth = getattr(obj, test_str)
        recipient = make_random_ghid()
        test_meth(recipient)
        self.assertTrue(
            self.ipc_fixture.shares.contains_within(obj._hgx_ghid, recipient)
        )
        
    def test_freeze(self):
        ''' Test freezing.
        '''
        self.hgxlink.RESET()
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        self.hgxlink.prep_obj(obj)
        
        test_str = self.name_prefix + 'freeze_threadsafe'
        test_meth = getattr(obj, test_str)
        test_meth()
        self.assertIn(obj._hgx_ghid, self.ipc_fixture.frozen)
        
    def test_hold(self):
        ''' Test holding.
        '''
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'hold_threadsafe'
        test_meth = getattr(obj, test_str)
        test_meth()
        self.assertIn(obj._hgx_ghid, self.ipc_fixture.held)
        
    def test_discard(self):
        ''' Test discarding.
        '''
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'discard_threadsafe'
        test_meth = getattr(obj, test_str)
        test_meth()
        self.assertIn(obj._hgx_ghid, self.ipc_fixture.discarded)
        
    def test_delete(self):
        ''' Test deletion.
        '''
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        test_str = self.name_prefix + 'delete_threadsafe'
        test_meth = getattr(obj, test_str)
        test_meth()
        self.assertIn(obj._hgx_ghid, self.ipc_fixture.deleted)
        
    def test_upstream_push(self):
        ''' Test force delete and force pull.
        '''
        self.hgxlink.RESET()
        self.ipc_fixture.RESET()
        
        obj = self.make_dummy_object(self.use_cls)
        notifier = threading.Event()
        
        new_state = bytes([random.randint(0, 255) for i in range(0, 25)])
        
        # Define a callback to use. This is already threadsafe and loopsafe,
        # so no wrapping is necessary.
        async def test_callback(obj):
            notifier.set()
            
        # Set the callback
        obj._hgx_callback = test_callback
        
        await_coroutine_threadsafe(
            coro = obj._hgx_force_pull(state=new_state),
            loop = self.hgxlink._loop
        )
        self.assertTrue(notifier.wait(timeout=1))
        self.assertEqual(obj._hgx_state, new_state)
        notifier.clear()
        
        await_coroutine_threadsafe(
            coro = obj._hgx_force_delete(),
            loop = self.hgxlink._loop
        )
        self.assertTrue(notifier.wait(timeout=1))
        self.assertFalse(obj._hgx_isalive)
        notifier.clear()


class CoreTest(GenericObjTest, unittest.TestCase):
    ''' Test everything from ObjCore.
    '''
    
    def __init__(self, *args, **kwargs):
        # Make the super() call with the appropriate prefix.
        super().__init__(name_convention='_hgx_', use_cls=ObjCore, *args,
                         **kwargs)
        
        
class ObjTest(GenericObjTest, unittest.TestCase):
    ''' Additional tests for a standard obj.
    '''
    
    def __init__(self, *args, **kwargs):
        # Make the super() call with the appropriate prefix.
        super().__init__(name_convention='', use_cls=Obj, *args, **kwargs)
        
        
class ProxyTest(GenericObjTest, unittest.TestCase):
    ''' Additional tests for a proxy obj.
    '''
    
    def __init__(self, *args, **kwargs):
        # Make the super() call with the appropriate prefix.
        super().__init__(name_convention='hgx_', use_cls=Proxy, *args,
                         **kwargs)
        
    def do_proxy_pair(self, state):
        ''' Fixture for making a proxy object, and returning both it and
        the original object.
        '''
        return state, self.make_dummy_object(Proxy, state)
        
    def test_handwritten(self):
        ''' Test stuff like repr, dir, etc that have been written by
        hand within the proxy definition, as opposed to explicit (but
        programmatically generated) magic/dunder/special method defs.
        '''
        prox = self.make_dummy_object(Proxy, {})
        
        # Check to make sure that all of the reference's things are in the dir.
        proxdir = dir(prox)
        for should_exist in dir({}):
            self.assertIn(should_exist, proxdir)
        # TODO: also ensure that all of the proxy object's stuff is there too.
        
        # Just go ahead and check this for errors; nothing special
        repr(prox)
        
    def test_attr_proxy(self):
        ''' Test attr proxy pass-through.
        '''
        mo = MockProgrammerObject()
        moproxy = self.make_dummy_object(Proxy, mo)
        
        moproxy.foo = 'bar'
        self.assertEqual(mo.foo, 'bar')
        self.assertEqual(moproxy.foo, 'bar')
        
        del moproxy.foo
        
        with self.assertRaises(AttributeError):
            moproxy.foo
        
        with self.assertRaises(AttributeError):
            mo.foo
    
    def test_equality(self):
        ''' Test equality of proxy and original object
        '''
        # Also test equality of two identical proxies with different ghids
        self.assertNotEqual(
            self.make_dummy_object(Proxy, 1),
            self.make_dummy_object(Proxy, 1)
        )
        
        # But everything else should compare equally.
        self.assertEqual(
            *(self.do_proxy_pair(1))
        )
        self.assertEqual(
            *(self.do_proxy_pair('1'))
        )
        self.assertEqual(
            *(self.do_proxy_pair(['hello']))
        )
        self.assertEqual(
            *(self.do_proxy_pair({1: 1}))
        )
        
    def test_num(self):
        self.assertGreater(self.make_dummy_object(Proxy, 1), 0)
        self.assertGreaterEqual(self.make_dummy_object(Proxy, 1), 1)
        self.assertGreaterEqual(self.make_dummy_object(Proxy, 2), 1)
        self.assertLess(self.make_dummy_object(Proxy, 1), 2)
        self.assertLessEqual(self.make_dummy_object(Proxy, 1), 1)
        self.assertLessEqual(self.make_dummy_object(Proxy, 1), 2)
        
        obj = 7
        prox = self.make_dummy_object(Proxy, obj)
        
        self.assertEqual(prox, obj)
        prox += 1
        self.assertEqual(prox, 8)
        self.assertTrue(isinstance(prox, Proxy))
        
    def test_mapping(self):
        obj = {
            1: 1,
            2: 2,
            3: 3,
            4: 4,
        }
        
        prox = self.make_dummy_object(Proxy, obj)
        
        self.assertEqual(prox[1], obj[1])
        
        del prox[4]
        
        with self.assertRaises(KeyError):
            prox[4]
            
        prox[4] = 4
        
        self.assertEqual(prox, obj)
        
    def test_list(self):
        obj = [1, 2, 3, 4]
        obj2 = [1, 2, 3, 4]
        prox = self.make_dummy_object(Proxy, obj)
        prox2 = self.make_dummy_object(Proxy, obj2)
        
        self.assertEqual(prox, obj)
        prox.append(5)
        self.assertEqual(prox[4], obj[4])
        prox.extend([6, 7, 8])
        self.assertEqual(prox, obj)
        prox += [1, 2, 3]
        self.assertEqual(prox, obj)
        obj3 = prox.copy()
        
        prox += prox2
        self.assertEqual(prox, obj3 + prox2)


class SerializationTest(GenericObjTest, unittest.TestCase):
    ''' Test everything from ObjCore.
    '''
    
    def __init__(self, *args, **kwargs):
        # Make the super() call with the appropriate prefix.
        super().__init__(name_convention='_hgx_', use_cls=ObjCore, *args,
                         **kwargs)
        
    def test_pickle(self):
        ''' Use closed-loop recasting to test PickleObj
        '''
        obj = self.make_dummy_object(PickleObj, 1)
        reprox = obj.recast_threadsafe(Proxy)
        
        # Interestingly, this fails with a proxy object, but below doesn't
        proxunpack = await_coroutine_threadsafe(
            coro = PickleObj.hgx_unpack(reprox._hgx_state),
            loop = self.hgxlink._loop
        )
        self.assertEqual(proxunpack, 1)
        
        redobj = reprox.hgx_recast_threadsafe(PickleObj)
        self.assertEqual(redobj._hgx_state, 1)
        
    def test_json(self):
        ''' Use closed-loop recasting to test JsonObj
        '''
        obj = self.make_dummy_object(JsonObj, 1)
        reprox = obj.recast_threadsafe(Proxy)
        
        # Interestingly, this works with a proxy object, but above doesn't
        proxunpack = await_coroutine_threadsafe(
            coro = JsonObj.hgx_unpack(reprox),
            loop = self.hgxlink._loop
        )
        self.assertEqual(proxunpack, 1)
        
        redobj = reprox.hgx_recast_threadsafe(JsonObj)
        self.assertEqual(redobj._hgx_state, 1)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
