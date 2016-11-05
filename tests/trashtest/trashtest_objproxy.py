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


class DummyLoop(loopa.TaskLooper):
    ''' Make a dummy event loop for manipulation of stuff.
    '''
    
    async def loop_run(self):
        await asyncio.sleep(.1)


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
        
    def test_basic(self):
        # Test basic object creation.
        obj = self.make_dummy_object(self.use_cls, b'hello world')
        self.assertEqual(obj._hgx_state, b'hello world')
        
        state_str = self.name_prefix + 'state'
        state = getattr(obj, state_str)
        self.assertEqual(state, b'hello world')


class CoreTest(GenericObjTest, unittest.TestCase):
    ''' Test everything from ObjCore.
    '''
    
    def __init__(self, *args, **kwargs):
        # Make the super() call with the appropriate prefix.
        super().__init__(name_convention='_hgx_', use_cls=ObjCore, *args,
                         **kwargs)
        
        
class ObjTest(GenericObjTest, unittest.TestCase):
    ''' Tests a standard obj.
    '''
    
    def __init__(self, *args, **kwargs):
        # Make the super() call with the appropriate prefix.
        super().__init__(name_convention='', use_cls=Obj, *args, **kwargs)
        
        
class ProxyTest(GenericObjTest, unittest.TestCase):
    ''' Tests a proxy obj.
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
        redobj = reprox.hgx_recast_threadsafe(PickleObj)
        
        self.assertEqual(redobj._hgx_state, 1)
        
    def test_json(self):
        ''' Use closed-loop recasting to test JsonObj
        '''
        obj = self.make_dummy_object(JsonObj, 1)
        reprox = obj.recast_threadsafe(Proxy)
        redobj = reprox.hgx_recast_threadsafe(JsonObj)
        
        self.assertEqual(redobj._hgx_state, 1)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
