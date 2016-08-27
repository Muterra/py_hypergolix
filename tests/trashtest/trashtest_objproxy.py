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
import weakref

from hypergolix.utils import LooperTrooper
from hypergolix.utils import Aengel

from hypergolix.objproxy import NoopProxy
from hypergolix.objproxy import ObjBase
from hypergolix.objproxy import PickleObj
from hypergolix.objproxy import JsonObj

from golix import Ghid


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid
        
        
class MockEmbed(LooperTrooper):
    def new_threadsafe(self, cls, *args, **kwargs):
        obj = cls(hgxlink=self, *args, **kwargs)
        obj._ghid_3141592 = make_random_ghid()
        obj._binder_3141592 = make_random_ghid()
        return obj
        
    def subscribe_to_updates(self, *args, **kwargs):
        # This is unused for these tests.
        pass
        
    async def loop_run(self):
        ''' Just await closure forever.
        '''
        await self._shutdown_init_flag.wait()
        
        
class MockObject:
    pass


# ###############################################
# Testing
# ###############################################
        
        
class ProxyTest(unittest.TestCase):
    ''' For now, just test the proxying stuff, and not the comms stuff.
    '''
    @classmethod
    def setUpClass(cls):
        cls.aengel = Aengel()
        cls.embed = MockEmbed(
            threaded = True,
            thread_name = 'mockembed',
            aengel = cls.aengel,
        )
        
    def do_proxy_pair(self, state):
        ''' Fixture for making a proxy object, and returning both it and
        the original object.
        '''
        return state, self.do_proxy_obj(state)
    
    def do_proxy_obj(self, state, cls=None):
        ''' Fixture for making a dynamic object with state, for testing
        all of the proxy stuff.
        '''
        if cls is None:
            cls = NoopProxy
        
        return self.embed.new_threadsafe(
            cls = cls,
            state = state, 
            api_id = bytes(64), 
            dynamic = True,
            private = False
        )
        
    def test_handwritten(self):
        ''' Test stuff like repr, dir, etc that have been written by 
        hand within the proxy definition, as opposed to explicit (but 
        programmatically generated) magic/dunder/special method defs.
        '''
        prox = self.do_proxy_obj({})
        
        # Check to make sure that all of the reference's things are in the dir.
        proxdir = dir(prox)
        for should_exist in dir({}):
            self.assertIn(should_exist, proxdir)
        # TODO: also ensure that all of the proxy object's stuff is there too.
        
        # Just go ahead and check this for errors; nothing special
        repr(prox)
        
    def test_recasting(self):
        ''' Test upcasting and downcasting the objects.
        '''
        prox = self.do_proxy_obj(b'1')
        reprox = ObjBase.hgx_recast_threadsafe(prox)
        
        self.assertEqual(reprox.hgx_state, b'1')
        self.assertEqual(reprox.hgx_ghid, prox.hgx_ghid)
        
        rereprox = NoopProxy.hgx_recast_threadsafe(reprox)
        
        self.assertEqual(rereprox.hgx_state, b'1')
        self.assertEqual(rereprox.hgx_ghid, reprox.hgx_ghid)
        
    def test_pickle(self):
        ''' Use recasting to test PickleObj
        '''
        prox = self.do_proxy_obj(1, PickleObj)
        reprox = ObjBase.hgx_recast_threadsafe(prox)
        rereprox = PickleObj.hgx_recast_threadsafe(reprox)
        
        self.assertEqual(rereprox.hgx_state, 1)
        
    def test_pickle(self):
        ''' Use recasting to test JsonObj
        '''
        prox = self.do_proxy_obj(1, JsonObj)
        reprox = ObjBase.hgx_recast_threadsafe(prox)
        rereprox = JsonObj.hgx_recast_threadsafe(reprox)
        
        self.assertEqual(rereprox.hgx_state, 1)
        
    def test_basic(self):
        prox = self.do_proxy_obj('hello world')
        self.assertEqual(prox._proxy_3141592, 'hello world')
        
    def test_attr_proxy(self):
        ''' Test attr proxy pass-through.
        '''
        mo = MockObject()
        moproxy = self.do_proxy_obj(mo)
        
        moproxy.foo = 'bar'
        self.assertEqual(mo.foo, 'bar')
        self.assertEqual(moproxy.foo, 'bar')
        
        del moproxy.foo
        
        with self.assertRaises(AttributeError):
            __ = moproxy.foo
        
        with self.assertRaises(AttributeError):
            __ = mo.foo
    
    def test_equality(self):
        ''' Test equality of proxy and original object
        '''
        # Also test equality of two identical proxies with different ghids
        self.assertNotEqual(
            self.do_proxy_obj(1),
            self.do_proxy_obj(1)
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
            *(self.do_proxy_pair({1:1,}))
        )
        
    def test_num(self):
        self.assertGreater(self.do_proxy_obj(1), 0)
        self.assertGreaterEqual(self.do_proxy_obj(1), 1)
        self.assertGreaterEqual(self.do_proxy_obj(2), 1)
        self.assertLess(self.do_proxy_obj(1), 2)
        self.assertLessEqual(self.do_proxy_obj(1), 1)
        self.assertLessEqual(self.do_proxy_obj(1), 2)
        
        obj = 7
        prox = self.do_proxy_obj(obj)
        
        self.assertEqual(prox, obj)
        prox += 1
        self.assertEqual(prox, 8)
        self.assertTrue(isinstance(prox, NoopProxy))
        
    def test_mapping(self):
        obj = {
            1: 1,
            2: 2,
            3: 3,
            4: 4,
        }
        
        prox = self.do_proxy_obj(obj)
        
        self.assertEqual(prox[1], obj[1])
        
        del prox[4]
        
        with self.assertRaises(KeyError):
            prox[4]
            
        prox[4] = 4
        
        self.assertEqual(prox, obj)
        
    def test_list(self):
        obj = [1, 2, 3, 4,]
        obj2 = [1, 2, 3, 4,]
        prox = self.do_proxy_obj(obj)
        prox2 = self.do_proxy_obj(obj2)
        
        self.assertEqual(prox, obj)
        prox.append(5)
        self.assertEqual(prox[4], obj[4])
        prox.extend([6,7,8])
        self.assertEqual(prox, obj)
        prox += [1,2,3]
        self.assertEqual(prox, obj)
        obj3 = prox.copy()
        
        prox += prox2
        self.assertEqual(prox, obj3 + prox2)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()