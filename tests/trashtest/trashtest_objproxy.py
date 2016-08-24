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

from hypergolix.objproxy import NoopProxy

# These are fixture imports
from golix import Ghid
from golix.utils import Secret


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid

from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
        
        
class MockEmbed:
    def new_threadsafe(self, *args, **kwargs):
        obj = NoopProxy(hgxlink=self, *args, **kwargs)
        obj._ghid_3141592 = make_random_ghid()
        obj._binder_3141592 = make_random_ghid()
        return obj
        
        
class MockObject:
    pass


# ###############################################
# Testing
# ###############################################
        
        
class ProxyTest(unittest.TestCase):
    ''' For now, just test the proxying stuff, and not the comms stuff.
    '''
    def do_proxy_pair(self, state):
        ''' Fixture for making a proxy object, and returning both it and
        the original object.
        '''
        return state, self.do_proxy_obj(state)
    
    def do_proxy_obj(self, state):
        ''' Fixture for making a dynamic object with state, for testing
        all of the proxy stuff.
        '''
        return self.embed.new_threadsafe(
            state = state, 
            api_id = bytes(64), 
            dynamic = True,
            private = False
        )
    
    def setUp(self):
        self.embed = MockEmbed()
        
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
    from _fixtures import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()