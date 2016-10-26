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
import weakref
import gc

# These imports fall within the scope of testing.
from hypergolix.utils import _WeakSet
from hypergolix.utils import SetMap
from hypergolix.utils import WeakSetMap


# ###############################################
# Fixtures
# ###############################################


class Refferee:
    ''' Trivial class that supports both hashing and weak references.
    '''


# ###############################################
# Testing
# ###############################################


class _WeakSetTest(unittest.TestCase):
    ''' Test everything about a _WeakSet.
    '''
    
    def test_make(self):
        obj1 = Refferee()
        obj2 = Refferee()
        obj3 = Refferee()
        obj4 = Refferee()
        obj5 = Refferee()
        
        west0 = weakref.WeakSet((obj1, obj2, obj3, obj4, obj5))
        
        west1 = _WeakSet()
        west2 = _WeakSet((obj1, obj2, obj3, obj4, obj5))
        
        for obj in west0:
            self.assertIn(obj, west2)
            self.assertNotIn(obj, gc.get_referents(west1))
            self.assertNotIn(obj, gc.get_referents(west2))
        
        
# class LooperTrooperTest(unittest.TestCase):
#     def test_samethread(self):
#         looper = LooperFixture(threaded=False)
#         looper.start()
#         self.assertTrue(looper._sum >= 5050)
#         self.assertEqual(looper._counter, 0)
        
#     def test_otherthreads(self):
#         for __ in range(TEST_THIS_MANY_THREADED_LOOPERS):
#             looper = LooperFixture(threaded=True)
#             looper._thread.join(timeout=10)
#             self.assertTrue(looper._sum >= 5050)
#             self.assertEqual(looper._counter, 0)
            
#         looper.stop_threadsafe()
            
#         # pass
#         # self.server._halt()
        
#         # -------------------------------------------------------------------
#         # Comment this out if no interactivity desired
            
#         # # Start an interactive IPython interpreter with local namespace,
#         # # but suppress all IPython-related warnings.
#         # with warnings.catch_warnings():
#         #     warnings.simplefilter('ignore')
#         #     IPython.embed()

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
