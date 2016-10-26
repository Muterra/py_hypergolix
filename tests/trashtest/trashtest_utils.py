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
# from hypergolix.utils import _WeakerSet
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


class WeakSetTest(unittest.TestCase):
    ''' Test everything about a _WeakSet.
    
    TODO: 100% coverage (we don't have it yet).
    '''
    TEST_CLS = _WeakSet
    
    def test_make(self):
        obj1 = Refferee()
        obj2 = Refferee()
        obj3 = Refferee()
        obj4 = Refferee()
        obj5 = Refferee()
        
        west0 = weakref.WeakSet((obj1, obj2, obj3, obj4, obj5))
        
        west1 = self.TEST_CLS()
        west2 = self.TEST_CLS((obj1, obj2, obj3, obj4, obj5))
        
        for obj in west0:
            self.assertIn(obj, west2)
            self.assertNotIn(obj, gc.get_referents(west1))
            self.assertNotIn(obj, gc.get_referents(west2))
    
    def test_iter(self):
        ''' Here we want to test three things:
        1. that iteration works
        2. that iteration correctly defers removal until after iteration
        3. that removal occurs immediately after iteration
        '''
        objs = [Refferee() for __ in range(10)]
        objrefs = [weakref.ref(obj) for obj in objs]
        west1 = self.TEST_CLS(objs)
        
        # Does iteration work?
        for obj1 in west1:
            self.assertIn(obj1, objs)
            
        # Does iteration defer removal until after iteration?
        referents_before = len(gc.get_referents(west1))
        for ii, obj1 in enumerate(west1):
            # Delete the first member.
            if ii == 0:
                # Remove the only strong reference to objs[0] (which is not
                # necessarily the same as the obj1 from enumerate, because
                # set iteration order is undefined
                del objs[0]
                # Make sure the reference persists
                self.assertEqual(
                    referents_before,
                    len(gc.get_referents(west1))
                )
            
            # Wait until we get to the next object to make sure the reference
            # is actually dead, just in case we were enumerating over the same
            # object -- in which case, obj1 would hold a strong reference. For
            # good measure, do an explicit GC call first.
            elif ii == 1:
                gc.collect()
                self.assertIsNone(objrefs[0]())
        
        # Does removal occur immediately after iteration?
        self.assertEqual(
            referents_before - 1,
            len(gc.get_referents(west1))
        )
    
    def test_contains(self):
        ''' Here we want to test one quick thing: that contains works
        with objects.
        '''
        objs = [Refferee() for __ in range(10)]
        west1 = self.TEST_CLS(objs)
        
        self.assertIn(objs[0], west1)
    
    def test_add(self):
        objs = [Refferee() for __ in range(10)]
        other = Refferee()
        west1 = self.TEST_CLS(objs)
        
        self.assertNotIn(other, west1)
        
        # Make sure we don't add extra references to the same thing
        referents_before = len(gc.get_referents(west1))
        west1.add(objs[0])
        self.assertEqual(
            referents_before,
            len(gc.get_referents(west1))
        )
        # And that it's still there
        self.assertIn(objs[0], west1)
        
        # Now make sure add works for something new
        west1.add(other)
        self.assertIn(other, west1)
    
    def test_weakness(self):
        ''' Make sure removal of weakrefs happens appropriately.
        '''
        objs = [Refferee() for __ in range(10)]
        west1 = self.TEST_CLS(objs)
        self.assertEqual(len(objs), len(west1))
        del objs
        # Explicit GC call, just in case
        gc.collect()
        self.assertEqual(len(west1), 0)
        
    def test_eq(self):
        ''' Make sure equal things compare equally.
        '''
        objs = {Refferee() for __ in range(10)}
        west0 = weakref.WeakSet(objs)
        west1 = self.TEST_CLS(objs)
        gc.collect()
        self.assertEqual(west1, objs)
        self.assertEqual(west1, west0)
        
    def test_len(self):
        ''' Make sure equal things compare equally. Also, length, just
        because it seems relevant.
        '''
        objnum = 10
        objs = {Refferee() for __ in range(objnum)}
        west1 = self.TEST_CLS(objs)
        gc.collect()
        self.assertEqual(west1, objs)
        self.assertEqual(objnum, len(west1))
    
    def test_inplace_stuff(self):
        ''' Test copy, pop, remove, etc.
        '''
        objs = {Refferee() for __ in range(10)}
        west0 = weakref.WeakSet(objs)
        west1 = self.TEST_CLS(objs)
        
        west2 = west1.copy()
        self.assertEqual(west1, west2)
        self.assertEqual(west2, west0)
        
        obj = west1.pop()
        self.assertNotIn(obj, west1)
        self.assertIn(obj, west2)
        
        west2.remove(obj)
        self.assertNotIn(obj, west2)
        self.assertIn(obj, west0)
        
        obj = west1.pop()
        west2.discard(obj)
        self.assertNotIn(obj, west2)
        west2.discard(obj)
        
        west2.update(objs)
        self.assertEqual(west2, objs)
        self.assertNotEqual(west1, objs)
        
        
@unittest.skipIf(True, 'DNE')
class WeakerSetTest(WeakSetTest):
    ''' Test creation of _WeakerSets.
    '''
    # TEST_CLS = _WeakerSet
    
    def test_make(self):
        objs = {Refferee() for __ in range(10)}
        west0 = {weakref.ref(item) for item in objs}
        
        west1 = self.TEST_CLS()
        gc.collect()
        self.assertTrue(west1._live)
        west2 = self.TEST_CLS(objs)
        gc.collect()
        self.assertTrue(west2._live)
        
        for obj in objs:
            self.assertTrue(west2._live)
            self.assertIn(obj, west2)
            self.assertNotIn(obj, gc.get_referents(west1))
            self.assertNotIn(obj, gc.get_referents(west2))
        
        
class SetMapTest(unittest.TestCase):
    ''' Test normal SetMaps.
    '''
    TEST_CLS = SetMap
    
    def test_make(self):
        # Trivially check to see that it creates w/out error.
        self.TEST_CLS()
        
    def test_setgetupdate(self):
        ''' Test setting items, getting items, and clear.
        '''
        sm = self.TEST_CLS()
        
        a = Refferee()
        b = Refferee()
        c = Refferee()
        
        sm.add(1, a)
        gc.collect()
        self.assertIn(1, sm._mapping)
        self.assertIn(a, sm[1])
        self.assertEqual(sm[1], {a})
        
        sm.add(1, b)
        gc.collect()
        self.assertIn(1, sm._mapping)
        self.assertEqual(sm[1], {a, b})
        
        self.assertEqual(sm.get_any(1), {a, b})
        # get_any should never raise, even if nothing is there
        self.assertEqual(sm.get_any(2), set())
        
        popped = sm.pop_any(1)
        self.assertEqual(popped, {a, b})
        empty = sm.pop_any(2)
        self.assertEqual(empty, set())
        
        sm.clear_all()
        gc.collect()
        self.assertEqual(sm._mapping, {})
        
        sm.update(2, {a, b, c})
        gc.collect()
        self.assertEqual(sm._mapping[2], {a, b, c})
        
        sm.clear(2)
        gc.collect()
        self.assertEqual(sm._mapping, {})
        
        sm.update(2, {a, b, c})
        gc.collect()
        self.assertEqual(sm._mapping[2], {a, b, c})
        
        sm.clear_any(2)
        gc.collect()
        self.assertEqual(sm._mapping, {})
        sm.clear_any(2)
        gc.collect()
        
        sm.add(1, a)
        sm.add(1, b)
        gc.collect()
        self.assertTrue(sm.contains_within(1, b))
        sm.discard(1, b)
        gc.collect()
        self.assertFalse(sm.contains_within(1, b))
        sm.discard(1, b)
        sm.discard(1, b)
        gc.collect()
        
        sm.remove(1, a)
        gc.collect()
        self.assertNotIn(1, sm)
        
    def test_contains(self):
        ''' Test contains and contains_within. Also, bool, len, eq,
        iter, and combine, just because.
        '''
        a = Refferee()
        b = Refferee()
        c = Refferee()
        
        sm = self.TEST_CLS()
        gc.collect()
        self.assertFalse(bool(sm))
        
        sm.add(1, a)
        gc.collect()
        self.assertTrue(bool(sm))
        
        self.assertIn(1, sm)
        self.assertNotIn(2, sm)
        self.assertTrue(sm.contains_within(1, a))
        self.assertFalse(sm.contains_within(2, a))
        self.assertFalse(sm.contains_within(1, b))
        
        sm1 = self.TEST_CLS()
        sm2 = self.TEST_CLS()
        gc.collect()
        
        self.assertEqual(sm1, sm2)
        sm1.add(1, a)
        sm2.add(1, a)
        gc.collect()
        self.assertEqual(sm1, sm2)
        sm1.add(1, b)
        gc.collect()
        self.assertNotEqual(sm1, sm2)
        
        self.assertEqual(len(sm1), 1)
        self.assertEqual(len(sm2), 1)
        
        sm = self.TEST_CLS()
        sm.add(1, a)
        sm.add(2, a)
        gc.collect()
        dd = {1: a, 2: a}
        for item in sm:
            self.assertIn(item, dd)
            
        sm1 = self.TEST_CLS()
        sm2 = self.TEST_CLS()
        sm3 = self.TEST_CLS()
        sm1.add(1, a)
        sm2.add(1, b)
        sm3.add(2, c)
        gc.collect()
        
        sm12 = self.TEST_CLS()
        sm12.add(1, a)
        sm12.add(1, b)
        gc.collect()
        
        sm13 = self.TEST_CLS()
        sm13.add(1, a)
        sm13.add(2, c)
        gc.collect()
        
        self.assertEqual(sm1.combine(sm2), sm12)
        self.assertEqual(sm1.combine(sm3), sm13)
        
        
class WeakSetMapTest(SetMapTest):
    ''' Test weakreffed setmaps.
    '''
    TEST_CLS = WeakSetMap
    
    def test_weakness(self):
        ''' Make sure all of the weak stuff works as advertised.
        '''
        sm = self.TEST_CLS()
        
        a = Refferee()
        a1 = weakref.ref(a)
        b = Refferee()
        b1 = weakref.ref(b)
        c = Refferee()
        c1 = weakref.ref(c)
        
        sm.add(1, a)
        sm.add(1, b)
        sm.add(2, c)
        
        gc.collect()
        self.assertIn(1, sm._mapping)
        self.assertIn(2, sm._mapping)
        self.assertTrue(sm.contains_within(1, a))
        self.assertTrue(sm.contains_within(1, b))
        self.assertTrue(sm.contains_within(2, c))
        
        # Do explicit GC just in case
        del c
        gc.collect()
        self.assertIsNone(c1())
        self.assertIsNotNone(b1())
        self.assertIsNotNone(a1())
        self.assertNotIn(2, sm._mapping)
        self.assertIn(1, sm._mapping)
        
        del b
        gc.collect()
        self.assertIsNone(b1())
        self.assertIsNotNone(a1())
        self.assertNotIn(2, sm._mapping)
        self.assertIn(1, sm._mapping)
        
        del a
        gc.collect()
        self.assertIsNone(a1())
        self.assertNotIn(1, sm._mapping)
        self.assertNotIn(2, sm._mapping)


if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
