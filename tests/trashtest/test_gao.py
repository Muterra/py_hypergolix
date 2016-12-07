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
import logging
import queue
import random
import inspect
import asyncio

from loopa import TaskLooper
from loopa import NoopLoop
from loopa.utils import await_coroutine_threadsafe

from hypergolix.gao import GAO
from hypergolix.gao import GAOCore
from hypergolix.gao import GAODict
from hypergolix.gao import GAOSet
from hypergolix.gao import GAOSetMap
from hypergolix.dispatch import _Dispatchable

from hypergolix.utils import ApiID

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Bookie
from hypergolix.core import GolixCore
from hypergolix.core import GhidProxier
from hypergolix.privateer import Privateer
from hypergolix.librarian import LibrarianCore

from hypergolix.ipc import IPCServerProtocol
from hypergolix.dispatch import Dispatcher

from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobsLite
from hypergolix.persistence import _GobdLite
from hypergolix.persistence import _GdxxLite
from hypergolix.persistence import _GarqLite


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid
from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2

from golix._getlow import GIDC
from _fixtures.remote_exchanges import gidc1
from _fixtures.remote_exchanges import secret1_1
from _fixtures.remote_exchanges import secret1_2
from _fixtures.remote_exchanges import cont1_1
from _fixtures.remote_exchanges import cont1_2
from _fixtures.remote_exchanges import bind1_1
from _fixtures.remote_exchanges import dyn1_1a
from _fixtures.remote_exchanges import dyn1_1b
from _fixtures.remote_exchanges import debind1_1
from _fixtures.remote_exchanges import dyndebind1_1

logger = logging.getLogger(__name__)

gidclite1 = _GidcLite.from_golix(GIDC.unpack(gidc1))
obj1 = _GeocLite.from_golix(cont1_1)
obj2 = _GeocLite.from_golix(cont1_2)
sbind1 = _GobsLite.from_golix(bind1_1)
dbind1a = _GobdLite.from_golix(dyn1_1a)
dbind1b = _GobdLite.from_golix(dyn1_1b)
xbind1 = _GdxxLite.from_golix(debind1_1)
xbind1d = _GdxxLite.from_golix(dyndebind1_1)


class GAOTestingCore:
    ''' Unified testing mechanism for GAO. Just add water (err, a
    make_gao method and a setUp method) to start.
    '''
    
    async def make_gao(self, ghid, dynamic, author, legroom, *args, **kwargs):
        ''' Must be capable of making a gao from this input. This is
        just here for reference.
        '''
        
    async def modify_gao(self, obj):
        ''' Inplace modification to gao for pushing. Allowed to be noop
        if (and only if) subsequent push() commands will still generate
        a new frame (ie, this must be defined for anything that tracks
        deltas, if adequate test coverage is to be hit).
        '''
    
    @classmethod
    def setUpClass(cls):
        cls.nooploop = NoopLoop(
            debug = True,
            threaded = True
        )
        cls.nooploop.start()
        
    @classmethod
    def tearDownClass(cls):
        # Kill the running loop.
        cls.nooploop.stop_threadsafe_nowait()
        
    def test_make(self):
        ''' Test making a GAO.
        '''
        GAO1 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = None,
                dynamic = None,
                author = None,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        self.assertTrue(isinstance(GAO1, GAOCore))
        
        GAO2 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = None,
                dynamic = None,
                author = None,
                legroom = 7,
                master_secret = self.privateer.new_secret()
            ),
            loop = self.nooploop._loop
        )
        self.assertTrue(isinstance(GAO2, GAOCore))
        
    def test_apply_delete(self):
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1, debind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1d, dyndebind1_1.packed),
            loop = self.nooploop._loop
        )
        # self.ghidproxy.lookup[dbind1a.ghid] = obj1.ghid
        
        GAO_s = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = obj1.ghid,
                dynamic = False,
                author = obj1.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = GAO_s.apply_delete(xbind1),
            loop = self.nooploop._loop
        )
        self.assertFalse(GAO_s.isalive)
        
        GAO_d = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = GAO_d.apply_delete(xbind1d),
            loop = self.nooploop._loop
        )
        self.assertFalse(GAO_d.isalive)
        
    def test_freeze(self):
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1, debind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1d, dyndebind1_1.packed),
            loop = self.nooploop._loop
        )
        # self.ghidproxy.lookup[dbind1a.ghid] = obj1.ghid
        
        GAO_s = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = obj1.ghid,
                dynamic = False,
                author = obj1.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        with self.assertRaises(TypeError):
            await_coroutine_threadsafe(
                coro = GAO_s.freeze(),
                loop = self.nooploop._loop
            )
        
        GAO_d = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        frozen = await_coroutine_threadsafe(
            coro = GAO_d.freeze(),
            loop = self.nooploop._loop
        )
        self.assertEqual(frozen, obj1.ghid)
        
    def test_hold(self):
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1, debind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1d, dyndebind1_1.packed),
            loop = self.nooploop._loop
        )
        # self.ghidproxy.lookup[dbind1a.ghid] = obj1.ghid
        
        GAO_s = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = obj1.ghid,
                dynamic = False,
                author = obj1.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = GAO_s.hold(),
            loop = self.nooploop._loop
        )
        
        GAO_d = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = GAO_d.hold(),
            loop = self.nooploop._loop
        )
        
    def test_delete(self):
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1, debind1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1d, dyndebind1_1.packed),
            loop = self.nooploop._loop
        )
        # self.ghidproxy.lookup[dbind1a.ghid] = obj1.ghid
        
        GAO_d = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = GAO_d.delete(),
            loop = self.nooploop._loop
        )
        
        GAO_s = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = obj1.ghid,
                dynamic = False,
                author = obj1.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = GAO_s.delete(),
            loop = self.nooploop._loop
        )
        
    def test_pushme_pullyou_static(self):
        ''' Test pushing and pulling static objects, ensuring that both
        directions work (and produce symmetric objects).
        '''
        # We need to have our author "on file" for any pulls.
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        
        # First create a new static object.
        GAO_s1 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = None,
                dynamic = False,
                author = None,
                legroom = 0
            ),
            loop = self.nooploop._loop
        )
        # Note that we call directly to _push here (note underscore)
        await_coroutine_threadsafe(
            coro = GAO_s1._push(),
            loop = self.nooploop._loop
        )
        
        # Make sure a ghid was actually assigned.
        self.assertTrue(GAO_s1.ghid)
        
        # Now test normal push (no underscore), which should fail.
        with self.assertRaises(TypeError):
            await_coroutine_threadsafe(
                coro = GAO_s1.push(),
                loop = self.nooploop._loop
            )
            
        # Now attemt to get it back...
        GAO_s2 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = GAO_s1.ghid,
                dynamic = None,
                author = None,
                legroom = 0
            ),
            loop = self.nooploop._loop
        )
        # A direct call to gao._pull() (note underscore) should succeed.
        await_coroutine_threadsafe(
            coro = GAO_s2._pull(),
            loop = self.nooploop._loop
        )
        # It should also compare equally to the original object.
        self.assertEqual(GAO_s1, GAO_s2)
        
        # But calls to gao.pull() (no underscore) should TypeError.
        with self.assertRaises(TypeError):
            await_coroutine_threadsafe(
                coro = GAO_s2.pull(notification=GAO_s1.ghid),
                loop = self.nooploop._loop
            )
        
    def test_pushme_pullyou_dynamic(self):
        ''' Test pushing and pulling dynamic objects, ensuring that both
        directions work (and produce symmetric objects).
        '''
        # We need to have our author "on file" for any pulls.
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        
        GAO_d1 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = None,
                dynamic = True,
                author = None,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = GAO_d1.push(),
            loop = self.nooploop._loop
        )
        
        # Make sure a ghid was actually assigned.
        self.assertTrue(GAO_d1.ghid)
        
        # Now make sure retrieval (of first frame) works correctly
        GAO_d2 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = GAO_d1.ghid,
                dynamic = None,
                author = None,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        # Note that the first time, we call (underscored) gao._pull() directly
        await_coroutine_threadsafe(
            coro = GAO_d2._pull(),
            loop = self.nooploop._loop
        )
        # These should compare equally!
        self.assertEqual(GAO_d1, GAO_d2)
        
        # NOW, test with a second frame.
        # Modify the original in-place
        await_coroutine_threadsafe(
            coro = self.modify_gao(GAO_d1),
            loop = self.nooploop._loop
        )
        # Push the updates
        await_coroutine_threadsafe(
            coro = GAO_d1.push(),
            loop = self.nooploop._loop
        )
        
        # Pull them on the second object, using complete gao.pull()
        await_coroutine_threadsafe(
            # Note that this is taking advantage of the fact that librarians
            # can retrieve via either dynamic or frame ghid.
            coro = GAO_d2.pull(notification=GAO_d1.ghid),
            loop = self.nooploop._loop
        )
        # These should (still) compare equally!
        self.assertEqual(GAO_d1, GAO_d2)
        
        # NOW, test with a third frame.
        # Modify the original in-place
        await_coroutine_threadsafe(
            coro = self.modify_gao(GAO_d1),
            loop = self.nooploop._loop
        )
        # Push the updates
        await_coroutine_threadsafe(
            coro = GAO_d1.push(),
            loop = self.nooploop._loop
        )
        
        # Pull them on the second object, using complete gao.pull()
        await_coroutine_threadsafe(
            # Note that this is taking advantage of the fact that librarians
            # can retrieve via either dynamic or frame ghid.
            coro = GAO_d2.pull(notification=GAO_d1.ghid),
            loop = self.nooploop._loop
        )
        # These should (still) compare equally!
        self.assertEqual(GAO_d1, GAO_d2)


# ###############################################
# Testing
# ###############################################
    
    
class GAOTest(GAOTestingCore, unittest.TestCase):
    ''' Test the standard GAO.
    '''
        
    def setUp(self):
        # These are directly required by the GAO
        self.librarian = LibrarianCore.__fixture__()
        self.golcore = GolixCore.__fixture__(TEST_AGENT1,
                                             librarian=self.librarian)
        # Don't fixture this. We need to actually resolve things.
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer.__fixture__(TEST_AGENT1)
        self.percore = PersistenceCore.__fixture__(librarian=self.librarian)
        # Some assembly required
        self.ghidproxy.assemble(self.librarian)
    
    async def make_gao(self, ghid, dynamic, author, legroom, *args, **kwargs):
        ''' Make a standard GAO.
        '''
        return GAO(
            ghid,
            dynamic,
            author,
            legroom,
            state = bytes([random.randint(0, 255) for i in range(32)]),
            *args,
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            **kwargs
        )
        
    async def modify_gao(self, obj):
        ''' Update state.
        '''
        obj.state = bytes([random.randint(0, 255) for i in range(32)])
    
    
class GAODictTest(GAOTestingCore, unittest.TestCase):
    ''' Test the standard GAO.
    '''
        
    def setUp(self):
        # These are directly required by the GAO
        self.librarian = LibrarianCore.__fixture__()
        self.golcore = GolixCore.__fixture__(TEST_AGENT1,
                                             librarian=self.librarian)
        # Don't fixture this. We need to actually resolve things.
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer.__fixture__(TEST_AGENT1)
        self.percore = PersistenceCore.__fixture__(librarian=self.librarian)
        # Some assembly required
        self.ghidproxy.assemble(self.librarian)
    
    async def make_gao(self, ghid, dynamic, author, legroom, *args, **kwargs):
        ''' Make a standard GAO.
        '''
        return GAODict(
            ghid,
            dynamic,
            author,
            legroom,
            state = {1: bytes([random.randint(0, 255) for i in range(32)])},
            *args,
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            **kwargs
        )
        
    async def modify_gao(self, obj):
        ''' Update state.
        '''
        obj.state[1] = bytes([random.randint(0, 255) for i in range(32)])
        
    def test_dict_stuff(self):
        ''' Make sure gaodict acts like, well, a dict.
        '''
        gao = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = None,
                dynamic = True,
                author = None,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        gao[1] = 5
        val = gao.pop(1)
        self.assertEqual(val, 5)
        gao.clear()
        gao[1] = 7
        self.assertEqual(gao[1], 7)
        del gao[1]
        gao.update({1: 1, 2: 2, 3: 3})
        len(gao)
        1 in gao
        for it in gao:
            pass
    
    
class GAOSetTest(GAOTestingCore, unittest.TestCase):
    ''' Test the standard GAO.
    '''
        
    def setUp(self):
        # These are directly required by the GAO
        self.librarian = LibrarianCore.__fixture__()
        self.golcore = GolixCore.__fixture__(TEST_AGENT1,
                                             librarian=self.librarian)
        # Don't fixture this. We need to actually resolve things.
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer.__fixture__(TEST_AGENT1)
        self.percore = PersistenceCore.__fixture__(librarian=self.librarian)
        # Some assembly required
        self.ghidproxy.assemble(self.librarian)
    
    async def make_gao(self, ghid, dynamic, author, legroom, *args, **kwargs):
        ''' Make a standard GAO.
        '''
        return GAOSet(
            ghid,
            dynamic,
            author,
            legroom,
            state = {bytes([random.randint(0, 255) for i in range(32)])},
            *args,
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            **kwargs
        )
        
    async def modify_gao(self, obj):
        ''' Update state.
        '''
        obj.state.add(bytes([random.randint(0, 255) for i in range(32)]))
        
    def test_set_stuff(self):
        ''' Make sure gaodict acts like, well, a dict.
        '''
        gao = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = None,
                dynamic = True,
                author = None,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        gao.clear()
        gao.add(5)
        val = gao.pop()
        self.assertEqual(val, 5)
        gao.add(7)
        gao.update({1, 2, 3})
        len(gao)
        1 in gao
        for it in gao:
            pass
    
    
class GAOSetMapTest(GAOTestingCore, unittest.TestCase):
    ''' Test the standard GAO.
    '''
        
    def setUp(self):
        # These are directly required by the GAO
        self.librarian = LibrarianCore.__fixture__()
        self.golcore = GolixCore.__fixture__(TEST_AGENT1,
                                             librarian=self.librarian)
        # Don't fixture this. We need to actually resolve things.
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer.__fixture__(TEST_AGENT1)
        self.percore = PersistenceCore.__fixture__(librarian=self.librarian)
        # Some assembly required
        self.ghidproxy.assemble(self.librarian)
    
    async def make_gao(self, ghid, dynamic, author, legroom, *args, **kwargs):
        ''' Make a standard GAO.
        '''
        return GAOSetMap(
            ghid,
            dynamic,
            author,
            legroom,
            *args,
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            **kwargs
        )
        
    async def modify_gao(self, obj):
        ''' Update state.
        '''
        obj.state.add(
            bytes([random.randint(0, 255) for i in range(4)]),
            bytes([random.randint(0, 255) for i in range(32)])
        )
        
    def test_setmap_stuff(self):
        ''' Make sure gaodict acts like, well, a dict.
        '''
        gao = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = None,
                dynamic = True,
                author = None,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        
        gao.clear_all()
        gao.add(5, 5)
        val = gao.pop_any(5)
        self.assertEqual(val, {5})
        gao.add(7, 7)
        gao.update(5, {1, 2, 3})
        len(gao)
        1 in gao
        for it in gao:
            pass
    
    
class DispatchableTest(GAOTestingCore, unittest.TestCase):
    ''' Test the standard GAO.
    '''
        
    def setUp(self):
        # These are directly required by the GAO
        self.librarian = LibrarianCore.__fixture__()
        self.golcore = GolixCore.__fixture__(TEST_AGENT1,
                                             librarian=self.librarian)
        # Don't fixture this. We need to actually resolve things.
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer.__fixture__(TEST_AGENT1)
        self.percore = PersistenceCore.__fixture__(librarian=self.librarian)
        # Some assembly required
        self.ghidproxy.assemble(self.librarian)
        self.ipc_protocol = IPCServerProtocol.__fixture__(whoami=None)
        self.dispatch = Dispatcher.__fixture__()
    
    async def make_gao(self, ghid, dynamic, author, legroom, *args, **kwargs):
        ''' Make a standard GAO.
        '''
        return _Dispatchable(
            ghid,
            dynamic,
            author,
            legroom,
            *args,
            state = bytes([random.randint(0, 255) for i in range(32)]),
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol,
            **kwargs
        )
        
    async def modify_gao(self, obj):
        ''' Update state.
        '''
        obj.state = bytes([random.randint(0, 255) for i in range(32)])
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
