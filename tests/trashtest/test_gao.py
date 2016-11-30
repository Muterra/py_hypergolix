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

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Bookie
from hypergolix.core import GolixCore
from hypergolix.core import GhidProxier
from hypergolix.privateer import Privateer
from hypergolix.librarian import LibrarianCore

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
        
    def test_push(self):
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
                coro = GAO_s.push(),
                loop = self.nooploop._loop
            )
        
        GAO_d1 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        GAO_d1.frame_history.append(dbind1a.frame_ghid)
        GAO_d1.target_history.append(obj1.ghid)
        self.privateer.stage(obj1.ghid, secret1_1)
        
        await_coroutine_threadsafe(
            coro = GAO_d1.push(),
            loop = self.nooploop._loop
        )
        
        GAO_d2 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7,
                master_secret = secret1_1   # This isn't really correct but w/e
            ),
            loop = self.nooploop._loop
        )
        GAO_d2.frame_history.append(dbind1a.frame_ghid)
        GAO_d2.target_history.append(obj1.ghid)
        
        await_coroutine_threadsafe(
            coro = GAO_d2.push(),
            loop = self.nooploop._loop
        )
        
    def test_pull(self):
        logger.info('STARTING GAO PULL TEST!')
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj2, cont1_2.packed),
            loop = self.nooploop._loop
        )
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
                coro = GAO_s.pull(),
                loop = self.nooploop._loop
            )
        
        GAO_d1 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        GAO_d1.frame_history.append(dbind1a.frame_ghid)
        GAO_d1.target_history.append(obj1.ghid)
        self.privateer.stage(obj1.ghid, secret1_1)
        self.privateer.stage(obj2.ghid, secret1_2)
        
        # Do this late or we will accidentally overwrite other stuff.
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1b, dyn1_1b.packed),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = GAO_d1.pull(dbind1b.frame_ghid),
            loop = self.nooploop._loop
        )
        
        GAO_d2 = await_coroutine_threadsafe(
            coro = self.make_gao(
                ghid = dbind1a.ghid,
                dynamic = True,
                author = dbind1a.author,
                legroom = 7,
                master_secret = secret1_1   # This isn't really correct but w/e
            ),
            loop = self.nooploop._loop
        )
        GAO_d2.frame_history.append(dbind1a.frame_ghid)
        GAO_d2.target_history.append(obj1.ghid)
        
        await_coroutine_threadsafe(
            coro = GAO_d2.pull(dbind1b.frame_ghid),
            loop = self.nooploop._loop
        )


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
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
