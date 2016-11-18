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
import concurrent.futures

from loopa import NoopLoop
from loopa.utils import await_coroutine_threadsafe

from hypergolix.core import GolixCore
from hypergolix.core import Oracle
from hypergolix.core import GhidProxier

from hypergolix.gao import GAOCore
from hypergolix.privateer import Privateer
from hypergolix.remotes import Salmonator
from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Bookie
from hypergolix.librarian import LibrarianCore
from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobdLite

# These are fixture imports
from golix import Ghid


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
from _fixtures.ghidutils import make_random_ghid
        
        
class MockCredential:
    ''' Temporary bypass of password inflation for purpose of getting
    the privateer bootstrap up and running. JUST FOR TESTING PURPOSES.
    '''
    
    def __init__(self, privateer, identity):
        self.identity = identity
        self._lookup = {}
        self._privateer = privateer
        
    def get_master(self, proxy):
        # JIT creation of the proxy secret. JUST FOR TESTING PURPOSES.
        if proxy not in self._lookup:
            self._lookup[proxy] = self._privateer.new_secret()
        return self._lookup[proxy]
        
    def is_primary(self, ghid):
        # Probably always false?
        return False


# ###############################################
# Testing
# ###############################################


class GolcoreTest(unittest.TestCase):
    ''' Test operations with GolixCore.
    '''
    
    @classmethod
    def setUpClass(cls):
        cls.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        cls.nooploop = NoopLoop(
            debug = True,
            threaded = True
        )
        cls.nooploop.start()
        
    @classmethod
    def tearDownClass(cls):
        # Kill the running loop.
        cls.nooploop.stop_threadsafe_nowait()
    
    def setUp(self):
        self.librarian = LibrarianCore.__fixture__()
        
        self.golcore = GolixCore(self.executor, self.nooploop._loop)
        self.golcore.assemble(self.librarian)
        self.golcore.bootstrap(MockCredential(None, TEST_AGENT1))
        
        # Our librarian fixturing is a little inadequate, so for now just
        # manually add this stuff.
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(
                _GidcLite(TEST_AGENT1.ghid, TEST_AGENT1.second_party),
                TEST_AGENT1.second_party.packed
            ),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(
                _GidcLite(TEST_AGENT2.ghid, TEST_AGENT2.second_party),
                TEST_AGENT2.second_party.packed
            ),
            loop = self.nooploop._loop
        )
        
    def test_trash(self):
        from _fixtures.remote_exchanges import handshake2_1
        from _fixtures.remote_exchanges import secret2_1
        from _fixtures.remote_exchanges import cont2_1

        # Test whoami
        self.assertEqual(self.golcore.whoami, TEST_AGENT1.ghid)
        
        # Note that all of these are stateless, so they don't need to be actual
        # legitimate targets or anything, just actual GHIDs
        payload = TEST_AGENT1.make_handshake(
            target = cont2_1.ghid,
            secret = secret2_1)
        
        unpacked = await_coroutine_threadsafe(
            coro = self.golcore.unpack_request(handshake2_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.golcore.open_request(unpacked),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = self.golcore.make_request(
                recipient = TEST_AGENT2.ghid,
                payload = payload
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = self.golcore.open_container(
                container = cont2_1,
                secret = secret2_1
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = self.golcore.make_container(
                data = b'Hello world',
                secret = TEST_AGENT1.new_secret()
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = self.golcore.make_binding_stat(target=cont2_1.ghid),
            loop = self.nooploop._loop
        )
        
        dynamic1 = await_coroutine_threadsafe(
            coro = self.golcore.make_binding_dyn(target=cont2_1.ghid),
            loop = self.nooploop._loop
        )
        
        dynamic2 = await_coroutine_threadsafe(
            coro = self.golcore.make_binding_dyn(
                target = cont2_1.ghid,
                ghid = dynamic1.ghid_dynamic,
                history = [dynamic1.ghid]
            ),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = self.golcore.make_debinding(target=dynamic2.ghid_dynamic),
            loop = self.nooploop._loop
        )

        
class GhidproxyTest(unittest.TestCase):
    ''' Test ghidproxier resolving, etc
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
        
    def setUp(self):
        self.librarian = LibrarianCore.__fixture__()
        
        self.ghidproxy = GhidProxier()
        self.ghidproxy.assemble(self.librarian)
    
    def test_trash(self):
        # Test round #1...
        from _fixtures.remote_exchanges import cont1_1
        geoc1_1 = _GeocLite(cont1_1.ghid, cont1_1.author)
        from _fixtures.remote_exchanges import cont1_2
        geoc1_2 = _GeocLite(cont1_2.ghid, cont1_2.author)
        from _fixtures.remote_exchanges import dyn1_1a
        gobd1_a = _GobdLite(
            dyn1_1a.ghid_dynamic,
            dyn1_1a.binder,
            dyn1_1a.target,
            dyn1_1a.ghid,
            dyn1_1a.history)
        from _fixtures.remote_exchanges import dyn1_1b
        gobd1_b = _GobdLite(
            dyn1_1b.ghid_dynamic,
            dyn1_1b.binder,
            dyn1_1b.target,
            dyn1_1b.ghid,
            dyn1_1b.history)
        
        # Hold on to your butts!
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(geoc1_1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(geoc1_2, cont1_2.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gobd1_a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.ghidproxy.resolve(dyn1_1a.ghid_dynamic),
                loop = self.nooploop._loop
            ),
            cont1_1.ghid
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gobd1_b, dyn1_1b.packed),
            loop = self.nooploop._loop
        )
        # Intentionally reuse old dynamic ghid, even though it's the same
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.ghidproxy.resolve(dyn1_1a.ghid_dynamic),
                loop = self.nooploop._loop
            ),
            cont1_2.ghid
        )
        
        # Test round #2...
        from _fixtures.remote_exchanges import cont2_1
        geoc2_1 = _GeocLite(cont2_1.ghid, cont2_1.author)
        from _fixtures.remote_exchanges import cont2_2
        geoc2_2 = _GeocLite(cont2_2.ghid, cont2_2.author)
        from _fixtures.remote_exchanges import dyn2_1a
        gobd2_a = _GobdLite(
            dyn2_1a.ghid_dynamic,
            dyn2_1a.binder,
            dyn2_1a.target,
            dyn2_1a.ghid,
            dyn2_1a.history)
        from _fixtures.remote_exchanges import dyn2_1b
        gobd2_b = _GobdLite(
            dyn2_1b.ghid_dynamic,
            dyn2_1b.binder,
            dyn2_1b.target,
            dyn2_1b.ghid,
            dyn2_1b.history)
        
        # Hold on to your butts!
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(geoc2_1, cont2_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(geoc2_2, cont2_2.packed),
            loop = self.nooploop._loop
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gobd2_a, dyn2_1a.packed),
            loop = self.nooploop._loop
        )
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.ghidproxy.resolve(dyn2_1a.ghid_dynamic),
                loop = self.nooploop._loop
            ),
            cont2_1.ghid
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gobd2_b, dyn2_1b.packed),
            loop = self.nooploop._loop
        )
        # Intentionally reuse old dynamic ghid, even though it's the same
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.ghidproxy.resolve(dyn2_1a.ghid_dynamic),
                loop = self.nooploop._loop
            ),
            cont2_2.ghid
        )

        
class OracleTest(unittest.TestCase):
    
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
    
    def setUp(self):
        # This is what we're testing!
        self.oracle = Oracle()
        
        # These are directly required by the oracle
        self.golcore = GolixCore.__fixture__(TEST_AGENT1)
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer.__fixture__(TEST_AGENT1)
        self.percore = PersistenceCore.__fixture__()
        self.bookie = Bookie.__fixture__()
        self.librarian = LibrarianCore.__fixture__()
        self.salmonator = Salmonator.__fixture__()
        
        # This is obviously necessary to test.
        self.oracle.assemble(self.golcore, self.ghidproxy, self.privateer,
                             self.percore, self.bookie, self.librarian,
                             self.salmonator)
    
    def test_load(self):
        ''' Test getting an existing object.
        '''
        # Test actual object getting
        ghid = make_random_ghid()
        obj = await_coroutine_threadsafe(
            coro = self.oracle.get_object(
                gaoclass = GAOCore.__fixture__,
                ghid = ghid
            ),
            loop = self.nooploop._loop
        )
        self.assertEqual(obj.ghid, ghid)
        
        # Now make sure that running it a second time returns the cached (and
        # identical) object
        obj2 = await_coroutine_threadsafe(
            coro = self.oracle.get_object(
                gaoclass = GAOCore.__fixture__,
                ghid = ghid
            ),
            loop = self.nooploop._loop
        )
        self.assertTrue(obj is obj2)
    
    def test_new(self):
        ''' Test creating new objects.
        '''
        # Test actual object creation
        obj = await_coroutine_threadsafe(
            coro = self.oracle.new_object(
                gaoclass = GAOCore.__fixture__,
                dynamic = True,
                legroom = 7
            ),
            loop = self.nooploop._loop
        )
        # Make sure that oracle calls _push() to create a ghid, etc
        self.assertTrue(obj.ghid)
        self.assertTrue(obj.dynamic)
        self.assertTrue(obj.legroom == 7)
        
        # Now make sure that running get after that returns the cached (and
        # identical) object
        obj2 = await_coroutine_threadsafe(
            coro = self.oracle.get_object(
                gaoclass = GAOCore.__fixture__,
                ghid = obj.ghid
            ),
            loop = self.nooploop._loop
        )
        self.assertTrue(obj is obj2)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
