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

from loopa import NoopLoop
from loopa.utils import await_coroutine_threadsafe

from golix._getlow import GIDC

from hypergolix.persistence import Enforcer
from hypergolix.persistence import PersistenceCore
from hypergolix.lawyer import LawyerCore
from hypergolix.librarian import LibrarianCore

from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobsLite
from hypergolix.persistence import _GobdLite
from hypergolix.persistence import _GdxxLite
from hypergolix.persistence import _GarqLite

from hypergolix.exceptions import InvalidIdentity
from hypergolix.exceptions import InconsistentAuthor


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid
from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
from _fixtures.identities import TEST_READER1
from _fixtures.identities import TEST_READER2

# Stuff to store
from _fixtures.remote_exchanges import cont1_1  # Container
from _fixtures.remote_exchanges import dyn1_1a  # Dynamic binding frame 1
from _fixtures.remote_exchanges import dyn1_1b  # Dynamic binding frame 2
from _fixtures.remote_exchanges import handshake1_1
from _fixtures.remote_exchanges import debind1_1
geoc1_1 = _GeocLite.from_golix(cont1_1)
gobd1_a = _GobdLite.from_golix(dyn1_1a)
gobd1_b = _GobdLite.from_golix(dyn1_1b)
garq1_1 = _GarqLite.from_golix(handshake1_1)
gdxx1_1 = _GdxxLite.from_golix(debind1_1)


# ###############################################
# Testing
# ###############################################


class GenericLibrarianTest:
    ''' Test any kind of librarian by subclassing this and defining a
    setUp method that defines the librarian.
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
        
    def test_contains(self):
        ''' Test librarian.contains(ghid).
        '''
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(geoc1_1.ghid),
                loop = self.nooploop._loop
            )
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(geoc1_1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(geoc1_1.ghid),
                loop = self.nooploop._loop
            )
        )
        
    def test_resolution(self):
        ''' Test librarian.resolve_frame(ghid).
        '''
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gobd1_a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.resolve_frame(gobd1_a.ghid),
                loop = self.nooploop._loop
            ),
            gobd1_a.frame_ghid
        )
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gobd1_b, dyn1_1b.packed),
            loop = self.nooploop._loop
        )
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.resolve_frame(gobd1_b.ghid),
                loop = self.nooploop._loop
            ),
            gobd1_b.frame_ghid
        )
        
    def test_recipient(self):
        ''' Test librarian.recipient_status(ghid).
        '''
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.recipient_status(garq1_1.recipient),
                loop = self.nooploop._loop
            ),
            frozenset()
        )
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(garq1_1, handshake1_1.packed),
            loop = self.nooploop._loop
        )
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.recipient_status(garq1_1.recipient),
                loop = self.nooploop._loop
            ),
            frozenset({garq1_1.ghid})
        )
        
    def test_bind_status(self):
        ''' Test librarian.bind_status(ghid).
        '''
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.bind_status(gobd1_a.target),
                loop = self.nooploop._loop
            ),
            frozenset()
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gobd1_a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.bind_status(gobd1_a.target),
                loop = self.nooploop._loop
            ),
            frozenset({gobd1_a.ghid})
        )
        
    def test_debind_status(self):
        ''' Test librarian.debind_status(ghid).
        '''
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.debind_status(gdxx1_1.target),
                loop = self.nooploop._loop
            ),
            frozenset()
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(gdxx1_1, debind1_1.packed),
            loop = self.nooploop._loop
        )
        
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.debind_status(gdxx1_1.target),
                loop = self.nooploop._loop
            ),
            frozenset({gdxx1_1.ghid})
        )
        
    def test_cache(self):
        ''' Test librarian.add_to_cache(obj, data),
        librarian.get_from_cache(ghid), and
        librarian.remove_from_cache(ghid).
        '''
        await_coroutine_threadsafe(
            coro = self.librarian.add_to_cache(geoc1_1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(geoc1_1.ghid),
                loop = self.nooploop._loop
            )
        )
        
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.get_from_cache(geoc1_1.ghid),
                loop = self.nooploop._loop
            ),
            cont1_1.packed
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.remove_from_cache(geoc1_1.ghid),
            loop = self.nooploop._loop
        )
        
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(geoc1_1.ghid),
                loop = self.nooploop._loop
            )
        )


class LibrarianCoreTest(GenericLibrarianTest, unittest.TestCase):
    ''' Test the core librarian-ness. Also, test the various (universal)
    librarian public_api bits (that don't get tested in the above).
    '''
        
    def setUp(self):
        # Do actually use the fixture, because we need to test the in-memory
        # version (plus otherwise, we simply cannot test LibrarianCore).
        self.librarian = LibrarianCore.__fixture__()
        
        self.enforcer = Enforcer.__fixture__(self.librarian)
        self.lawyer = LawyerCore.__fixture__(self.librarian)
        self.percore = PersistenceCore.__fixture__()
        
        # And assemble the librarian
        self.librarian.assemble(self.enforcer, self.lawyer, self.percore)
        
    def test_bound(self):
        ''' Test librarian.is_bound(obj).
        '''
        obj = _GeocLite(ghid=make_random_ghid(), author=make_random_ghid())
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.is_bound(obj),
                loop = self.nooploop._loop
            )
        )
        
        self.librarian._bound_by_ghid.add(obj.ghid, make_random_ghid())
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.is_bound(obj),
                loop = self.nooploop._loop
            )
        )
        
    def test_debound(self):
        ''' Test librarian.is_debound(obj).
        '''
        obj = _GeocLite(ghid=make_random_ghid(), author=make_random_ghid())
        binding = _GobsLite(
            ghid = make_random_ghid(),
            target = make_random_ghid(),
            author = obj.author
        )
        self.librarian._catalog[binding.ghid] = binding
        debinding = _GdxxLite(
            ghid = make_random_ghid(),
            author = binding.author,
            target = binding.ghid
        )
        
        # This doesn't need the debinding "uploaded"...
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.is_debound(binding),
                loop = self.nooploop._loop
            )
        )
        
        # ...but this does.
        self.librarian._debound_by_ghid.add(binding.ghid, debinding.ghid)
        self.librarian._catalog[debinding.ghid] = debinding
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.is_debound(binding),
                loop = self.nooploop._loop
            )
        )
        
        # Now let's do the same with an invalid debinding and make sure it gets
        # removed during the check
        self.librarian.RESET()
        self.librarian._catalog[binding.ghid] = binding
        illegal_author = _GdxxLite(
            ghid = make_random_ghid(),
            author = make_random_ghid(),
            target = binding.ghid
        )
        self.librarian._debound_by_ghid.add(binding.ghid, illegal_author.ghid)
        self.librarian._catalog[illegal_author.ghid] = illegal_author
        
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.is_debound(binding),
                loop = self.nooploop._loop
            )
        )
        # Make sure it was also removed.
        self.assertFalse(
            self.librarian._debound_by_ghid.contains_within(
                binding.ghid,
                illegal_author.ghid
            )
        )
        
        # Now let's do the same with an invalid target and make sure it gets
        # removed during the check
        self.librarian.RESET()
        self.librarian._catalog[obj.ghid] = obj
        illegal_target = _GdxxLite(
            ghid = make_random_ghid(),
            author = obj.author,
            target = obj.ghid
        )
        self.librarian._debound_by_ghid.add(obj.ghid, illegal_target.ghid)
        self.librarian._catalog[illegal_target.ghid] = illegal_target
        
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.is_debound(obj),
                loop = self.nooploop._loop
            )
        )
        # Make sure it was also removed.
        self.assertFalse(
            self.librarian._debound_by_ghid.contains_within(
                obj.ghid,
                illegal_target.ghid
            )
        )
        
    def test_store(self):
        ''' Test librarian.store(obj, data).
        '''
        await_coroutine_threadsafe(
            coro = self.librarian.store(geoc1_1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        self.assertIn(geoc1_1.ghid, self.librarian._shelf)
        self.assertIn(geoc1_1.ghid, self.librarian._catalog)
        
        await_coroutine_threadsafe(
            coro = self.librarian.store(gobd1_a, dyn1_1a.packed),
            loop = self.nooploop._loop
        )
        self.assertIn(gobd1_a.frame_ghid, self.librarian._shelf)
        self.assertIn(gobd1_a.frame_ghid, self.librarian._catalog)
        self.assertIn(gobd1_a.ghid, self.librarian._dyn_resolver)
        self.assertEqual(
            gobd1_a.frame_ghid,
            await_coroutine_threadsafe(
                coro = self.librarian.resolve_frame(gobd1_a.ghid),
                loop = self.nooploop._loop
            )
        )
        
        await_coroutine_threadsafe(
            coro = self.librarian.store(gobd1_b, dyn1_1b.packed),
            loop = self.nooploop._loop
        )
        self.assertIn(gobd1_b.frame_ghid, self.librarian._shelf)
        self.assertIn(gobd1_b.frame_ghid, self.librarian._catalog)
        self.assertNotIn(gobd1_a.frame_ghid, self.librarian._shelf)
        self.assertNotIn(gobd1_a.frame_ghid, self.librarian._catalog)
        self.assertIn(gobd1_b.ghid, self.librarian._dyn_resolver)
        self.assertEqual(
            gobd1_b.frame_ghid,
            await_coroutine_threadsafe(
                coro = self.librarian.resolve_frame(gobd1_b.ghid),
                loop = self.nooploop._loop
            )
        )
        
    def test_retrieve(self):
        ''' Test librarian.retrieve(ghid).
        '''
        self.librarian._shelf[cont1_1.ghid] = cont1_1.packed
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.retrieve(cont1_1.ghid),
                loop = self.nooploop._loop
            ),
            cont1_1.packed
        )
        
        # Note that as a GOLIX object, this ghid is the frame ghid.
        self.librarian._shelf[dyn1_1a.ghid] = dyn1_1a.packed
        self.librarian._dyn_resolver[dyn1_1a.ghid_dynamic] = dyn1_1a.ghid
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.retrieve(dyn1_1a.ghid_dynamic),
                loop = self.nooploop._loop
            ),
            dyn1_1a.packed
        )
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.librarian.retrieve(dyn1_1a.ghid),
                loop = self.nooploop._loop
            ),
            dyn1_1a.packed
        )
        
    def test_summarize(self):
        ''' Test librarian.summarize(ghid).
        '''
        obj = _GeocLite(ghid=make_random_ghid(), author=make_random_ghid())
        self.librarian._catalog[obj.ghid] = obj
        self.assertIs(
            await_coroutine_threadsafe(
                coro = self.librarian.summarize(obj.ghid),
                loop = self.nooploop._loop
            ),
            obj
        )
        
    def test_abandon(self):
        ''' Test librarian.abandon(ghid).
        '''
        self.librarian._shelf[cont1_1.ghid] = cont1_1.packed
        self.librarian._catalog[geoc1_1.ghid] = geoc1_1
        await_coroutine_threadsafe(
            coro = self.librarian.abandon(geoc1_1),
            loop = self.nooploop._loop
        )
        self.assertNotIn(geoc1_1.ghid, self.librarian._shelf)
        self.assertNotIn(geoc1_1.ghid, self.librarian._catalog)
        
        # Note that as a GOLIX object, this ghid is the frame ghid.
        self.librarian._shelf[dyn1_1a.ghid] = dyn1_1a.packed
        self.librarian._dyn_resolver[dyn1_1a.ghid_dynamic] = dyn1_1a.ghid
        self.librarian._catalog[dyn1_1a.ghid] = gobd1_a
        self.librarian._bound_by_ghid.add(gobd1_a.target, gobd1_a.ghid)
        await_coroutine_threadsafe(
            coro = self.librarian.abandon(gobd1_a),
            loop = self.nooploop._loop
        )
        self.assertNotIn(gobd1_a.frame_ghid, self.librarian._shelf)
        self.assertNotIn(gobd1_a.frame_ghid, self.librarian._catalog)
        self.assertNotIn(gobd1_a.ghid, self.librarian._dyn_resolver)
        self.assertFalse(
            self.librarian._bound_by_ghid.contains_within(
                gobd1_a.target,
                gobd1_a.ghid
            )
        )


if __name__ == "__main__":

    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
