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

# Identities
from _fixtures.remote_exchanges import gidc1
from _fixtures.remote_exchanges import gidc2
gidclite1 = _GidcLite.from_golix(GIDC.unpack(gidc1))
gidclite2 = _GidcLite.from_golix(GIDC.unpack(gidc2))
# Containers
from _fixtures.remote_exchanges import cont1_1  # Known author
from _fixtures.remote_exchanges import cont3_1  # Unknown author
# Static bindings
from _fixtures.remote_exchanges import bind1_1  # Known author
from _fixtures.remote_exchanges import bind3_2  # Unknown author
# Dynamic bindings
from _fixtures.remote_exchanges import dyn1_1a  # Known author frame 1
from _fixtures.remote_exchanges import dyn1_1b  # Known author frame 2
from _fixtures.remote_exchanges import dyn3_1a  # Unknown author frame 1
from _fixtures.remote_exchanges import dyn3_1b  # Unknown author frame 2
from _fixtures.remote_exchanges import dynF_a   # Inconsistent author frame 1
from _fixtures.remote_exchanges import dynF_b   # Inconsistent author frame 2
# Debindings
from _fixtures.remote_exchanges import debind1_1        # Consistent author
from _fixtures.remote_exchanges import debind2_1_bad    # Inconsistent author
from _fixtures.remote_exchanges import debind3_1        # Unknown author
# Requests
from _fixtures.remote_exchanges import handshake1_1     # Known recipient
from _fixtures.remote_exchanges import handshake3_1     # Unknown recipient


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
        
    def test_retrieve(self):
        ''' Test librarian.retrieve(ghid).
        '''
        
    def test_summarize(self):
        ''' Test librarian.summarize(ghid).
        '''
        
    def test_abandon(self):
        ''' Test librarian.abandon(ghid).
        '''


if __name__ == "__main__":

    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
