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
# Containers
from _fixtures.remote_exchanges import cont1_1  # Known author
from _fixtures.remote_exchanges import cont3_1  # Unknown author
# Static bindings
from _fixtures.remote_exchanges import bind1_1  # Known author
from _fixtures.remote_exchanges import bind3_1  # Unknown author
# Dynamic bindings
from _fixtures.remote_exchanges import dyn1_1a  # Known author frame 1
from _fixtures.remote_exchanges import dyn1_1b  # Known author frame 2
from _fixtures.remote_exchanges import dyn3_1a  # Unknown author frame 1
from _fixtures.remote_exchanges import dyn3_1b  # Unknown author frame 2
from _fixtures.remote_exchanges import dynF_a   # Inconsistent author frame 1
from _fixtures.remote_exchanges import dynF_b   # Inconsistent author frame 2
from _fixtures.remote_exchanges import dynF_c   # Inconsistent, unk author frm2
# Debindings
from _fixtures.remote_exchanges import debind1_1        # Consistent author
from _fixtures.remote_exchanges import debind1_F        # Inconsistent author
from _fixtures.remote_exchanges import debind3_1        # Unknown author
from _fixtures.remote_exchanges import debindR_1
from _fixtures.remote_exchanges import debindR_F
from _fixtures.remote_exchanges import debind3_TF
# Requests
from _fixtures.remote_exchanges import handshake1_1     # Known recipient
from _fixtures.remote_exchanges import handshake3_1     # Unknown recipient


# Identities
gidclite1 = _GidcLite.from_golix(GIDC.unpack(gidc1))
gidclite2 = _GidcLite.from_golix(GIDC.unpack(gidc2))

# Containers
obj1 = _GeocLite.from_golix(cont1_1)
obj3 = _GeocLite.from_golix(cont3_1)

# Static bindings
sbind1 = _GobsLite.from_golix(bind1_1)
sbind3 = _GobsLite.from_golix(bind3_1)

# Dynamic bindings
dbind1a = _GobdLite.from_golix(dyn1_1a)
dbind1b = _GobdLite.from_golix(dyn1_1b)

dbind3a = _GobdLite.from_golix(dyn3_1a)
dbind3b = _GobdLite.from_golix(dyn3_1b)

dbindFa = _GobdLite.from_golix(dynF_a)
dbindFb = _GobdLite.from_golix(dynF_b)
dbindFc = _GobdLite.from_golix(dynF_c)

# Debindings
xbind1 = _GdxxLite.from_golix(debind1_1)
xbind3 = _GdxxLite.from_golix(debind3_1)
xbind1R = _GdxxLite.from_golix(debindR_1)
xbindF1 = _GdxxLite.from_golix(debind1_F)
xbindFR = _GdxxLite.from_golix(debindR_F)
xbind3TF = _GdxxLite.from_golix(debind3_TF)

# Requests
req1 = _GarqLite.from_golix(handshake1_1)
req3 = _GarqLite.from_golix(handshake3_1)


# ###############################################
# Testing
# ###############################################


class GenericLawyerTest:
    ''' Test any kind of lawyer by subclassing this and defining a
    setUp method that includes a self.librarian.
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
        
    def test_gidc(self):
        ''' Test gidc operations.
        '''
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gidc(gidclite1),
                loop = self.nooploop._loop
            )
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gidc(gidclite2),
                loop = self.nooploop._loop
            )
        )
        
    def test_geoc(self):
        ''' Test geoc operations.
        '''
        # Load valid authors
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        
        # With a known author
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_geoc(obj1),
                loop = self.nooploop._loop
            )
        )
        # With an unknown author
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_geoc(obj3),
                loop = self.nooploop._loop
            )
        
    def test_gobs(self):
        ''' Test gobs operations.
        '''
        # Load valid authors
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        
        # With a known author
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gobs(sbind1),
                loop = self.nooploop._loop
            )
        )
        # With an unknown author
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_geoc(sbind3),
                loop = self.nooploop._loop
            )
        
    def test_gobd(self):
        ''' Test gobd operations.
        '''
        # Load valid authors
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        
        # FIRST FRAME ########################################################
        # With a known author
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gobd(dbind1a),
                loop = self.nooploop._loop
            )
        )
        # With an unknown author
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gobd(dbind3a),
                loop = self.nooploop._loop
            )
        
        # SECOND FRAME #######################################################
        # Prep by loading first frame...
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind3a, dyn3_1a),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbindFa, dynF_a),
            loop = self.nooploop._loop
        )
        
        # Test with a known author and a valid progression
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gobd(dbind1b),
                loop = self.nooploop._loop
            )
        )
        # Test with an unknown author and a valid progression
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gobd(dbind3b),
                loop = self.nooploop._loop
            )
        
        # Test with a known author and an invalid progression
        with self.assertRaises(InconsistentAuthor):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gobd(dbindFb),
                loop = self.nooploop._loop
            )
        # Test with an unknown author and an invalid progression
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gobd(dbindFc),
                loop = self.nooploop._loop
            )
        
    def test_gdxx(self):
        ''' Test gdxx operations.
        '''
        # Load valid authors
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        
        # Load debinding targets
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind3, bind3_1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(req1, handshake1_1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj3, cont3_1),
            loop = self.nooploop._loop
        )
        
        # With a known, consistent author and a valid target
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gdxx(xbind1),
                loop = self.nooploop._loop
            )
        )
        # With a known, consistent author and a valid (request) target
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gdxx(xbind1R),
                loop = self.nooploop._loop
            )
        )
        # With an unknown, consistent author and a valid target
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gdxx(xbind3),
                loop = self.nooploop._loop
            )
        # With an unknown, consistent author and an invalid target
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gdxx(xbind3TF),
                loop = self.nooploop._loop
            )
        # With a known, inconsistent author and a valid target
        with self.assertRaises(InconsistentAuthor):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gdxx(xbindF1),
                loop = self.nooploop._loop
            )
        # With a known, inconsistent author and a valid (request) target
        with self.assertRaises(InconsistentAuthor):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_gdxx(xbindFR),
                loop = self.nooploop._loop
            )
        
    def test_garq(self):
        ''' Test garq operations.
        '''
        # Load valid authors
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        
        # With a known author
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_garq(req1),
                loop = self.nooploop._loop
            )
        )
        # With an unknown author
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.lawyer.validate_garq(req3),
                loop = self.nooploop._loop
            )


class LawyerCoreTest(GenericLawyerTest, unittest.TestCase):
    ''' Test the core lawyer-ness.
    '''
        
    def setUp(self):
        self.librarian = LibrarianCore.__fixture__()
        
        self.lawyer = LawyerCore()
        self.lawyer.assemble(self.librarian)


if __name__ == "__main__":

    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
