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

from hypergolix.core import GolixCore
from hypergolix.core import Oracle
from hypergolix.core import GhidProxier
from hypergolix.core import _GAO

from hypergolix.privateer import Privateer
from hypergolix.persistence import MemoryLibrarian
from hypergolix.persistence import SalmonatorNoop
from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobdLite

# This is a semi-normal import
from golix.utils import _dummy_ghid

# These are fixture imports
from golix import Ghid


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2


class MockDispatch:
    ''' Test fixture for dispatch.
    
    Oops, didn't actually need to do this. I think?
    '''
    def __init__(self):
        self.incoming = []
        self.acks = []
        self.naks = []
        
    def assemble(self, *args, **kwargs):
        # Noop
        pass
        
    def bootstrap(self, *args, **kwargs):
        # Noop
        pass
        
    def dispatch_share(self, target):
        self.incoming.append(target)
        
    def dispatch_share_ack(self, target, recipient):
        self.acks.append((target, recipient))
        
    def dispatch_share_nak(self, target, recipient):
        self.naks.append((target, recipient))


# ###############################################
# Testing
# ###############################################


class GCoreTest(unittest.TestCase):
    def setUp(self):
        self.core = GolixCore()
        self.librarian = MemoryLibrarian()
        self.salmonator = SalmonatorNoop()
        
        self.core.assemble(self.librarian)
        self.librarian.assemble(self.core, self.salmonator)
        
        self.core.bootstrap(TEST_AGENT1)
        self.librarian.store(
            _GidcLite(TEST_AGENT1.ghid, TEST_AGENT1.second_party), 
            TEST_AGENT1.second_party.packed)
        self.librarian.store(
            _GidcLite(TEST_AGENT2.ghid, TEST_AGENT2.second_party), 
            TEST_AGENT2.second_party.packed)
        
    def test_trash(self):
        from _fixtures.remote_exchanges import handshake2_1
        from _fixtures.remote_exchanges import secret2_1
        from _fixtures.remote_exchanges import cont2_1

        # Test whoami
        self.assertEqual(self.core.whoami, TEST_AGENT1.ghid)
        # Test legroom
        self.assertGreaterEqual(self.core._legroom, 2)
        
        # Note that all of these are stateless, so they don't need to be actual
        # legitimate targets or anything, just actual GHIDs
        payload = TEST_AGENT1.make_handshake(
            target = cont2_1.ghid,
            secret = secret2_1)
        self.core.open_request(handshake2_1.packed)
        self.core.make_request(
            recipient = TEST_AGENT2.ghid, 
            payload = payload)
        self.core.open_container(
            container = cont2_1, 
            secret = secret2_1)
        self.core.make_container(
            data = b'Hello world',
            secret = TEST_AGENT1.new_secret())
        self.core.make_binding_stat(target=cont2_1.ghid)
        dynamic1 = self.core.make_binding_dyn(target=cont2_1.ghid)
        dynamic2 = self.core.make_binding_dyn(
            target = cont2_1.ghid, 
            ghid = dynamic1.ghid_dynamic,
            history = [dynamic1.ghid])
        self.core.make_debinding(target=dynamic2.ghid_dynamic)
        
        
class GhidproxyTest(unittest.TestCase):
    def setUp(self):
        self.ghidproxy = GhidProxier()
        self.librarian = MemoryLibrarian()
        # A proper fixture for Librarian would remove these two
        self.salmonator = SalmonatorNoop()
        self.gcore = GolixCore()
        
        self.ghidproxy.assemble(self.librarian)
        # A proper fixture for Librarian would also remove these three
        self.librarian.assemble(self.gcore, self.salmonator)
        self.gcore.assemble(self.librarian)
        self.gcore.bootstrap(TEST_AGENT1)
    
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
        self.librarian.store(geoc1_1, cont1_1.packed)
        self.librarian.store(geoc1_2, cont1_2.packed)
        
        self.librarian.store(gobd1_a, dyn1_1a.packed)
        self.assertEqual(
            self.ghidproxy.resolve(dyn1_1a.ghid_dynamic),
            cont1_1.ghid)
        
        self.librarian.store(gobd1_b, dyn1_1b.packed)
        # Intentionally reuse old dynamic ghid, even though it's the same
        self.assertEqual(
            self.ghidproxy.resolve(dyn1_1a.ghid_dynamic),
            cont1_2.ghid)
        
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
        self.librarian.store(geoc2_1, cont2_1.packed)
        self.librarian.store(geoc2_2, cont2_2.packed)
        
        self.librarian.store(gobd2_a, dyn2_1a.packed)
        self.assertEqual(
            self.ghidproxy.resolve(dyn2_1a.ghid_dynamic),
            cont2_1.ghid)
        
        self.librarian.store(gobd2_b, dyn2_1b.packed)
        # Intentionally reuse old dynamic ghid, even though it's the same
        self.assertEqual(
            self.ghidproxy.resolve(dyn2_1a.ghid_dynamic),
            cont2_2.ghid)
        
        
class OracleTest(unittest.TestCase):
    def test_trash(self):
        raise NotImplementedError()
        
        
class PrivateerTest(unittest.TestCase):
    def test_trash(self):
        raise NotImplementedError()
        

if __name__ == "__main__":
    from _fixtures import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()