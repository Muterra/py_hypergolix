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

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Doorman
from hypergolix.persistence import Lawyer
from hypergolix.persistence import Enforcer
from hypergolix.persistence import Bookie
from hypergolix.persistence import MemoryLibrarian
from hypergolix.persistence import MrPostman
from hypergolix.persistence import Undertaker
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
        
        
class MockRolodex:
    def request_handler(self, subs_ghid, notify_ghid):
        # Noop
        pass


# ###############################################
# Testing
# ###############################################


class GCoreTest(unittest.TestCase):
    def setUp(self):
        self.gcore = GolixCore()
        self.librarian = MemoryLibrarian()
        self.salmonator = SalmonatorNoop()
        
        self.gcore.assemble(self.librarian)
        self.librarian.assemble(self.gcore, self.salmonator)
        
        self.gcore.bootstrap(TEST_AGENT1)
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
        self.assertEqual(self.gcore.whoami, TEST_AGENT1.ghid)
        # Test legroom
        self.assertGreaterEqual(self.gcore._legroom, 2)
        
        # Note that all of these are stateless, so they don't need to be actual
        # legitimate targets or anything, just actual GHIDs
        payload = TEST_AGENT1.make_handshake(
            target = cont2_1.ghid,
            secret = secret2_1)
        self.gcore.open_request(handshake2_1.packed)
        self.gcore.make_request(
            recipient = TEST_AGENT2.ghid, 
            payload = payload)
        self.gcore.open_container(
            container = cont2_1, 
            secret = secret2_1)
        self.gcore.make_container(
            data = b'Hello world',
            secret = TEST_AGENT1.new_secret())
        self.gcore.make_binding_stat(target=cont2_1.ghid)
        dynamic1 = self.gcore.make_binding_dyn(target=cont2_1.ghid)
        dynamic2 = self.gcore.make_binding_dyn(
            target = cont2_1.ghid, 
            ghid = dynamic1.ghid_dynamic,
            history = [dynamic1.ghid])
        self.gcore.make_debinding(target=dynamic2.ghid_dynamic)
        
        
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
        
        
class GAOTest(unittest.TestCase):
    def setUp(self):
        # These are directly required by the GAO
        self.golcore = GolixCore()
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer()
        self.percore = PersistenceCore()
        self.bookie = Bookie()
        self.librarian = MemoryLibrarian()
        
        # These are here, for lack of fixturing of the above.
        self.oracle = Oracle()
        self.doorman = Doorman()
        self.enforcer = Enforcer()
        self.lawyer = Lawyer()
        self.postman = MrPostman()
        self.undertaker = Undertaker()
        self.salmonator = SalmonatorNoop()
        self.rolodex = MockRolodex()
        
        # These are a mix of "necessary" and "unnecessary if well-fixtured"
        self.golcore.assemble(self.librarian)
        self.ghidproxy.assemble(self.librarian)
        self.oracle.assemble(self.golcore, self.ghidproxy, self.privateer, 
                            self.percore, self.bookie, self.librarian, 
                            self.postman, self.salmonator)
        self.privateer.assemble(self.golcore, self.oracle)
        self.percore.assemble(self.doorman, self.enforcer, self.lawyer, 
                            self.bookie, self.librarian, self.postman, 
                            self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.lawyer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore, self.salmonator)
        self.postman.assemble(self.golcore, self.librarian, self.bookie,
                            self.rolodex)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        
        # These are both "who-knows-if-necessary-when-fixtured"
        self.golcore.bootstrap(TEST_AGENT1)
        self.privateer.bootstrap()
        self.percore.ingest(TEST_AGENT1.second_party.packed)
        
    def test_source(self):
        ''' These tests are alone-ish and can definitely be wholly 
        fixtured
        '''
        # Test a static object being created
        obj_static = _GAO(self.golcore, self.ghidproxy, self.privateer, 
                        self.percore, self.bookie, self.librarian, False)
        msg1 = b'hello stagnant world'
        obj_static.apply_state(msg1)
        obj_static.push()
        self.assertTrue(obj_static.ghid)
        self.assertEqual(obj_static.extract_state(), msg1)
        self.assertIn(obj_static.ghid, self.privateer)
        self.assertIn(obj_static.ghid, self.librarian)
        
        # Now test a dynamic object being created
        obj_dyn = _GAO(self.golcore, self.ghidproxy, self.privateer, 
                        self.percore, self.bookie, self.librarian, True)
        msg2 = b'hello mutable world'
        obj_dyn.apply_state(msg2)
        obj_dyn.push()
        self.assertTrue(obj_dyn.ghid)
        # Speaking of inadequate fixturing...
        self.oracle._lookup[obj_dyn.ghid] = obj_dyn
        # Back to business as usual now.
        self.assertEqual(obj_dyn.extract_state(), msg2)
        # We should NOT see the dynamic ghid in privateer.
        self.assertNotIn(obj_dyn.ghid, self.privateer)
        # But we should see the most recent target
        self.assertIn(obj_dyn._history_targets[0], self.privateer)
        self.assertIn(obj_dyn.ghid, self.librarian)
        
        # And let's test mutation thereof. 0000001000000110000011100001111
        msg3 = b'AFFIRMATIVE'
        obj_dyn.apply_state(msg3)
        obj_dyn.push()
        self.assertTrue(obj_dyn.ghid)
        self.assertEqual(obj_dyn.extract_state(), msg3)
        # We should NOT see the dynamic ghid in privateer.
        self.assertNotIn(obj_dyn.ghid, self.privateer)
        # But we should see the most recent target
        self.assertIn(obj_dyn._history_targets[0], self.privateer)
        self.assertIn(obj_dyn.ghid, self.librarian)
        
        # Now let's try freezing it
        frozen_ghid = obj_dyn.freeze()
        self.assertEqual(frozen_ghid, obj_dyn._history_targets[0])
        
        # And then delete the original
        obj_dyn.delete()
        # TODO: assess that this has actually deleted anything...
        
    def test_sink(self):
        ''' Test retrieval from known ghids, pulls, etc
        '''
        # "Remote" frame 1
        from _fixtures.remote_exchanges import pt1
        from _fixtures.remote_exchanges import secret1_1
        from _fixtures.remote_exchanges import cont1_1
        from _fixtures.remote_exchanges import dyn1_1a
        
        # "Remote" frame 2
        from _fixtures.remote_exchanges import pt2
        from _fixtures.remote_exchanges import secret1_2
        from _fixtures.remote_exchanges import cont1_2
        from _fixtures.remote_exchanges import dyn1_1b
        
        self.percore.ingest(dyn1_1a.packed)
        self.percore.ingest(cont1_1.packed)
        self.privateer.stage(cont1_1.ghid, secret1_1)
        
        obj_dyn = _GAO.from_ghid(dyn1_1a.ghid_dynamic, self.golcore, 
            self.ghidproxy, self.privateer, self.percore, self.bookie,
            self.librarian)
        self.assertEqual(obj_dyn.extract_state(), pt1)
        
        # NOTE! These are not ratcheted.
        self.percore.ingest(dyn1_1b.packed)
        self.percore.ingest(cont1_2.packed)
        self.privateer.stage(cont1_2.ghid, secret1_2)
        
        obj_dyn.pull()
        self.assertEqual(obj_dyn.extract_state(), pt2)
        
class OracleTest(unittest.TestCase):
    def __setUp(self):
        self.golcore = GolixCore()
        self.salmonator = SalmonatorNoop()
        self.librarian = MemoryLibrarian()
        self.oracle = Oracle()
        
        self.golcore.assemble(self.librarian)
        self.librarian.assemble(self.golcore, self.salmonator)
        self.oracle.assemble(self.golcore, self.ghidproxy, self.privateer, 
                            self.percore, self.bookie, self.librarian, 
                            self.postman, self.salmonator)
        
        self.golcore.bootstrap(TEST_AGENT1)
    
    def test_trash(self):
        raise NotImplementedError()
        
        # First let's try simple retrieval
        from _fixtures.remote_exchanges import pt1
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
        
        self.librarian.store(geoc1_1, cont1_1.packed)
        self.librarian.store(gobd1_a, dyn1_1a.packed)
        
        # Okay, now we should be able to get that.
        obj = self.oracle.get_obect(_GAO, dyn1_1a.ghid_dynamic)
        self.assertEqual(obj.extract_state(), pt1)
        
        # self.librarian.store(geoc1_2, cont1_2.packed)
        # self.librarian.store(gobd1_b, dyn1_1b.packed)
        
        
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