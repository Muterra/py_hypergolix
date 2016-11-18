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

from loopa import TaskLooper
from loopa.utils import await_coroutine_threadsafe

from hypergolix.gao import GAO

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Bookie
from hypergolix.core import GolixCore
from hypergolix.core import GhidProxier
from hypergolix.privateer import Privateer
from hypergolix.librarian import LibrarianCore


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid
from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2


# ###############################################
# Testing
# ###############################################

        
@unittest.skip('Unfinished.')
class GAOTest(unittest.TestCase):
    def setUp(self):
        # These are directly required by the GAO
        self.golcore = GolixCore.__fixture__(TEST_AGENT1)
        self.ghidproxy = GhidProxier.__fixture__()
        self.privateer = Privateer.__fixture__()
        self.percore = PersistenceCore.__fixture__()
        self.bookie = Bookie.__fixture__()
        self.librarian = LibrarianCore.__fixture__()
        
        # These are a mix of "necessary" and "unnecessary if well-fixtured"
        self.golcore.assemble(self.librarian)
        self.ghidproxy.assemble(self.librarian, self.salmonator)
        self.oracle.assemble(self.golcore, self.ghidproxy, self.privateer, 
                            self.percore, self.bookie, self.librarian, 
                            self.postman, self.salmonator)
        self.privateer.assemble(self.golcore, self.ghidproxy, self.oracle)
        self.percore.assemble(self.doorman, self.enforcer, self.lawyer, 
                            self.bookie, self.librarian, self.postman, 
                            self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.lawyer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore)
        self.postman.assemble(self.golcore, self.librarian, self.bookie,
                            self.rolodex)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        
        # These are both "who-knows-if-necessary-when-fixtured"
        credential = MockCredential(self.privateer, TEST_AGENT1)
        self.golcore.bootstrap(credential)
        self.privateer.prep_bootstrap()
        # Just do this manually.
        self.privateer._credential = credential
        # self.privateer.bootstrap(
        #     persistent_secrets = {}, 
        #     staged_secrets = {},
        #     chains = {}
        # )
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
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
