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
from hypergolix.core import GhidProxier

from hypergolix.privateer import Privateer
from hypergolix.exceptions import ConflictingSecrets
from hypergolix.exceptions import RatchetError
from hypergolix.exceptions import UnknownSecret

# These are fixture imports
from golix import Ghid
from golix.utils import Secret


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
from _fixtures.ghidutils import make_random_ghid


# ###############################################
# Testing
# ###############################################
        
        
class PrivateerTest(unittest.TestCase):
    def setUp(self):
        # Fixtures are helpful
        self.golcore = GolixCore.__fixture__(TEST_AGENT1)
        self.ghidproxy = GhidProxier.__fixture__()
        
        # This is obviously necessary for testing.
        self.privateer = Privateer()
        self.privateer.assemble(self.golcore)
        self.privateer.prep_bootstrap()
        # Note that we don't actually need to bootstrap, because prep_bootstrap
        # creates a fully-functional privateer (just without using GAOs)
        
    def test_new_secret(self):
        ''' Test creating a new secret.
        '''
        # Not much in the way of verification here. Room to improve?
        secret = self.privateer.new_secret()
        self.assertTrue(isinstance(secret, Secret))
    
    def test_stage(self):
        ''' Test staging operations.
        '''
        ghid1 = make_random_ghid()
        secret1 = self.privateer.new_secret()
        
        ghid2 = make_random_ghid()
        secret2 = self.privateer.new_secret()
        
        ghid3 = make_random_ghid()
        secret3 = self.privateer.new_secret()
        
        ghid4 = make_random_ghid()
        secret4 = self.privateer.new_secret()
        
        ghid5 = make_random_ghid()
        secret5 = self.privateer.new_secret()
        
        ghid6 = make_random_ghid()
        secret6 = self.privateer.new_secret()
        
        # First, stage a secret and verify its limited availability.
        self.privateer.stage(ghid1, secret1)
        self.assertIn(ghid1, self.privateer)
        self.assertIn(ghid1, self.privateer._secrets_staging)
        self.assertNotIn(ghid1, self.privateer._secrets_persistent)
            
        # And attempt to re-stage the same secret (should be idempotent).
        self.privateer.stage(ghid1, secret1)
        self.assertIn(ghid1, self.privateer)
        self.assertIn(ghid1, self.privateer._secrets_staging)
        self.assertNotIn(ghid1, self.privateer._secrets_persistent)
        
        # Okay, now attempt to stage a competing secret.
        with self.assertRaises(ConflictingSecrets,
                               msg = 'Allowed conflicting secret to stage.'):
            self.privateer.stage(ghid1, secret2)
        
        # Now stage a bad secret, unstage it, and restage a good one.
        self.privateer.stage(ghid3, secret2)
        self.privateer.unstage(ghid3)
        self.assertNotIn(ghid3, self.privateer)
        self.assertNotIn(ghid3, self.privateer._secrets_staging)
        self.assertNotIn(ghid3, self.privateer._secrets_persistent)
        self.privateer.stage(ghid3, secret3)
        self.assertIn(ghid3, self.privateer)
        self.assertIn(ghid3, self.privateer._secrets_staging)
        self.assertNotIn(ghid3, self.privateer._secrets_persistent)
        
        # Also, unstaging something that doesn't exist.
        with self.assertRaises(UnknownSecret,
                               msg = 'Allowed abandoning unknown secret.'):
            self.privateer.unstage(ghid4)
            
            
            
        # And finally, make sure that getting secrets does, in fact, return
        # them correctly, both from staged and committed.
        self.assertEqual(self.privateer.get(ghid1), secret1)
            
    def test_commit(self):
        ghid1 = make_random_ghid()
        secret1 = self.privateer.new_secret()
        
        ghid2 = make_random_ghid()
        secret2 = self.privateer.new_secret()
        
        ghid3 = make_random_ghid()
        secret3 = self.privateer.new_secret()
        
        ghid4 = make_random_ghid()
        secret4 = self.privateer.new_secret()
        
        ghid5 = make_random_ghid()
        secret5 = self.privateer.new_secret()
        
        ghid6 = make_random_ghid()
        secret6 = self.privateer.new_secret()
        
        # Committing without staging should fail.
        with self.assertRaises(UnknownSecret,
                               msg = 'Allowed committing unknown secret.'):
            self.privateer.commit(ghid3)
            
        # Now actually commit the secret and verify it persistence
        self.privateer.stage(ghid1, secret1)
        self.privateer.commit(ghid1)
        self.assertIn(ghid1, self.privateer)
        self.assertNotIn(ghid1, self.privateer._secrets_staging)
        self.assertIn(ghid1, self.privateer._secrets_persistent)
        
        # Once again, attempt to stage a competing secret
        with self.assertRaises(ConflictingSecrets,
                               msg = 'Allowed conflicting secret to stage.'):
            self.privateer.stage(ghid1, secret2)
            
        # Again attempt to re-stage the correct secret. Make sure commit's both
        # indempotent AND doesn't result in garbage within the staging arena.
        self.privateer.stage(ghid1, secret1)
        self.assertIn(ghid1, self.privateer)
        self.assertNotIn(ghid1, self.privateer._secrets_staging)
        self.assertIn(ghid1, self.privateer._secrets_persistent)
        
        # Okay, now test committing local-only
        self.privateer.stage(ghid4, secret4)
        self.privateer.commit(ghid4, localize=True)
        self.assertIn(ghid4, self.privateer)
        self.assertNotIn(ghid4, self.privateer._secrets_staging)
        self.assertNotIn(ghid4, self.privateer._secrets_persistent)
        self.assertIn(ghid4, self.privateer._secrets_local)
        
        # And finally, make sure that getting secrets does, in fact, return
        # the correct secret
        self.assertEqual(self.privateer.get(ghid1), secret1)
        self.assertEqual(self.privateer.get(ghid4), secret4)
    
    def test_etc(self):
        ''' Some extra get tests, abandonment tests, etc.
        '''
        ghid1 = make_random_ghid()
        secret1 = self.privateer.new_secret()
        
        ghid2 = make_random_ghid()
        secret2 = self.privateer.new_secret()
        
        ghid3 = make_random_ghid()
        secret3 = self.privateer.new_secret()
        
        ghid4 = make_random_ghid()
        secret4 = self.privateer.new_secret()
        
        ghid5 = make_random_ghid()
        secret5 = self.privateer.new_secret()
        
        ghid6 = make_random_ghid()
        secret6 = self.privateer.new_secret()
        
        # And getting an unknown secret...
        with self.assertRaises(UnknownSecret,
                               msg = 'Allowed getting unknown secret.'):
            self.privateer.get(ghid3)
            
        # Now test abandonment issues
        self.privateer.stage(ghid2, secret2)
        self.assertIn(ghid2, self.privateer)
        self.assertIn(ghid2, self.privateer._secrets_staging)
        self.assertNotIn(ghid2, self.privateer._secrets_persistent)
        self.privateer.abandon(ghid2)
        self.assertNotIn(ghid2, self.privateer)
        self.assertNotIn(ghid2, self.privateer._secrets_staging)
        self.assertNotIn(ghid2, self.privateer._secrets_persistent)
        
        # Now same with committed secrets
        self.privateer.stage(ghid2, secret2)
        self.privateer.commit(ghid2)
        self.assertIn(ghid2, self.privateer)
        self.assertNotIn(ghid2, self.privateer._secrets_staging)
        self.assertIn(ghid2, self.privateer._secrets_persistent)
        self.privateer.abandon(ghid2)
        self.assertNotIn(ghid2, self.privateer)
        self.assertNotIn(ghid2, self.privateer._secrets_staging)
        self.assertNotIn(ghid2, self.privateer._secrets_persistent)
        
        # Now same with locally-committed secrets
        self.privateer.stage(ghid1, secret1)
        self.privateer.commit(ghid1, localize=True)
        self.assertIn(ghid1, self.privateer)
        self.assertNotIn(ghid1, self.privateer._secrets_staging)
        self.assertIn(ghid1, self.privateer._secrets_local)
        self.privateer.abandon(ghid1)
        self.assertNotIn(ghid1, self.privateer)
        self.assertNotIn(ghid1, self.privateer._secrets_staging)
        self.assertNotIn(ghid1, self.privateer._secrets_local)
        
        # And now let's test abandoning an unknown secret, both quiet and loud
        self.privateer.abandon(ghid3, quiet=True)
        with self.assertRaises(UnknownSecret,
                               msg = 'Allowed abandoning unknown secret.'):
            self.privateer.abandon(ghid3, quiet=False)
        
    def test_standard_ratchet(self):
        ''' Test standard ratchets and healing.
        
        TODO: create vectors for this.
        '''
        # Test ratcheting
        proxy = make_random_ghid()
        ghid1 = make_random_ghid()
        ghid2 = make_random_ghid()
        ghid3 = make_random_ghid()
        secret1 = self.privateer.new_secret()
        
        self.privateer.stage(ghid1, secret1)
        secret2 = self.privateer.ratchet_chain(proxy, ghid1)
        self.privateer.stage(ghid2, secret2)
        secret3 = self.privateer.ratchet_chain(proxy, ghid2)
        
        # Test normal healing therefrom
        self.privateer.abandon(ghid2)
        
        history = [ghid3, ghid2, ghid1]
        self.privateer.heal_chain(proxy, history)
        self.assertIn(ghid2, self.privateer)
        self.assertIn(ghid3, self.privateer)
        self.assertEqual(self.privateer.get(ghid3), secret3)
        
    def test_mastered_ratchet(self):
        ''' Test mastered ratchets and healing.
        
        TODO: create vectors for this.
        '''
        # Test ratcheting
        master = self.privateer.new_secret()
        proxy = make_random_ghid()
        ghid1 = make_random_ghid()
        ghid2 = make_random_ghid()
        ghid3 = make_random_ghid()
        secret1 = self.privateer.new_secret()
        
        self.privateer.stage(ghid1, secret1)
        secret2 = self.privateer.ratchet_chain(proxy, ghid1, master)
        self.privateer.stage(ghid2, secret2)
        secret3 = self.privateer.ratchet_chain(proxy, ghid2, master)
        
        # Test normal healing therefrom
        self.privateer.abandon(ghid2)
        
        history = [ghid3, ghid2, ghid1]
        self.privateer.heal_chain(proxy, history, master)
        self.assertIn(ghid2, self.privateer)
        self.assertIn(ghid3, self.privateer)
        self.assertEqual(self.privateer.get(ghid3), secret3)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
