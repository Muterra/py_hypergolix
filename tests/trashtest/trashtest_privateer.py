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

from hypergolix.privateer import Privateer
from hypergolix.exceptions import ConflictingSecrets
from hypergolix.exceptions import RatchetError
from hypergolix.exceptions import SecretUnknown

# These are fixture imports
from golix import Ghid
from golix.utils import Secret


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
        
        
class MockOracle:
    pass
        
        
class MockGolcore:
    def __init__(self):
        # This is just being used to call new_secret(), so no worries
        self._identity = TEST_AGENT1


# ###############################################
# Testing
# ###############################################
        
        
class PrivateerTest(unittest.TestCase):
    def setUp(self):
        self.golcore = MockGolcore()
        self.oracle = MockOracle()
        self.privateer = Privateer()
        
        self.privateer.assemble(self.golcore, self.oracle)
        self.privateer.bootstrap()
    
    def test_simple(self):
        ''' Beep boop, SweetBot1.0 reporting
        '''
        # Not much in the way of verification here, definitely room to improve
        secret = self.privateer.new_secret()
        self.assertTrue(isinstance(secret, Secret))
        
        # Okay some of this might be silly but if you got it might as well 
        # flaunt it, amirite?
        secret1 = self.privateer.new_secret()
        from _fixtures.remote_exchanges import cont1_1 as cont1
        secret2 = self.privateer.new_secret()
        from _fixtures.remote_exchanges import cont1_2 as cont2
        secret3 = self.privateer.new_secret()
        from _fixtures.remote_exchanges import cont2_1 as cont3
        secret4 = self.privateer.new_secret()
        from _fixtures.remote_exchanges import cont2_2 as cont4
        secret5 = self.privateer.new_secret()
        from _fixtures.remote_exchanges import cont3_1 as cont5
        secret6 = self.privateer.new_secret()
        from _fixtures.remote_exchanges import cont3_2 as cont6
        
        # First, stage a secret and verify its limited availability.
        self.privateer.stage(cont1.ghid, secret1)
        self.assertIn(cont1.ghid, self.privateer)
        self.assertIn(cont1.ghid, self.privateer._secrets_staging)
        self.assertNotIn(cont1.ghid, self.privateer._secrets_persistent)
        
        # Okay, now attempt to stage a competing secret.
        with self.assertRaises(ConflictingSecrets, 
                                msg = 'Allowed conflicting secret to stage.'):
            self.privateer.stage(cont1.ghid, secret2)
            
        # And attempt to re-stage the correct secret.
        self.privateer.stage(cont1.ghid, secret1)
        self.assertIn(cont1.ghid, self.privateer)
        self.assertIn(cont1.ghid, self.privateer._secrets_staging)
        self.assertNotIn(cont1.ghid, self.privateer._secrets_persistent)
            
        # Now actually commit the secret and verify it persistence
        self.privateer.commit(cont1.ghid)
        self.assertIn(cont1.ghid, self.privateer)
        self.assertNotIn(cont1.ghid, self.privateer._secrets_staging)
        self.assertIn(cont1.ghid, self.privateer._secrets_persistent)
        
        # Once again, attempt to stage a competing secret
        with self.assertRaises(ConflictingSecrets, 
                                msg = 'Allowed conflicting secret to stage.'):
            self.privateer.stage(cont1.ghid, secret2)
            
        # Again attempt to re-stage the correct secret. Make sure commit's both
        # indempotent AND doesn't result in garbage within the staging arena.
        self.privateer.stage(cont1.ghid, secret1)
        self.assertIn(cont1.ghid, self.privateer)
        self.assertNotIn(cont1.ghid, self.privateer._secrets_staging)
        self.assertIn(cont1.ghid, self.privateer._secrets_persistent)
            
        # Now test abandonment issues
        self.privateer.stage(cont2.ghid, secret2)
        self.assertIn(cont2.ghid, self.privateer)
        self.assertIn(cont2.ghid, self.privateer._secrets_staging)
        self.assertNotIn(cont2.ghid, self.privateer._secrets_persistent)
        self.privateer.abandon(cont2.ghid)
        self.assertNotIn(cont2.ghid, self.privateer)
        self.assertNotIn(cont2.ghid, self.privateer._secrets_staging)
        self.assertNotIn(cont2.ghid, self.privateer._secrets_persistent)
        
        # Now same with committed secrets
        self.privateer.stage(cont2.ghid, secret2)
        self.privateer.commit(cont2.ghid)
        self.assertIn(cont2.ghid, self.privateer)
        self.assertNotIn(cont2.ghid, self.privateer._secrets_staging)
        self.assertIn(cont2.ghid, self.privateer._secrets_persistent)
        self.privateer.abandon(cont2.ghid)
        self.assertNotIn(cont2.ghid, self.privateer)
        self.assertNotIn(cont2.ghid, self.privateer._secrets_staging)
        self.assertNotIn(cont2.ghid, self.privateer._secrets_persistent)
        
        # And now let's test abandoning an unknown secret, both quiet and loud
        self.privateer.abandon(cont3.ghid, quiet=True)
        with self.assertRaises(SecretUnknown, 
                                msg = 'Allowed abandoning unknown secret.'):
            self.privateer.abandon(cont3.ghid, quiet=False)
        
        # And committing an unknown secret...
        with self.assertRaises(SecretUnknown, 
                                msg = 'Allowed abandoning unknown secret.'):
            self.privateer.commit(cont3.ghid)
        
        # And getting an unknown secret...
        with self.assertRaises(SecretUnknown, 
                                msg = 'Allowed abandoning unknown secret.'):
            self.privateer.get(cont3.ghid)
        
        # Now stage a bad secret, unstage it, and restage a good one.
        self.privateer.stage(cont3.ghid, secret2)
        self.privateer.unstage(cont3.ghid)
        self.assertNotIn(cont3.ghid, self.privateer)
        self.assertNotIn(cont3.ghid, self.privateer._secrets_staging)
        self.assertNotIn(cont3.ghid, self.privateer._secrets_persistent)
        self.privateer.stage(cont3.ghid, secret3)
        self.assertIn(cont3.ghid, self.privateer)
        self.assertIn(cont3.ghid, self.privateer._secrets_staging)
        self.assertNotIn(cont3.ghid, self.privateer._secrets_persistent)
        
        # Also, unstaging something that doesn't exist.
        with self.assertRaises(SecretUnknown, 
                                msg = 'Allowed abandoning unknown secret.'):
            self.privateer.unstage(cont4.ghid)
            
        # And finally, make sure that getting secrets does, in fact, return
        # them correctly, both from staged and committed.
        self.assertEqual(self.privateer.get(cont1.ghid), secret1)
        self.assertEqual(self.privateer.get(cont3.ghid), secret3)
        
    @unittest.expectedFailure
    def test_chains_and_ratchets(self):
        ''' Mostly checking ratchets and healing and stuff for 
        self-consistency.
        '''
        # Okay some of this might be silly but if you got it might as well 
        # flaunt it, amirite?
        secret1 = self.privateer.new_secret()
        from _fixtures.remote_exchanges import cont1_1 as cont1
        from _fixtures.remote_exchanges import dyn1_1a as dyn1a
        from _fixtures.remote_exchanges import cont1_1 as cont2
        from _fixtures.remote_exchanges import dyn1_1b as dyn1b
        
        raise NotImplementedError()
        
        # Note: need better mock for oracle. That won't cut it for ratcheting!
        

if __name__ == "__main__":
    from _fixtures import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()