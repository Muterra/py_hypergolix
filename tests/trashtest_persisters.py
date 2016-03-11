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

# These are normal imports
from hypergolix.persisters import MemoryPersister
from hypergolix import NakError
from hypergolix import PersistenceWarning

# These are abnormal imports
from golix import Guid
from golix import ThirdParty
from golix import SecondParty
from golix import FirstParty

# ###############################################
# Testing
# ###############################################
    
class TrashTest(unittest.TestCase):
    def setUp(self):
        self.server1 = MemoryPersister()
    
        self.agent1 = FirstParty()
        self.agent2 = FirstParty()
        self.agent3 = FirstParty()
    
        self.reader1 = self.agent1.second_party
        self.reader2 = self.agent2.second_party
        
    def test_trash(self):
        # ---------------------------------------
        # Create and publish identity containers
        midc1 = self.reader1.packed
        midc2 = self.reader2.packed
        
        self.server1.publish(midc1)
        self.server1.publish(midc2)
        # Don't publish the third, we want to test refusal
        
        # ---------------------------------------
        # Prerequisites for testing -- should move to setUp?
        # Make some objects for known IDs
        pt1 = b'[[ Hello, world? ]]'
        pt2 = b'[[ Hiyaback! ]]'
        secret1_1 = self.agent1.new_secret()
        cont1_1 = self.agent1.make_container(
            secret = secret1_1,
            plaintext = pt1
        )
        secret1_2 = self.agent1.new_secret()
        cont1_2 = self.agent1.make_container(
            secret = secret1_2,
            plaintext = pt2
        )
        
        secret2_1 = self.agent2.new_secret()
        cont2_1 = self.agent2.make_container(
            secret = secret2_1,
            plaintext = pt1
        )
        secret2_2 = self.agent2.new_secret()
        cont2_2 = self.agent2.make_container(
            secret = secret2_2,
            plaintext = pt2
        )
        
        # Make some objects for an unknown ID
        secret3_1 = self.agent3.new_secret()
        cont3_1 = self.agent3.make_container(
            secret = secret3_1,
            plaintext = pt1
        )
        secret3_2 = self.agent3.new_secret()
        cont3_2 = self.agent3.make_container(
            secret = secret3_2,
            plaintext = pt2
        )
        
        # Make some bindings for known IDs
        bind1_1 = self.agent1.make_bind_static(
            target = cont1_1.guid
        )
        bind1_2 = self.agent1.make_bind_static(
            target = cont1_2.guid
        )
        
        bind2_1 = self.agent2.make_bind_static(
            target = cont2_1.guid
        )
        bind2_2 = self.agent2.make_bind_static(
            target = cont2_2.guid
        )
        
        # Make some bindings for the unknown ID
        bind3_1 = self.agent3.make_bind_static(
            target = cont3_1.guid
        )
        bind3_2 = self.agent3.make_bind_static(
            target = cont3_2.guid
        )
        
        # Make some debindings 
        debind2_1 = self.agent2.make_debind(
            target = bind2_1.guid
        )
        debind2_2 = self.agent2.make_debind(
            target = bind2_2.guid
        )
        
        # And make some author-inconsistent debindings
        debind2_1_bad = self.agent1.make_debind(
            target = bind2_1.guid
        )
        debind2_2_bad = self.agent1.make_debind(
            target = bind2_2.guid
        )
        
        # And then make some debindings for the debindings
        dedebind2_1 = self.agent2.make_debind(
            target = debind2_1.guid
        )
        dedebind2_2 = self.agent2.make_debind(
            target = debind2_2.guid
        )
        
        # ---------------------------------------
        # Publish bindings and then containers
        self.server1.publish(bind1_1.packed)
        self.server1.publish(bind1_2.packed)
        self.server1.publish(bind2_1.packed)
        self.server1.publish(bind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown binder.'):
            self.server1.publish(bind3_1.packed)
            self.server1.publish(bind3_2.packed)
            
        self.server1.publish(cont1_1.packed)
        self.server1.publish(cont1_2.packed)
        self.server1.publish(cont2_1.packed)
        self.server1.publish(cont2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown author.'):
            self.server1.publish(cont3_1.packed)
            self.server1.publish(cont3_2.packed)
        
        # ---------------------------------------
        # Publish impersonation debindings for the second identity
        with self.assertRaises(NakError, msg='Server allowed wrong author debind.'):
            self.server1.publish(debind2_1_bad.packed)
            self.server1.publish(debind2_2_bad.packed)
        
        # ---------------------------------------
        # Publish debindings for the second identity
        self.server1.publish(debind2_1.packed)
        self.server1.publish(debind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed binding replay.'):
            self.server1.publish(bind2_1.packed)
            self.server1.publish(bind2_2.packed)
            
        self.assertIn(debind2_1.guid, self.server1._store)
        self.assertNotIn(bind2_1.guid, self.server1._store)
        self.assertNotIn(cont2_1.guid, self.server1._store)
        
        # ---------------------------------------
        # Publish debindings for those debindings and then rebind them
        self.server1.publish(dedebind2_1.packed)
        self.server1.publish(dedebind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed debinding replay.'):
            self.server1.publish(debind2_1.packed)
            self.server1.publish(debind2_2.packed)
            
        self.assertNotIn(debind2_1.guid, self.server1._targets_debind)
        self.assertNotIn(debind2_2.guid, self.server1._targets_debind)
        self.assertNotIn(bind2_1.guid, self.server1._debindings)
        self.assertNotIn(bind2_2.guid, self.server1._debindings)
            
        self.assertIn(dedebind2_1.guid, self.server1._store)
        self.assertNotIn(debind2_1.guid, self.server1._store)
        self.assertNotIn(bind2_1.guid, self.server1._store)
        self.assertNotIn(cont2_1.guid, self.server1._store)
        
        self.server1.publish(bind2_1.packed)
        self.server1.publish(bind2_2.packed)
        self.server1.publish(cont2_1.packed)
        self.server1.publish(cont2_2.packed)
        
        self.assertIn(bind2_1.guid, self.server1._store)
        self.assertIn(cont2_1.guid, self.server1._store)
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # Start an interactive IPython interpreter with local namespace, but
        # suppress all IPython-related warnings.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            IPython.embed()

if __name__ == "__main__":
    unittest.main()