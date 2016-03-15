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
        self.reader3 = self.agent3.second_party
        
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
        debind1_1 = self.agent1.make_debind(
            target = bind1_1.guid
        )
        debind1_2 = self.agent1.make_debind(
            target = bind1_2.guid
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
        
        # And then make some debindings for the debindings for the...
        dededebind2_1 = self.agent2.make_debind(
            target = dedebind2_1.guid
        )
        dededebind2_2 = self.agent2.make_debind(
            target = dedebind2_2.guid
        )
        
        # Make requests between known IDs
        handshake1_1 = self.agent1.make_request(
            recipient = self.reader2,
            request = self.agent1.make_handshake(
                                                target = cont1_1.guid,
                                                secret = secret1_1
                                                )
        )
        
        handshake2_1 = self.agent2.make_request(
            recipient = self.reader1,
            request = self.agent2.make_handshake(
                                                target = cont2_1.guid,
                                                secret = secret2_1
                                                )
        )
        
        # Make a request to an unknown ID
        handshake3_1 = self.agent1.make_request(
            recipient = self.reader3,
            request = self.agent1.make_handshake(
                                                target = cont1_1.guid,
                                                secret = secret1_1
                                                )
        )
        
        # Make some debindings for those requests
        degloveshake1_1 = self.agent2.make_debind(
            target = handshake1_1.guid
        )
        degloveshake2_1 = self.agent1.make_debind(
            target = handshake2_1.guid
        )
        
        # Make some dynamic bindings!
        dyn1_1a = self.agent1.make_bind_dynamic(
            target = cont1_1.guid
        )
        dyn1_1b = self.agent1.make_bind_dynamic(
            target = cont1_2.guid,
            guid_dynamic = dyn1_1a.guid_dynamic,
            history = [dyn1_1a.guid]
        )
        
        dyn2_1a = self.agent2.make_bind_dynamic(
            target = cont2_1.guid
        )
        dyn2_1b = self.agent2.make_bind_dynamic(
            target = cont2_2.guid,
            guid_dynamic = dyn2_1a.guid_dynamic,
            history = [dyn2_1a.guid]
        )
        
        # And make some fraudulent ones
        dynF_1b = self.agent1.make_bind_dynamic(
            target = cont1_2.guid,
            guid_dynamic = dyn2_1a.guid_dynamic,
            history = [dyn2_1a.guid]
        )
        dynF_2b = self.agent2.make_bind_dynamic(
            target = cont2_2.guid,
            guid_dynamic = dyn1_1a.guid_dynamic,
            history = [dyn1_1a.guid]
        )
        
        # ---------------------------------------
        # Publish bindings and then containers
        self.server1.publish(bind1_1.packed)
        self.server1.publish(bind1_2.packed)
        self.server1.publish(bind2_1.packed)
        self.server1.publish(bind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown binder.'):
            self.server1.publish(bind3_1.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown binder.'):
            self.server1.publish(bind3_2.packed)
            
        self.server1.publish(cont1_1.packed)
        self.server1.publish(cont1_2.packed)
        self.server1.publish(cont2_1.packed)
        self.server1.publish(cont2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown author.'):
            self.server1.publish(cont3_1.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown author.'):
            self.server1.publish(cont3_2.packed)
        
        # ---------------------------------------
        # Publish impersonation debindings for the second identity
        with self.assertRaises(NakError, msg='Server allowed wrong author debind.'):
            self.server1.publish(debind2_1_bad.packed)
        with self.assertRaises(NakError, msg='Server allowed wrong author debind.'):
            self.server1.publish(debind2_2_bad.packed)
        
        # ---------------------------------------
        # Publish debindings for the second identity
        self.server1.publish(debind2_1.packed)
        self.server1.publish(debind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed binding replay.'):
            self.server1.publish(bind2_1.packed)
        with self.assertRaises(NakError, msg='Server allowed binding replay.'):
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
        with self.assertRaises(NakError, msg='Server allowed debinding replay.'):
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
        
        # ---------------------------------------
        # Publish debindings for those debindings' debindings (... fer srlsy?) 
        # and then redebind them
        self.server1.publish(dededebind2_1.packed)
        self.server1.publish(dededebind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed dedebinding replay.'):
            self.server1.publish(dedebind2_1.packed)
        with self.assertRaises(NakError, msg='Server allowed dedebinding replay.'):
            self.server1.publish(dedebind2_2.packed)
            
        self.assertNotIn(dedebind2_1.guid, self.server1._targets_debind)
        self.assertNotIn(dedebind2_2.guid, self.server1._targets_debind)
        self.assertNotIn(debind2_1.guid, self.server1._debindings)
        self.assertNotIn(debind2_2.guid, self.server1._debindings)
        
        self.server1.publish(debind2_1.packed)
        self.server1.publish(debind2_2.packed)
        
        self.assertIn(debind2_1.guid, self.server1._store)
        self.assertIn(debind2_2.guid, self.server1._store)
        
        # ---------------------------------------
        # Publish requests.
        self.server1.publish(handshake1_1.packed)
        self.server1.publish(handshake2_1.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown recipient.'):
            self.server1.publish(handshake3_1.packed)
            
        self.assertIn(handshake1_1.guid, self.server1._store)
        self.assertIn(handshake2_1.guid, self.server1._store)
        
        # ---------------------------------------
        # Debind those requests.
        self.server1.publish(degloveshake1_1.packed)
        self.server1.publish(degloveshake2_1.packed)
            
        self.assertNotIn(handshake1_1.guid, self.server1._store)
        self.assertNotIn(handshake2_1.guid, self.server1._store)
        
        with self.assertRaises(NakError, msg='Server allowed request replay.'):
            self.server1.publish(handshake1_1.packed)
        with self.assertRaises(NakError, msg='Server allowed request replay.'):
            self.server1.publish(handshake2_1.packed)
            
        self.assertNotIn(handshake1_1.guid, self.server1._store)
        self.assertNotIn(handshake2_1.guid, self.server1._store)
        
        # ---------------------------------------
        # Test some dynamic bindings.
        # First make sure the container is actually not there.
        self.assertNotIn(cont2_1.guid, self.server1._store)
        self.assertNotIn(cont2_2.guid, self.server1._store)
        # Now let's see what happens if we upload stuff
        self.server1.publish(dyn1_1a.packed)
        self.server1.publish(dyn2_1a.packed)
        self.server1.publish(cont2_1.packed)
        self.assertIn(dyn1_1a.guid, self.server1._store)
        self.assertIn(dyn2_1a.guid, self.server1._store)
        self.assertIn(cont2_1.guid, self.server1._store)
        # And make sure that the container is retained if we remove the static
        self.server1.publish(debind1_1.packed)
        self.assertNotIn(bind1_1.guid, self.server1._store)
        self.assertIn(cont1_1.guid, self.server1._store)
        # Now let's try some fraudulent updates
        with self.assertRaises(NakError, msg='Server allowed fraudulent dynamic.'):
            self.server1.publish(dynF_1b.packed)
        with self.assertRaises(NakError, msg='Server allowed fraudulent dynamic.'):
            self.server1.publish(dynF_2b.packed)
        # Now the real updates.
        self.server1.publish(dyn1_1b.packed)
        self.server1.publish(dyn2_1b.packed)
        # And now test that cont1 was actually GC'd
        self.assertNotIn(cont1_1.guid, self.server1._store)
        # And that the previous frame (but not its references) were as well
        self.assertIn(dyn1_1b.guid, self.server1._store)
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # Start an interactive IPython interpreter with local namespace, but
        # suppress all IPython-related warnings.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            IPython.embed()

if __name__ == "__main__":
    unittest.main()