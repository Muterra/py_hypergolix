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
import time
import threading
import logging
import pathlib

# These are normal imports
from hypergolix.remotes import MemoryPersister
from hypergolix.remotes import DiskCachePersister
from hypergolix.remotes import PersisterBridgeServer
from hypergolix.remotes import PersisterBridgeClient
from hypergolix.remotes import RemotePersistenceServer

from hypergolix.utils import Aengel

from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicServer
from hypergolix.comms import WSBasicClient

from hypergolix.exceptions import NakError
from hypergolix.exceptions import PersistenceWarning

# These are abnormal imports
from golix import Ghid
from golix import ThirdParty
from golix import SecondParty

# ###############################################
# Test fixtures
# ###############################################

# logging.basicConfig(filename='persister_refactor.py', level=logging.INFO)

from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
from _fixtures.identities import TEST_AGENT3

from _fixtures.identities import TEST_READER1
from _fixtures.identities import TEST_READER2
from _fixtures.identities import TEST_READER3

SUBS_NOTIFIER_alice = threading.Event()
SUBS_NOTIFIER_bob = threading.Event()


def clear_notifiers():
    SUBS_NOTIFIER_bob.clear()
    SUBS_NOTIFIER_alice.clear()
    


def subs_notification_checker(timeout=5):
    result_bob = SUBS_NOTIFIER_bob.wait(timeout)
    result_alice = SUBS_NOTIFIER_alice.wait(.1)
    clear_notifiers()
    
    if (result_bob and not result_alice):
        return True
    else:
        return False
    
    
def clear_ghidcache(cache_dir):
    dirpath = pathlib.Path(cache_dir)
    for fpath in [f for f in dirpath.iterdir() if f.is_file()]:
        fpath.unlink()
        
        
# ---------------------------------------
# Create and publish identity containers
gidc1 = TEST_READER1.packed
gidc2 = TEST_READER2.packed


# ---------------------------------------
# Prerequisites for testing -- should move to setUp?
# Make some objects for known IDs
pt1 = b'[[ Hello, world? ]]'
pt2 = b'[[ Hiyaback! ]]'
secret1_1 = TEST_AGENT1.new_secret()
cont1_1 = TEST_AGENT1.make_container(
    secret = secret1_1,
    plaintext = pt1
)
secret1_2 = TEST_AGENT1.new_secret()
cont1_2 = TEST_AGENT1.make_container(
    secret = secret1_2,
    plaintext = pt2
)

secret2_1 = TEST_AGENT2.new_secret()
cont2_1 = TEST_AGENT2.make_container(
    secret = secret2_1,
    plaintext = pt1
)
secret2_2 = TEST_AGENT2.new_secret()
cont2_2 = TEST_AGENT2.make_container(
    secret = secret2_2,
    plaintext = pt2
)

# Make some objects for an unknown ID
secret3_1 = TEST_AGENT3.new_secret()
cont3_1 = TEST_AGENT3.make_container(
    secret = secret3_1,
    plaintext = pt1
)
secret3_2 = TEST_AGENT3.new_secret()
cont3_2 = TEST_AGENT3.make_container(
    secret = secret3_2,
    plaintext = pt2
)

# Make some bindings for known IDs
bind1_1 = TEST_AGENT1.make_bind_static(
    target = cont1_1.ghid
)
bind1_2 = TEST_AGENT1.make_bind_static(
    target = cont1_2.ghid
)

bind2_1 = TEST_AGENT2.make_bind_static(
    target = cont2_1.ghid
)
bind2_2 = TEST_AGENT2.make_bind_static(
    target = cont2_2.ghid
)

# Make some bindings for the unknown ID
bind3_1 = TEST_AGENT3.make_bind_static(
    target = cont3_1.ghid
)
bind3_2 = TEST_AGENT3.make_bind_static(
    target = cont3_2.ghid
)

# Make some debindings 
debind1_1 = TEST_AGENT1.make_debind(
    target = bind1_1.ghid
)
debind1_2 = TEST_AGENT1.make_debind(
    target = bind1_2.ghid
)

# Make some debindings 
debind2_1 = TEST_AGENT2.make_debind(
    target = bind2_1.ghid
)
debind2_2 = TEST_AGENT2.make_debind(
    target = bind2_2.ghid
)

# And make some author-inconsistent debindings
debind2_1_bad = TEST_AGENT1.make_debind(
    target = bind2_1.ghid
)
debind2_2_bad = TEST_AGENT1.make_debind(
    target = bind2_2.ghid
)

# And then make some debindings for the debindings
dedebind2_1 = TEST_AGENT2.make_debind(
    target = debind2_1.ghid
)
dedebind2_2 = TEST_AGENT2.make_debind(
    target = debind2_2.ghid
)

# And then make some debindings for the debindings for the...
dededebind2_1 = TEST_AGENT2.make_debind(
    target = dedebind2_1.ghid
)
dededebind2_2 = TEST_AGENT2.make_debind(
    target = dedebind2_2.ghid
)

# Make requests between known IDs
handshake1_1 = TEST_AGENT1.make_request(
    recipient = TEST_READER2,
    request = TEST_AGENT1.make_handshake(
                                        target = cont1_1.ghid,
                                        secret = secret1_1
                                        )
)

handshake2_1 = TEST_AGENT2.make_request(
    recipient = TEST_READER1,
    request = TEST_AGENT2.make_handshake(
                                        target = cont2_1.ghid,
                                        secret = secret2_1
                                        )
)

# Make a request to an unknown ID
handshake3_1 = TEST_AGENT1.make_request(
    recipient = TEST_READER3,
    request = TEST_AGENT1.make_handshake(
                                        target = cont1_1.ghid,
                                        secret = secret1_1
                                        )
)

# Make some debindings for those requests
degloveshake1_1 = TEST_AGENT2.make_debind(
    target = handshake1_1.ghid
)
degloveshake2_1 = TEST_AGENT1.make_debind(
    target = handshake2_1.ghid
)

# Make some dynamic bindings!
dyn1_1a = TEST_AGENT1.make_bind_dynamic(
    target = cont1_1.ghid
)
dyn1_1b = TEST_AGENT1.make_bind_dynamic(
    target = cont1_2.ghid,
    ghid_dynamic = dyn1_1a.ghid_dynamic,
    history = [dyn1_1a.ghid]
)

dyn2_1a = TEST_AGENT2.make_bind_dynamic(
    target = cont2_1.ghid
)
dyn2_1b = TEST_AGENT2.make_bind_dynamic(
    target = cont2_2.ghid,
    ghid_dynamic = dyn2_1a.ghid_dynamic,
    history = [dyn2_1a.ghid]
)

# And make some fraudulent ones
dynF_1b = TEST_AGENT1.make_bind_dynamic(
    target = cont1_2.ghid,
    ghid_dynamic = dyn2_1a.ghid_dynamic,
    history = [dyn2_1a.ghid]
)
dynF_2b = TEST_AGENT2.make_bind_dynamic(
    target = cont2_2.ghid,
    ghid_dynamic = dyn1_1a.ghid_dynamic,
    history = [dyn1_1a.ghid]
)

# Make some debindings 
dyndebind1_1 = TEST_AGENT1.make_debind(
    target = dyn1_1b.ghid_dynamic
)
dyndebind2_1 = TEST_AGENT2.make_debind(
    target = dyn2_1b.ghid_dynamic
)


# ###############################################
# Unified persister testing (bad division of concerns)
# ###############################################
    
    
class _GenericPersisterTest:
    def dummy_callback_alice(self, subs_ghid, notify_ghid):
        # Note that we can't necessarily simply look for notify_ghid in the
        # vault, because for dynamic objects, the notify_ghid is the frame_ghid
        # (which is not tracked by the vault).
        self.assertIn(notify_ghid, self.vault)
        SUBS_NOTIFIER_alice.set()
        
    def dummy_callback_bob(self, subs_ghid, notify_ghid):
        # Note that we can't necessarily simply look for notify_ghid in the
        # vault, because for dynamic objects, the notify_ghid is the frame_ghid
        # (which is not tracked by the vault).
        self.assertIn(notify_ghid, self.vault)
        SUBS_NOTIFIER_bob.set()
        
    def test_trash(self):
        self.remote1.publish(gidc1)
        self.remote1.publish(gidc2)
        
        self.assertIn(TEST_READER1.ghid, self.vault)
        self.assertIn(TEST_READER2.ghid, self.vault)
        # Don't publish the third, we want to test refusal
        
        # ---------------------------------------
        # Publish bindings and then containers
        self.remote1.publish(bind1_1.packed)
        self.remote1.publish(bind1_2.packed)
        self.remote1.publish(bind2_1.packed)
        self.remote1.publish(bind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown binder.'):
            self.remote1.publish(bind3_1.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown binder.'):
            self.remote1.publish(bind3_2.packed)
            
        self.remote1.publish(cont1_1.packed)
        self.remote1.publish(cont1_2.packed)
        self.remote1.publish(cont2_1.packed)
        self.remote1.publish(cont2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown author.'):
            self.remote1.publish(cont3_1.packed)
        with self.assertRaises(NakError, msg='Server allowed unknown author.'):
            self.remote1.publish(cont3_2.packed)
        
        # ---------------------------------------
        # Publish impersonation debindings for the second identity
        with self.assertRaises(NakError, msg='Server allowed wrong author debind.'):
            self.remote1.publish(debind2_1_bad.packed)
        with self.assertRaises(NakError, msg='Server allowed wrong author debind.'):
            self.remote1.publish(debind2_2_bad.packed)
        
        # ---------------------------------------
        # Publish debindings for the second identity
        self.remote1.publish(debind2_1.packed)
        self.remote1.publish(debind2_2.packed)
            
        self.assertIn(debind2_1.ghid, self.vault)
        self.assertNotIn(bind2_1.ghid, self.vault)
        self.assertNotIn(cont2_1.ghid, self.vault)
        
        with self.assertRaises(NakError, msg='Server allowed binding replay.'):
            self.remote1.publish(bind2_1.packed)
        with self.assertRaises(NakError, msg='Server allowed binding replay.'):
            self.remote1.publish(bind2_2.packed)
        
        # ---------------------------------------
        # Now our impersonation debindings should work
        self.remote1.publish(debind2_1_bad.packed)
        self.remote1.publish(debind2_2_bad.packed)
        
        # ---------------------------------------
        # Publish debindings for the valid debindings
        self.remote1.publish(dedebind2_1.packed)
        self.remote1.publish(dedebind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed debinding replay.'):
            self.remote1.publish(debind2_1.packed)
        with self.assertRaises(NakError, msg='Server allowed debinding replay.'):
            self.remote1.publish(debind2_2.packed)
            
        self.assertNotIn(bind2_1.ghid, self.debound_by_ghid)
        self.assertNotIn(bind2_2.ghid, self.debound_by_ghid)
            
        self.assertIn(dedebind2_1.ghid, self.vault)
        self.assertNotIn(debind2_1.ghid, self.vault)
        self.assertNotIn(bind2_1.ghid, self.vault)
        self.assertNotIn(cont2_1.ghid, self.vault)
        
        # ---------------------------------------
        # Now rebind the original objects. Should succeed, and remove the 
        # illegal impersonation bindings while we're at it.
        self.remote1.publish(bind2_1.packed)
        self.remote1.publish(bind2_2.packed)
        self.remote1.publish(cont2_1.packed)
        self.remote1.publish(cont2_2.packed)
        
        self.assertIn(bind2_1.ghid, self.vault)
        self.assertIn(cont2_1.ghid, self.vault)
        self.assertNotIn(debind2_1_bad.ghid, self.vault)
        self.assertNotIn(debind2_2_bad.ghid, self.vault)
        
        # ---------------------------------------
        # Publish debindings for those debindings' debindings (... fer srlsy?) 
        # and then redebind them
        self.remote1.publish(dededebind2_1.packed)
        self.remote1.publish(dededebind2_2.packed)
        with self.assertRaises(NakError, msg='Server allowed dedebinding replay.'):
            self.remote1.publish(dedebind2_1.packed)
        with self.assertRaises(NakError, msg='Server allowed dedebinding replay.'):
            self.remote1.publish(dedebind2_2.packed)
            
        self.assertNotIn(debind2_1.ghid, self.debound_by_ghid)
        self.assertNotIn(debind2_2.ghid, self.debound_by_ghid)
        
        self.remote1.publish(debind2_1.packed)
        self.remote1.publish(debind2_2.packed)
        
        self.assertIn(debind2_1.ghid, self.vault)
        self.assertIn(debind2_2.ghid, self.vault)
        
        # ---------------------------------------
        # Subscribe to requests.
        
        self.remote1.subscribe(TEST_AGENT1.ghid, self.dummy_callback_alice)
        self.remote1.subscribe(TEST_AGENT2.ghid, self.dummy_callback_alice)
        self.remote2.subscribe(TEST_AGENT1.ghid, self.dummy_callback_bob)
        self.remote2.subscribe(TEST_AGENT2.ghid, self.dummy_callback_bob)
        
        # ---------------------------------------
        # Publish requests.
        clear_notifiers()
        self.remote1.publish(handshake1_1.packed)
        self.assertTrue(subs_notification_checker())
        
        clear_notifiers()
        self.remote1.publish(handshake2_1.packed)
        self.assertTrue(subs_notification_checker())
        
        with self.assertRaises(NakError, msg='Server allowed unknown recipient.'):
            self.remote1.publish(handshake3_1.packed)
            
        self.assertIn(handshake1_1.ghid, self.vault)
        self.assertIn(handshake2_1.ghid, self.vault)
        
        # ---------------------------------------
        # Debind those requests.
        self.remote1.publish(degloveshake1_1.packed)
        self.remote1.publish(degloveshake2_1.packed)
            
        self.assertNotIn(handshake1_1.ghid, self.vault)
        self.assertNotIn(handshake2_1.ghid, self.vault)
        
        with self.assertRaises(NakError, msg='Server allowed request replay.'):
            self.remote1.publish(handshake1_1.packed)
        with self.assertRaises(NakError, msg='Server allowed request replay.'):
            self.remote1.publish(handshake2_1.packed)
            
        self.assertNotIn(handshake1_1.ghid, self.vault)
        self.assertNotIn(handshake2_1.ghid, self.vault)
        
        # ---------------------------------------
        # Test some dynamic bindings.
        # First make sure the container is actually not there.
        self.assertNotIn(cont2_1.ghid, self.vault)
        self.assertNotIn(cont2_2.ghid, self.vault)
        # Now let's see what happens if we upload stuff
        self.remote1.publish(dyn1_1a.packed)
        self.remote1.publish(dyn2_1a.packed)
        self.remote1.publish(cont2_1.packed)
        d11a = self.vault.whois(dyn1_1a.ghid_dynamic)
        d21a = self.vault.whois(dyn2_1a.ghid_dynamic)
        self.assertEqual(dyn1_1a.ghid, d11a.frame_ghid)
        self.assertEqual(dyn2_1a.ghid, d21a.frame_ghid)
        self.assertIn(cont2_1.ghid, self.vault)
        # And make sure that the container is retained if we remove the static
        self.remote1.publish(debind1_1.packed)
        self.assertNotIn(bind1_1.ghid, self.vault)
        self.assertIn(cont1_1.ghid, self.vault)
        self.assertIn(cont2_1.ghid, self.vault)
        # Now let's try some fraudulent updates
        with self.assertRaises(NakError, msg='Server allowed fraudulent dynamic.'):
            self.remote1.publish(dynF_1b.packed)
        with self.assertRaises(NakError, msg='Server allowed fraudulent dynamic.'):
            self.remote1.publish(dynF_2b.packed)
            
        # Subscribe to updates before actually sending the real ones.
        self.remote1.subscribe(dyn1_1a.ghid_dynamic, self.dummy_callback_alice)
        self.remote1.subscribe(dyn2_1a.ghid_dynamic, self.dummy_callback_alice)
        self.remote2.subscribe(dyn1_1a.ghid_dynamic, self.dummy_callback_bob)
        self.remote2.subscribe(dyn2_1a.ghid_dynamic, self.dummy_callback_bob)
            
        # Now the real updates.
        # Since we already have an object for this binding, it should immediately
        # notify.
        clear_notifiers()
        self.remote1.publish(dyn1_1b.packed)
        self.assertTrue(subs_notification_checker())
        
        # Since we need to upload the object for this binding, it should not notify
        # until we've uploaded the container itself.
        clear_notifiers()
        self.remote1.publish(dyn2_1b.packed)
        self.remote1.publish(cont2_2.packed)
        self.assertTrue(subs_notification_checker())
        
        # And now test that containers were actually GC'd
        self.assertNotIn(cont1_1.ghid, self.vault)
        self.assertNotIn(cont2_1.ghid, self.vault)
        # And that the previous frame (but not its references) were as well
        d11b = self.vault.whois(dyn1_1b.ghid_dynamic)
        d21b = self.vault.whois(dyn2_1b.ghid_dynamic)
        self.assertEqual(dyn1_1b.ghid, d11b.frame_ghid)
        self.assertEqual(dyn2_1b.ghid, d21b.frame_ghid)
        self.assertIn(cont2_2.ghid, self.vault)
        
        # Make sure we cannot replay old frames.
        with self.assertRaises(NakError, msg='Server allowed dyn frame replay.'):
            self.remote1.publish(dyn1_1a.packed)
        with self.assertRaises(NakError, msg='Server allowed dyn frame replay.'):
            self.remote1.publish(dyn2_1a.packed)
        
        # Now let's try debinding the dynamics.
        clear_notifiers()
        self.remote1.publish(dyndebind1_1.packed)
        self.assertTrue(subs_notification_checker())
        
        clear_notifiers()
        self.remote1.publish(dyndebind2_1.packed)
        self.assertTrue(subs_notification_checker())
        
        with self.assertRaises(NakError, msg='Server allowed debound dyn replay.'):
            self.remote1.publish(dyn1_1a.packed)
        with self.assertRaises(NakError, msg='Server allowed debound dyn replay.'):
            self.remote1.publish(dyn2_1a.packed)
        # And check their state.
        self.assertIn(dyndebind1_1.ghid, self.vault)
        self.assertIn(dyndebind2_1.ghid, self.vault)
        
        self.assertNotIn(dyn1_1a.ghid_dynamic, self.vault)
        self.assertNotIn(dyn2_1a.ghid_dynamic, self.vault)
        # self.assertNotIn(dyn1_1a.ghid, self.vault)
        # self.assertNotIn(dyn1_1b.ghid, self.vault)
        # self.assertNotIn(dyn2_1a.ghid, self.vault)
        # self.assertNotIn(dyn2_1b.ghid, self.vault)
        self.assertIn(cont1_2.ghid, self.vault)
        self.assertNotIn(cont2_2.ghid, self.vault)
        
        # ----------------------------------------
        # Test remaining subscription methods
        self.remote1.list_subs()
        self.remote1.unsubscribe(TEST_AGENT1.ghid, self.dummy_callback_bob)
        self.remote1.disconnect()
        
        # Test listing bindings
        holdings_cont1_2 = set(self.remote1.list_bindings(cont1_2.ghid))
        self.assertEqual(holdings_cont1_2, {bind1_2.ghid})
        
        # Test querying debindings
        debindings_cont1_1 = self.remote1.list_debindings(bind1_1.ghid)
        self.assertIn(debind1_1.ghid, debindings_cont1_1)
        debindings_cont1_2 = self.remote1.list_debindings(bind1_2.ghid)
        self.assertFalse(debindings_cont1_2)
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()


# ###############################################
# Testing
# ###############################################

    
# class MemoryPersisterTrashtest(unittest.TestCase, _GenericPersisterTest):
#     @classmethod
#     def setUpClass(cls):
#         cls.remote1 = MemoryPersister()
#         cls.remote2 = cls.remote1
#         cls.vault = cls.remote1.librarian
#         cls.debound_by_ghid = cls.remote1.bookie._debound_by_ghid

    
# class DiskPersisterTrashtest(unittest.TestCase, _GenericPersisterTest):
#     @classmethod
#     def setUp(self):
#         # Do this on a per-test basis so we have a clean ghidcache for the 
#         # restoration test
#         clear_ghidcache('/ghidcache_test')
#         self.remote1 = DiskCachePersister('/ghidcache_test')
#         self.remote2 = self.remote1
#         self.vault = self.remote1.librarian
#         self.debound_by_ghid = self.remote1.bookie._debound_by_ghid
        
#     def test_restoration(self):
#         ''' This is really an indirect test for 
#         persistence.DiskCachePersister, and therefore should really be
#         in trashtest_persistence.
        
#         '''
#         self.test_trash()
#         persister2 = DiskCachePersister('/ghidcache_test')
#         vault2 = persister2.librarian
#         debound_by_ghid2 = persister2.bookie._debound_by_ghid
#         vault2.restore()
        
#         self.assertEqual(self.vault._catalog, vault2._catalog)
        
#         all_debound = self.remote1.bookie._debound_by_ghid.combine(
#             self.remote1.bookie._debound_by_ghid_staged
#         )
#         all_debound2 = persister2.bookie._debound_by_ghid.combine(
#             persister2.bookie._debound_by_ghid_staged
#         )
        
#         self.assertEqual(all_debound, all_debound2)
        
    
class WSPersisterTrashtest(unittest.TestCase, _GenericPersisterTest):
    @classmethod
    def setUpClass(cls):
        cls.aengel = Aengel()
        cls.remote_server = RemotePersistenceServer()
        cls.vault = cls.remote_server.librarian
        cls.debound_by_ghid = cls.remote_server.bookie._debound_by_ghid
        
        cls.server = Autocomms(
            autoresponder_class = PersisterBridgeServer,
            # autoresponder_kwargs = { 'persister': cls.backend, },
            connector_class = WSBasicServer,
            connector_kwargs = {
                'host': 'localhost',
                'port': 5358,
                # 48 bits = 1% collisions at 2.4 e 10^6 connections
                'birthday_bits': 48,
            },
            debug = True,
            aengel = cls.aengel,
        )
        cls.remote_server.assemble(cls.server)
        
        cls.remote1 = Autocomms(
            autoresponder_class = PersisterBridgeClient,
            connector_class = WSBasicClient,
            connector_kwargs = {
                'host': 'localhost',
                'port': 5358,
            },
            debug = True,
            aengel = cls.aengel,
        )
        cls.remote2 = Autocomms(
            autoresponder_class = PersisterBridgeClient,
            connector_class = WSBasicClient,
            connector_kwargs = {
                'host': 'localhost',
                'port': 5358,
            },
            debug = True,
            aengel = cls.aengel,
        )
        
    @classmethod
    def tearDownClass(cls):
        cls.aengel.stop()
        

if __name__ == "__main__":
    from _fixtures import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()