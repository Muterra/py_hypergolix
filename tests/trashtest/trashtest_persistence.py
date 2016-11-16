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
from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Doorman
from hypergolix.persistence import Lawyer
from hypergolix.persistence import Enforcer
from hypergolix.undertaker import UndertakerCore
from hypergolix.bookie import BookieCore
from hypergolix.librarian import DiskLibrarian
from hypergolix.librarian import MemoryLibrarian

from hypergolix.postal import MrPostman

from hypergolix.remotes import SalmonatorNoop

from hypergolix.exceptions import PersistenceError
from hypergolix.exceptions import StillBoundWarning

# These are abnormal imports
from golix import Ghid
from golix import ThirdParty
from golix import SecondParty

# ###############################################
# Test fixtures
# ###############################################

from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
from _fixtures.identities import TEST_AGENT3

from _fixtures.identities import TEST_READER1
from _fixtures.identities import TEST_READER2
from _fixtures.identities import TEST_READER3

from _fixtures.remote_exchanges import *

SUBS_NOTIFIER = threading.Event()


def subs_notification_checker(timeout=.1):
    result = SUBS_NOTIFIER.wait(timeout)
    SUBS_NOTIFIER.clear()
    return result
    
    
def clear_ghidcache(cache_dir):
    dirpath = pathlib.Path(cache_dir)
    for fpath in [f for f in dirpath.iterdir() if f.is_file()]:
        fpath.unlink()
        
        
class RolodexMock:
    def notification_handler(self, subs_ghid, notify_ghid):
        # Note that we can't necessarily simply look for notify_ghid in the
        # vault, because for dynamic objects, the notify_ghid is the frame_ghid
        # (which is not tracked by the vault).
        SUBS_NOTIFIER.set()
        
        
class GCoreMock:
    def __init__(self, identity):
        self.identity = identity
        
    @property
    def whoami(self):
        return self.identity.ghid
        
        
class GaoMock:
    def __init__(self, ghid):
        self.ghid = ghid
        
        def weak_touch(subscription, notification):
            # Yep, just the subs notifier again
            SUBS_NOTIFIER.set()
        
        self._weak_touch = weak_touch


# ###############################################
# Unified persistence testing (bad division of concerns)
# ###############################################
    

class IntegrationTest:
    ''' Test integration of all cores.
    '''
    
    def dummy_callback(self, subs_ghid, notify_ghid):
        # Note that we can't necessarily simply look for notify_ghid in the
        # vault, because for dynamic objects, the notify_ghid is the frame_ghid
        # (which is not tracked by the vault).
        self.assertIn(notify_ghid, self.librarian)
        SUBS_NOTIFIER.set()
        
    def test_trash(self):
        # ---------------------------------------
        # Publish identity containers
        
        self.percore.ingest(gidc1)
        self.percore.ingest(gidc2)
        
        self.assertIn(TEST_READER1.ghid, self.librarian)
        self.assertIn(TEST_READER2.ghid, self.librarian)
        # Don't publish the third, we want to test refusal
        
        # ---------------------------------------
        # Publish bindings and then containers
        self.percore.ingest(bind1_1.packed)
        self.percore.ingest(bind1_2.packed)
        self.percore.ingest(bind2_1.packed)
        self.percore.ingest(bind2_2.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed unknown binder.'):
            self.percore.ingest(bind3_1.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed unknown binder.'):
            self.percore.ingest(bind3_2.packed)
            
        self.percore.ingest(cont1_1.packed)
        self.percore.ingest(cont1_2.packed)
        self.percore.ingest(cont2_1.packed)
        self.percore.ingest(cont2_2.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed unknown author.'):
            self.percore.ingest(cont3_1.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed unknown author.'):
            self.percore.ingest(cont3_2.packed)
        
        # ---------------------------------------
        # Publish impersonation debindings for the second identity
        with self.assertRaises(PersistenceError, msg='Server allowed wrong author debind.'):
            self.percore.ingest(debind2_1_bad.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed wrong author debind.'):
            self.percore.ingest(debind2_2_bad.packed)
        
        # ---------------------------------------
        # Publish debindings for the second identity
        self.percore.ingest(debind2_1.packed)
        self.percore.ingest(debind2_2.packed)
            
        self.assertIn(debind2_1.ghid, self.librarian)
        self.assertNotIn(bind2_1.ghid, self.librarian)
        self.assertNotIn(cont2_1.ghid, self.librarian)
        
        with self.assertRaises(PersistenceError, msg='Server allowed binding replay.'):
            self.percore.ingest(bind2_1.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed binding replay.'):
            self.percore.ingest(bind2_2.packed)
        
        # ---------------------------------------
        # Now our impersonation debindings should work
        self.percore.ingest(debind2_1_bad.packed)
        self.percore.ingest(debind2_2_bad.packed)
        
        # ---------------------------------------
        # Publish debindings for the valid debindings
        self.percore.ingest(dedebind2_1.packed)
        self.percore.ingest(dedebind2_2.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed debinding replay.'):
            self.percore.ingest(debind2_1.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed debinding replay.'):
            self.percore.ingest(debind2_2.packed)
            
        # Check to verify emptiness
        self.assertNotIn(debind2_1.ghid, self.bookie.debind_status(bind2_1.ghid))
        self.assertNotIn(debind2_2.ghid, self.bookie.debind_status(bind2_2.ghid))
            
        self.assertIn(dedebind2_1.ghid, self.librarian)
        self.assertNotIn(debind2_1.ghid, self.librarian)
        self.assertNotIn(bind2_1.ghid, self.librarian)
        self.assertNotIn(cont2_1.ghid, self.librarian)
        
        # ---------------------------------------
        # Now rebind the original objects. Should succeed, and remove the 
        # illegal impersonation bindings while we're at it.
        self.percore.ingest(bind2_1.packed)
        self.percore.ingest(bind2_2.packed)
        self.percore.ingest(cont2_1.packed)
        self.percore.ingest(cont2_2.packed)
        
        self.assertIn(bind2_1.ghid, self.librarian)
        self.assertIn(cont2_1.ghid, self.librarian)
        self.assertNotIn(debind2_1_bad.ghid, self.librarian)
        self.assertNotIn(debind2_2_bad.ghid, self.librarian)
        
        # ---------------------------------------
        # Publish debindings for those debindings' debindings (... fer srlsy?) 
        # and then redebind them
        self.percore.ingest(dededebind2_1.packed)
        self.percore.ingest(dededebind2_2.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed dedebinding replay.'):
            self.percore.ingest(dedebind2_1.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed dedebinding replay.'):
            self.percore.ingest(dedebind2_2.packed)
            
        # Check to verify emptiness
        self.assertFalse(self.bookie.debind_status(debind2_1.ghid))
        self.assertFalse(self.bookie.debind_status(debind2_2.ghid))
        
        self.percore.ingest(debind2_1.packed)
        self.percore.ingest(debind2_2.packed)
        
        self.assertIn(debind2_1.ghid, self.librarian)
        self.assertIn(debind2_2.ghid, self.librarian)
        
        # ---------------------------------------
        # Subscribe to requests.
        
        # Identity-based subs are auto-handled by rolodex.
        
        # self.postman.subscribe(TEST_AGENT1.ghid, self.dummy_callback)
        # self.postman.subscribe(TEST_AGENT2.ghid, self.dummy_callback)
        
        # ---------------------------------------
        # Publish requests.
        
        # Our fake identity is only Alice, not Bob, so this should not notify
        self.percore.ingest(handshake1_1.packed)
        self.postman.do_mail_run()
        self.assertFalse(subs_notification_checker())
        
        # But this should.
        self.percore.ingest(handshake2_1.packed)
        self.postman.do_mail_run()
        self.assertTrue(subs_notification_checker())
        
        with self.assertRaises(PersistenceError, msg='Server allowed unknown recipient.'):
            self.percore.ingest(handshake3_1.packed)
            
        self.assertIn(handshake1_1.ghid, self.librarian)
        self.assertIn(handshake2_1.ghid, self.librarian)
        
        # ---------------------------------------
        # Debind those requests.
        self.percore.ingest(degloveshake1_1.packed)
        self.percore.ingest(degloveshake2_1.packed)
            
        self.assertNotIn(handshake1_1.ghid, self.librarian)
        self.assertNotIn(handshake2_1.ghid, self.librarian)
        
        with self.assertRaises(PersistenceError, msg='Server allowed request replay.'):
            self.percore.ingest(handshake1_1.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed request replay.'):
            self.percore.ingest(handshake2_1.packed)
            
        self.assertNotIn(handshake1_1.ghid, self.librarian)
        self.assertNotIn(handshake2_1.ghid, self.librarian)
        
        # ---------------------------------------
        # Test some dynamic bindings.
        # First make sure the container is actually not there.
        self.assertNotIn(cont2_1.ghid, self.librarian)
        self.assertNotIn(cont2_2.ghid, self.librarian)
        # Now let's see what happens if we upload stuff
        self.percore.ingest(dyn1_1a.packed)
        self.percore.ingest(dyn2_1a.packed)
        self.percore.ingest(cont2_1.packed)
        d11a = self.librarian.summarize(dyn1_1a.ghid_dynamic)
        d21a = self.librarian.summarize(dyn2_1a.ghid_dynamic)
        self.assertEqual(dyn1_1a.ghid, d11a.frame_ghid)
        self.assertEqual(dyn2_1a.ghid, d21a.frame_ghid)
        self.assertIn(cont2_1.ghid, self.librarian)
        # And make sure that the container is retained if we remove the static
        self.percore.ingest(debind1_1.packed)
        self.assertNotIn(bind1_1.ghid, self.librarian)
        self.assertIn(cont1_1.ghid, self.librarian)
        self.assertIn(cont2_1.ghid, self.librarian)
        # Now let's try some fraudulent updates
        with self.assertRaises(PersistenceError, msg='Server allowed fraudulent dynamic.'):
            self.percore.ingest(dynF_1b.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed fraudulent dynamic.'):
            self.percore.ingest(dynF_2b.packed)
            
        # Subscribe to updates before actually sending the real ones.
        # Note that subs are auto-handled through registering a gao.
        gaomock1_1a = GaoMock(dyn1_1a.ghid_dynamic)
        gaomock2_1a = GaoMock(dyn2_1a.ghid_dynamic)
        self.postman.register(gaomock1_1a)
        self.postman.register(gaomock2_1a)
        self.assertIn(gaomock1_1a.ghid, self.postman._listeners)
        self.assertIn(gaomock2_1a.ghid, self.postman._listeners)
            
        # Now the real updates.
        # Since we already have an object for this binding, it should immediately
        # notify.
        self.percore.ingest(dyn1_1b.packed)
        self.postman.do_mail_run()
        self.assertTrue(subs_notification_checker())
        
        # Since we need to upload the object for this binding, it should not notify
        # until we've uploaded the container itself.
        self.percore.ingest(dyn2_1b.packed)
        self.postman.do_mail_run()
        self.assertFalse(subs_notification_checker())
        self.percore.ingest(cont2_2.packed)
        self.postman.do_mail_run()
        self.assertTrue(subs_notification_checker())
        
        # And now test that containers were actually GC'd
        self.assertNotIn(cont1_1.ghid, self.librarian)
        self.assertNotIn(cont2_1.ghid, self.librarian)
        # And that the previous frame (but not its references) were as well
        d11b = self.librarian.summarize(dyn1_1b.ghid_dynamic)
        d21b = self.librarian.summarize(dyn2_1b.ghid_dynamic)
        self.assertEqual(dyn1_1b.ghid, d11b.frame_ghid)
        self.assertEqual(dyn2_1b.ghid, d21b.frame_ghid)
        self.assertIn(cont2_2.ghid, self.librarian)
        
        # Make sure we cannot replay old frames.
        with self.assertRaises(PersistenceError, msg='Server allowed dyn frame replay.'):
            self.percore.ingest(dyn1_1a.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed dyn frame replay.'):
            self.percore.ingest(dyn2_1a.packed)
        
        # Now let's try debinding the dynamics.
        self.percore.ingest(dyndebind1_1.packed)
        self.postman.do_mail_run()
        self.assertTrue(subs_notification_checker())
        
        self.percore.ingest(dyndebind2_1.packed)
        self.postman.do_mail_run()
        self.assertTrue(subs_notification_checker())
        
        with self.assertRaises(PersistenceError, msg='Server allowed debound dyn replay.'):
            self.percore.ingest(dyn1_1a.packed)
        with self.assertRaises(PersistenceError, msg='Server allowed debound dyn replay.'):
            self.percore.ingest(dyn2_1a.packed)
        # And check their state.
        self.assertIn(dyndebind1_1.ghid, self.librarian)
        self.assertIn(dyndebind2_1.ghid, self.librarian)
        
        self.assertNotIn(dyn1_1a.ghid_dynamic, self.librarian)
        self.assertNotIn(dyn2_1a.ghid_dynamic, self.librarian)
        # self.assertNotIn(dyn1_1a.ghid, self.librarian)
        # self.assertNotIn(dyn1_1b.ghid, self.librarian)
        # self.assertNotIn(dyn2_1a.ghid, self.librarian)
        # self.assertNotIn(dyn2_1b.ghid, self.librarian)
        self.assertIn(cont1_2.ghid, self.librarian)
        self.assertNotIn(cont2_2.ghid, self.librarian)
        
        # ----------------------------------------
        # Test remaining subscription methods
        # self.postman.unsubscribe(TEST_AGENT1.ghid, self.dummy_callback)
        # Postmen don't unsub; they automatically remove items when the gao is
        # GC'd.
        del gaomock1_1a
        del gaomock2_1a
        self.assertEqual(len(self.postman._listeners), 0)
        
        # Test listing bindings
        holdings_cont1_2 = set(self.bookie.bind_status(cont1_2.ghid))
        self.assertEqual(holdings_cont1_2, {bind1_2.ghid})
        
        # Test querying debindings
        debindings_cont1_1 = self.bookie.debind_status(bind1_1.ghid)
        self.assertIn(debind1_1.ghid, debindings_cont1_1)
        debindings_cont1_2 = self.bookie.debind_status(bind1_2.ghid)
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


@unittest.skip('Deprecated.')
class MemoryLibrarianTrashtest(unittest.TestCase, IntegrationTest):
    def setUp(self):
        self.percore = PersistenceCore()
        self.doorman = Doorman()
        self.enforcer = Enforcer()
        self.lawyer = Lawyer()
        self.bookie = BookieCore()
        self.librarian = MemoryLibrarian()
        self.postman = MrPostman()
        self.undertaker = UndertakerCore()
        self.salmonator = SalmonatorNoop()
        
        self.rolodex = RolodexMock()
        self.golcore = GCoreMock(TEST_AGENT1)
        
        self.percore.assemble(self.doorman, self.enforcer, self.lawyer, 
                            self.bookie, self.librarian, self.postman, 
                            self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.postman.assemble(self.golcore, self.librarian, self.bookie, 
                            self.rolodex)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        self.lawyer.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore)
        self.salmonator.assemble(self.percore, self.percore, self.doorman, 
                                self.postman, self.librarian)

    
@unittest.skip('Deprecated.')
class DiskLibrarianTrashtest(unittest.TestCase, IntegrationTest):
    def setUp(self):
        # Do this on a per-test basis so we have a clean ghidcache for the 
        # restoration test
        clear_ghidcache('/ghidcache_test')
        
        self.percore = PersistenceCore()
        self.doorman = Doorman()
        self.enforcer = Enforcer()
        self.lawyer = Lawyer()
        self.bookie = BookieCore()
        self.librarian = DiskLibrarian(cache_dir='/ghidcache_test')
        self.postman = MrPostman()
        self.undertaker = UndertakerCore()
        self.salmonator = SalmonatorNoop()
        
        self.rolodex = RolodexMock()
        self.golcore = GCoreMock(TEST_AGENT1)
        
        self.percore.assemble(self.doorman, self.enforcer, self.lawyer, 
                            self.bookie, self.librarian, self.postman, 
                            self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.postman.assemble(self.golcore, self.librarian, self.bookie, 
                            self.rolodex)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        self.lawyer.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore)
        self.salmonator.assemble(self.percore, self.percore, self.doorman, 
                                self.postman, self.librarian)
        
    def test_restoration(self):
        self.test_trash()
        
        percore = PersistenceCore()
        doorman = Doorman()
        enforcer = Enforcer()
        lawyer = Lawyer()
        bookie = Bookie()
        librarian = DiskLibrarian(cache_dir='/ghidcache_test')
        postman = MrPostman()
        undertaker = UndertakerCore()
        salmonator = SalmonatorNoop()
        
        rolodex = RolodexMock()
        golcore = GCoreMock(TEST_AGENT1)
        
        percore.assemble(doorman, enforcer, lawyer, 
                            bookie, librarian, postman, 
                            undertaker, salmonator)
        doorman.assemble(librarian)
        postman.assemble(golcore, librarian, bookie, rolodex)
        undertaker.assemble(librarian, bookie, postman)
        lawyer.assemble(librarian)
        enforcer.assemble(librarian)
        bookie.assemble(librarian, lawyer, undertaker)
        librarian.assemble(percore)
        salmonator.assemble(percore, percore, doorman, 
                                postman, librarian)
        
        # And now reload errything
        librarian.restore()
        
        # And make sure the two librarians are equivalent
        self.assertEqual(self.librarian._catalog, librarian._catalog)
        
        all_debound = self.bookie._debound_by_ghid.combine(
            self.bookie._debound_by_ghid_staged)
        all_debound2 = bookie._debound_by_ghid.combine(
            bookie._debound_by_ghid_staged)
        
        self.assertEqual(all_debound, all_debound2)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
