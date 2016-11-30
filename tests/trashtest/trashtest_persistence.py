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
import logging
import loopa
import collections
import concurrent.futures

from loopa.utils import await_coroutine_threadsafe

# These are normal imports
from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Doorman
from hypergolix.persistence import Enforcer
from hypergolix.persistence import Bookie

from hypergolix.lawyer import LawyerCore
from hypergolix.undertaker import UndertakerCore
from hypergolix.librarian import LibrarianCore
from hypergolix.postal import PostalCore
from hypergolix.remotes import Salmonator

from hypergolix.exceptions import PersistenceError
from hypergolix.exceptions import HypergolixException
from hypergolix.exceptions import RemoteNak
from hypergolix.exceptions import MalformedGolixPrimitive
from hypergolix.exceptions import VerificationFailure
from hypergolix.exceptions import UnboundContainer
from hypergolix.exceptions import InvalidIdentity
from hypergolix.exceptions import DoesNotExist
from hypergolix.exceptions import AlreadyDebound
from hypergolix.exceptions import InvalidTarget
from hypergolix.exceptions import StillBoundWarning
from hypergolix.exceptions import RequestError
from hypergolix.exceptions import InconsistentAuthor
from hypergolix.exceptions import IllegalDynamicFrame
from hypergolix.exceptions import IntegrityError
from hypergolix.exceptions import UnavailableUpstream

from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobsLite
from hypergolix.persistence import _GobdLite
from hypergolix.persistence import _GdxxLite
from hypergolix.persistence import _GarqLite


# ###############################################
# Fixture imports
# ###############################################


from hypergolix.postal import _SubsUpdate

from golix._getlow import GIDC

from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
from _fixtures.identities import TEST_AGENT3

from _fixtures.identities import TEST_READER1
from _fixtures.identities import TEST_READER2
from _fixtures.identities import TEST_READER3

from _fixtures.remote_exchanges import gidc1
from _fixtures.remote_exchanges import gidc2
# Containers
from _fixtures.remote_exchanges import cont1_1  # Known author
from _fixtures.remote_exchanges import cont1_2  # Known author
from _fixtures.remote_exchanges import cont2_1  # Known author
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
from _fixtures.remote_exchanges import dyndebind1_1
from _fixtures.remote_exchanges import dyndebind1_F
from _fixtures.remote_exchanges import dedebind1_1
from _fixtures.remote_exchanges import dededebind1_1
# Requests
from _fixtures.remote_exchanges import handshake1_1     # Known recipient
from _fixtures.remote_exchanges import handshake3_1     # Unknown recipient


# Identities
gidclite1 = _GidcLite.from_golix(GIDC.unpack(gidc1))
gidclite2 = _GidcLite.from_golix(GIDC.unpack(gidc2))

# Containers
obj1 = _GeocLite.from_golix(cont1_1)
obj2 = _GeocLite.from_golix(cont1_2)
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
xxbind1 = _GdxxLite.from_golix(dedebind1_1)
xxxbind1 = _GdxxLite.from_golix(dededebind1_1)
xbind3 = _GdxxLite.from_golix(debind3_1)
xbind1R = _GdxxLite.from_golix(debindR_1)
xbindF1 = _GdxxLite.from_golix(debind1_F)
xbindFR = _GdxxLite.from_golix(debindR_F)
xbind3TF = _GdxxLite.from_golix(debind3_TF)

xbind1d = _GdxxLite.from_golix(dyndebind1_1)
xbind1x = _GdxxLite.from_golix(dedebind1_1)

# Requests
req1 = _GarqLite.from_golix(handshake1_1)
req3 = _GarqLite.from_golix(handshake3_1)


# ###############################################
# Fixture code and boilerplate
# ###############################################


logger = logging.getLogger(__name__)


class PostalFixture(PostalCore):
    ''' Simply register the notifications.
    '''
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.deliveries = collections.deque()
    
    async def _deliver(self, subscription, notification, skip_conn):
        ''' Add in a tuple for everything.
        '''
        self.deliveries.append(
            _SubsUpdate(subscription, notification, skip_conn)
        )
        
        
class LibrarianFixture(LibrarianCore.__fixture__):
    ''' Restore original, non-fixtured version of is_debound for the
    integration test, so that we can verify removal of bad debindings
    when their valid target is uploaded a posteriori.
    '''
    is_debound = LibrarianCore.is_debound


# ###############################################
# Testing
# ###############################################
    

class IntegrationTest(unittest.TestCase):
    ''' Test integration of all cores.
    '''
    
    @classmethod
    def setUpClass(cls):
        # We need a task command (in particular its loop) and an executor
        cls.cmd = loopa.TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'percmd'}
        )
        cls.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        
        # We need the actual components
        cls.percore = PersistenceCore()
        cls.doorman = Doorman(cls.executor, cls.cmd._loop)
        cls.enforcer = Enforcer()
        cls.bookie = Bookie()
        cls.lawyer = LawyerCore()
        cls.postman = PostalFixture()
        cls.undertaker = UndertakerCore()
        # We need to fixture the librarian to have memory-based persistence, as
        # well as the ability to reset state.
        cls.librarian = LibrarianFixture()
        # Upstream/downstream isn't necessary here, and will be covered in the
        # whole-system integration test.
        cls.salmonator = Salmonator.__fixture__()
        
        # Some assembly required...
        cls.percore.assemble(cls.doorman, cls.enforcer, cls.lawyer, cls.bookie,
                             cls.librarian, cls.postman, cls.undertaker,
                             cls.salmonator)
        cls.doorman.assemble(cls.librarian)
        cls.enforcer.assemble(cls.librarian)
        cls.bookie.assemble(cls.librarian)
        cls.lawyer.assemble(cls.librarian)
        cls.librarian.assemble(cls.enforcer, cls.lawyer, cls.percore)
        cls.postman.assemble(cls.librarian)
        cls.undertaker.assemble(cls.librarian, cls.postman)
        # Note that normally salmonator would also require assembly, but due to
        # its fixturing, it is not required.
        
        # We need to register taskloopers with the task command and start it
        cls.cmd.register_task(cls.postman)
        cls.cmd.register_task(cls.undertaker)
        cls.cmd.start()
        
    @classmethod
    def tearDownClass(cls):
        ''' Stop the task command.
        '''
        cls.cmd.stop_threadsafe_nowait()
    
    def setUp(self):
        ''' Reset the librarian and postman to a pristine state.
        '''
        self.librarian.RESET()
        await_coroutine_threadsafe(
            coro = self.postman.await_idle(),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.undertaker.await_idle(),
            loop = self.cmd._loop
        )
        self.postman.deliveries.clear()
        
    def test_gidc(self):
        ''' Test normal ingestion of GIDC.
        '''
        await_coroutine_threadsafe(
            coro = self.percore.ingest(gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.percore.ingest(gidc2),
            loop = self.cmd._loop
        )
        
        # Now make sure both are contained in the librarian.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(TEST_READER1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(TEST_READER2.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_geoc_from_static(self):
        ''' Test normal ingestion of GEOC.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(cont1_1.packed),
            loop = self.cmd._loop
        )
        with self.assertRaises(UnboundContainer):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(cont2_1.packed),
                loop = self.cmd._loop
            )
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(cont3_1.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont2_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont3_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_geoc_from_dynamic(self):
        ''' Test normal ingestion of GEOC.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(cont1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobs(self):
        ''' Test normal ingestion of GOBS.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(bind1_1.packed),
            loop = self.cmd._loop
        )
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(bind3_1.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(bind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(bind3_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobd_new_good(self):
        ''' Test normal ingestion of GOBD.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1a.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobd_new_unknown(self):
        ''' Test ingestion of GOBD with unknown author.
        '''
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(dyn3_1a.packed),
                loop = self.cmd._loop
            )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn3_1a.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobd_update_good(self):
        ''' Test normal ingestion of GOBD.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyn1_1b.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1b.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1a.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobd_update_inconsistent(self):
        ''' Test normal ingestion of GOBD.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbindFa, dynF_a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(InconsistentAuthor):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(dynF_b.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dynF_b.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobd_frame_replay(self):
        ''' Test replaying old GOBD frames.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1b, dyn1_1b.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(IllegalDynamicFrame):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(dyn1_1a.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1b.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1a.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobd_target_cleanup_on_update(self):
        ''' Test that updating a gobd properly GC's its old target.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyn1_1b.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1b.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_gobd_notification_immediate(self):
        ''' Test gobd notifications when the target already exists at
        the librarian (therefore resulting in immediate notification).
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj2, cont1_2.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        self.postman.deliveries.clear()
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyn1_1b.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure notification state is correct.
        update = self.postman.deliveries.pop()
        self.assertEqual(update.subscription, dyn1_1b.ghid_dynamic)
        self.assertEqual(update.notification, dyn1_1b.ghid)
        
    def test_gobd_notification_deferred(self):
        ''' Test gobd notifications when the target is initially missing
        at the librarian (therefore resulting in notification deferral
        until the object itself is ingested).
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        self.postman.deliveries.clear()
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyn1_1b.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure notification state is deferred.
        with self.assertRaises(IndexError):
            update = self.postman.deliveries.pop()
        
        # Now upload the object
        await_coroutine_threadsafe(
            coro = self.percore.ingest(cont1_2.packed),
            loop = self.cmd._loop
        )
        
        # And make sure notification state is correct.
        update = self.postman.deliveries.pop()
        self.assertEqual(update.subscription, dyn1_1b.ghid_dynamic)
        self.assertEqual(update.notification, dyn1_1b.ghid)
    
    def test_obj_retention_from_mixed_fwd(self):
        ''' Test object retention when removing dynamic binding while
        keeping static binding
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyndebind1_1.packed),
            loop = self.cmd._loop
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_obj_retention_from_mixed_rev(self):
        ''' Test object retention when removing static binding while
        keeping dynamic binding
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(debind1_1.packed),
            loop = self.cmd._loop
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_retention_from_mixed_update(self):
        ''' Test container retention when a static binding exists but a
        concurrent dynamic binding passes out of scope through an
        update.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyn1_1b.packed),
            loop = self.cmd._loop
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1a.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(cont1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_static_good(self):
        ''' Test valid removal of static debindings.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(debind1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(bind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_static_inconsistent(self):
        ''' Test inconsistent removal of static debindings.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(sbind1, bind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(InconsistentAuthor):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(debind1_F.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(bind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_F.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_dynamic_good(self):
        ''' Test valid removal of dynamic debindings.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dyndebind1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyndebind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1a.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_dynamic_inconsistent(self):
        ''' Test inconsistent removal of dynamic debindings.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(dbind1a, dyn1_1a.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(InconsistentAuthor):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(dyndebind1_F.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1a.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_F.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_binding_replay_static(self):
        ''' Ensure the persister prevents binding replay when something
        has been successfully debound.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1, debind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(AlreadyDebound):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(bind1_1.packed),
                loop = self.cmd._loop
            )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(bind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_binding_replay_dynamic(self):
        ''' Ensure the persister prevents binding replay when something
        has been successfully debound.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1d, dyndebind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(AlreadyDebound):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(dyn1_1a.packed),
                loop = self.cmd._loop
            )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyndebind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dyn1_1a.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_illegal_debinding_removal(self):
        ''' Make sure that illegal debindings are removed if their
        valid targets are subsequently uploaded.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbindF1, debind1_F.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(bind1_1.packed),
            loop = self.cmd._loop
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(bind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_F.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_debinding(self):
        ''' Test removal of debindings themselves.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1, debind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dedebind1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dedebind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_replay(self):
        ''' Test removal of debindings themselves.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xxbind1, dedebind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(AlreadyDebound):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(debind1_1.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dedebind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_debinding_DEbinding(self):
        ''' Lulz
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xxbind1, dedebind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(dededebind1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dededebind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dedebind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_debinding_REbinding(self):
        ''' Lulz
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xxxbind1, dededebind1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(debind1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(dededebind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debind1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_request_good(self):
        ''' Test uploading a plain request.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(handshake1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(handshake1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_request_unknown(self):
        ''' Test uploading a request with an unknown target.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(InvalidIdentity):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(handshake3_1.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(handshake3_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_request_notification(self):
        ''' Test that a request generates a postman update.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        self.postman.deliveries.clear()
        await_coroutine_threadsafe(
            coro = self.percore.ingest(handshake1_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        update = self.postman.deliveries.pop()
        self.assertEqual(update.subscription, handshake1_1.recipient)
        self.assertEqual(update.notification, handshake1_1.ghid)
        
    def test_debinding_request_good(self):
        ''' Test a proper request debinding.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(req1, handshake1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        await_coroutine_threadsafe(
            coro = self.percore.ingest(debindR_1.packed),
            loop = self.cmd._loop
        )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debindR_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(handshake1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_debinding_request_inconsistent(self):
        ''' Test an inconsistent-author request debinding.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(req1, handshake1_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(InconsistentAuthor):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(debindR_F.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debindR_F.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(handshake1_1.ghid),
                loop = self.cmd._loop
            )
        )
        
    def test_request_replay(self):
        ''' Make sure requests, once debound, cannot be replayed.
        '''
        # PREP WORK!!
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.cmd._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind1R, debindR_1.packed),
            loop = self.cmd._loop
        )
        
        # TEST-SPECIFIC:
        with self.assertRaises(AlreadyDebound):
            await_coroutine_threadsafe(
                coro = self.percore.ingest(handshake1_1.packed),
                loop = self.cmd._loop
            )
        
        # Now make sure librarian state is correct.
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(debindR_1.ghid),
                loop = self.cmd._loop
            )
        )
        self.assertFalse(
            await_coroutine_threadsafe(
                coro = self.librarian.contains(handshake1_1.ghid),
                loop = self.cmd._loop
            )
        )
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
