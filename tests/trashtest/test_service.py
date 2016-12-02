'''
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
import threading
import pathlib
import logging
import tempfile
import shutil
# Just used for fixture
import random

from loopa.utils import await_coroutine_threadsafe
from loopa import TaskCommander
from loopa import NoopLoop

# These are normal imports
from hypergolix.remotes import RemotePersistenceProtocol
from hypergolix.remotes import Salmonator

from hypergolix.service import RemotePersistenceServer

from hypergolix.comms import BasicServer
from hypergolix.comms import WSConnection
from hypergolix.comms import ConnectionManager
from hypergolix.comms import _ConnectionBase

from hypergolix.utils import Aengel

from hypergolix.persistence import PersistenceCore
from hypergolix.librarian import LibrarianCore
from hypergolix.postal import PostalCore
from hypergolix.postal import PostOffice
from hypergolix.core import GolixCore

from hypergolix.exceptions import RemoteNak
from hypergolix.exceptions import StillBoundWarning

# These are abnormal imports
from golix import Ghid
from golix import ThirdParty
from golix import SecondParty

from golix._getlow import GIDC
from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobsLite
from hypergolix.persistence import _GobdLite
from hypergolix.persistence import _GdxxLite
from hypergolix.persistence import _GarqLite


# ###############################################
# Fixture imports
# ###############################################


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

logger = logging.getLogger(__name__)


# ###############################################
# Testing
# ###############################################


class ServiceTest(unittest.TestCase):
    ''' Perform a whole-system test on the RemotePersistenceServer.
    '''
    
    @classmethod
    def setUpClass(cls):
        ''' Set up all of the various stuff. And things.
        '''
        # Set up the server
        cls.cachedir = tempfile.mkdtemp()
        cls.rps = RemotePersistenceServer(
            cache_dir = cls.cachedir,
            host = '127.0.0.1',
            port = 1989,
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'remps'}
        )
        
        # Set up a remote client.
        cls.client1_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'client1'}
        )
        cls.client1_librarian = LibrarianCore.__fixture__()
        cls.client1_percore = PersistenceCore.__fixture__(
            librarian = cls.client1_librarian
        )
        cls.client1_protocol = RemotePersistenceProtocol()
        cls.client1_protocol._percore = cls.client1_percore
        cls.client1_protocol._librarian = cls.client1_librarian
        cls.client1 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = cls.client1_protocol
        )
        cls.client1_commander.register_task(
            cls.client1,
            host = 'localhost',
            port = 1989,
            tls = False
        )
        
        cls.rps.start()
        cls.client1_commander.start()
        
    @classmethod
    def tearDownClass(cls):
        cls.client1_commander.stop_threadsafe_nowait()
        cls.rps.stop_threadsafe_nowait()
        shutil.rmtree(cls.cachedir, ignore_errors=True)
        
    def setUp(self):
        ''' Do any per-test fixturing.
        '''
        self.client1_librarian.RESET()
        
    def test_ping(self):
        ''' Perform one test ping.
        '''
        await_coroutine_threadsafe(
            coro = self.client1.ping(timeout=1),
            loop = self.client1_commander._loop
        )
        
    def test_exchange_1(self):
        ''' Perform one test exchange.
        '''
        await_coroutine_threadsafe(
            coro = self.client1.publish(gidc1, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.rps.librarian.contains(gidclite1.ghid),
                loop = self.rps._loop
            )
        )
        
    def test_exchange_2(self):
        ''' Perform one test exchange.
        '''
        await_coroutine_threadsafe(
            coro = self.client1.publish(gidc1, timeout=1),
            loop = self.client1_commander._loop
        )
        await_coroutine_threadsafe(
            coro = self.client1.publish(bind1_1.packed, timeout=1),
            loop = self.client1_commander._loop
        )
        await_coroutine_threadsafe(
            coro = self.client1.publish(cont1_1.packed, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.rps.librarian.contains(gidclite1.ghid),
                loop = self.rps._loop
            )
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.rps.librarian.contains(obj1.ghid),
                loop = self.rps._loop
            )
        )
        self.assertTrue(
            await_coroutine_threadsafe(
                coro = self.rps.librarian.contains(sbind1.ghid),
                loop = self.rps._loop
            )
        )
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
