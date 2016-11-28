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
import queue
import random
import inspect
import asyncio

from loopa import TaskLooper
from loopa import NoopLoop
from loopa.utils import await_coroutine_threadsafe

from hypergolix.gao import GAO
from hypergolix.gao import GAOCore

from hypergolix.rolodex import Rolodex

from hypergolix.persistence import PersistenceCore
from hypergolix.core import GolixCore
from hypergolix.core import GhidProxier
from hypergolix.privateer import Privateer
from hypergolix.librarian import LibrarianCore
from hypergolix.dispatch import Dispatcher
from hypergolix.remotes import Salmonator

from hypergolix.utils import SetMap

from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobsLite
from hypergolix.persistence import _GobdLite
from hypergolix.persistence import _GdxxLite
from hypergolix.persistence import _GarqLite


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.ghidutils import make_random_ghid
from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2

from golix._getlow import GIDC
from _fixtures.remote_exchanges import gidc1
from _fixtures.remote_exchanges import gidc2
from _fixtures.remote_exchanges import secret1_1
from _fixtures.remote_exchanges import secret1_2
from _fixtures.remote_exchanges import cont1_1
from _fixtures.remote_exchanges import cont1_2
from _fixtures.remote_exchanges import bind1_1
from _fixtures.remote_exchanges import dyn1_1a
from _fixtures.remote_exchanges import dyn1_1b
from _fixtures.remote_exchanges import debind1_1
from _fixtures.remote_exchanges import dyndebind1_1
from _fixtures.remote_exchanges import debindR_2
from _fixtures.remote_exchanges import handshake2_1

logger = logging.getLogger(__name__)

gidclite1 = _GidcLite.from_golix(GIDC.unpack(gidc1))
gidclite2 = _GidcLite.from_golix(GIDC.unpack(gidc2))
obj1 = _GeocLite.from_golix(cont1_1)
obj2 = _GeocLite.from_golix(cont1_2)
sbind1 = _GobsLite.from_golix(bind1_1)
dbind1a = _GobdLite.from_golix(dyn1_1a)
dbind1b = _GobdLite.from_golix(dyn1_1b)
xbind1 = _GdxxLite.from_golix(debind1_1)
xbind1d = _GdxxLite.from_golix(dyndebind1_1)
req1 = _GarqLite.from_golix(handshake2_1)
xbind_req1 = _GdxxLite.from_golix(debindR_2)

# ###############################################
# Testing
# ###############################################


class RolodexTest(unittest.TestCase):
    ''' Test the standard GAO.
    '''
    
    @classmethod
    def setUpClass(cls):
        cls.nooploop = NoopLoop(
            debug = True,
            threaded = True
        )
        cls.nooploop.start()
        
    @classmethod
    def tearDownClass(cls):
        # Kill the running loop.
        cls.nooploop.stop_threadsafe_nowait()
        
    def setUp(self):
        # These are directly required by the GAO
        self.librarian = LibrarianCore.__fixture__()
        self.golcore = GolixCore.__fixture__(TEST_AGENT1,
                                             librarian=self.librarian)
        self.ghidproxy = GhidProxier.__fixture__()
        self.privateer = Privateer.__fixture__(TEST_AGENT1)
        self.percore = PersistenceCore.__fixture__()
        self.salmonator = Salmonator.__fixture__()
        self.dispatch = Dispatcher.__fixture__()
        
        self.rolodex = Rolodex()
        self.rolodex.assemble(self.golcore, self.ghidproxy, self.privateer,
                              self.percore, self.librarian, self.salmonator,
                              self.dispatch)
        self.rolodex.bootstrap(
            pending_requests = {},
            outstanding_shares = SetMap()
        )
        
    def test_share(self):
        ''' Test share_object.
        '''
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        self.privateer.stage(obj1.ghid, secret1_1)
        
        # Try with no requesting token
        await_coroutine_threadsafe(
            coro = self.rolodex.share_object(
                target = obj1.ghid,
                recipient = gidclite2.ghid,
                requesting_token = None
            ),
            loop = self.nooploop._loop
        )
        
        # Try with fake requesting token
        await_coroutine_threadsafe(
            coro = self.rolodex.share_object(
                target = obj1.ghid,
                recipient = gidclite2.ghid,
                requesting_token = bytes(4)
            ),
            loop = self.nooploop._loop
        )
        
    def test_notification_handler(self):
        ''' Test share_object.
        '''
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(req1, handshake2_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind_req1, debindR_2.packed),
            loop = self.nooploop._loop
        )
        
        # Try with request notification
        await_coroutine_threadsafe(
            coro = self.rolodex.notification_handler(
                subscription = gidclite1.ghid,
                notification = req1.ghid
            ),
            loop = self.nooploop._loop
        )
        
        # Try with debinding notification
        await_coroutine_threadsafe(
            coro = self.rolodex.notification_handler(
                subscription = gidclite1.ghid,
                notification = xbind_req1.ghid
            ),
            loop = self.nooploop._loop
        )
        
    def test_share_handlers(self):
        ''' Test Rolodex.share_handler, Rolodex.receipt_ack_handler,
        and Rolodex.receipt_nak_handler.
        '''
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite1, gidc1),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(gidclite2, gidc2),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(obj1, cont1_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(req1, handshake2_1.packed),
            loop = self.nooploop._loop
        )
        await_coroutine_threadsafe(
            coro = self.librarian.store(xbind_req1, debindR_2.packed),
            loop = self.nooploop._loop
        )
        
        # Try share handler
        await_coroutine_threadsafe(
            coro = self.rolodex.share_handler(
                target = req1.ghid,
                sender = gidclite2.ghid
            ),
            loop = self.nooploop._loop
        )
        
        # Try ack handler
        await_coroutine_threadsafe(
            coro = self.rolodex.receipt_ack_handler(
                target = req1.ghid,
                recipient = gidclite1.ghid
            ),
            loop = self.nooploop._loop
        )
        
        # Try nak handler
        await_coroutine_threadsafe(
            coro = self.rolodex.receipt_nak_handler(
                target = req1.ghid,
                recipient = gidclite1.ghid
            ),
            loop = self.nooploop._loop
        )
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
