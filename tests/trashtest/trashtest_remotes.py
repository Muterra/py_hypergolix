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
import threading
import pathlib
import logging

from loopa.utils import await_coroutine_threadsafe
from loopa import TaskCommander

# These are normal imports
from hypergolix.remotes import RemotePersistenceProtocol
from hypergolix.remotes import Salmonator
from hypergolix.remotes import Remote

from hypergolix.service import RemotePersistenceServer

from hypergolix.comms import BasicServer
from hypergolix.comms import WSConnection
from hypergolix.comms import ConnectionManager

from hypergolix.utils import Aengel

from golix._getlow import GIDC
from hypergolix.persistence import _GidcLite
from hypergolix.persistence import PersistenceCore
from hypergolix.librarian import LibrarianCore
from hypergolix.postal import PostalCore
from hypergolix.postal import PostOffice

from hypergolix.exceptions import RemoteNak
from hypergolix.exceptions import StillBoundWarning

# These are abnormal imports
from golix import Ghid
from golix import ThirdParty
from golix import SecondParty

# ###############################################
# Test fixtures
# ###############################################

# logging.basicConfig(filename='persister_refactor.py', level=logging.INFO)

from _fixtures.ghidutils import make_random_ghid
from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2
from _fixtures.identities import TEST_AGENT3

from _fixtures.identities import TEST_READER1
from _fixtures.identities import TEST_READER2
from _fixtures.identities import TEST_READER3

from _fixtures.remote_exchanges import gidc1

gidclite1 = _GidcLite.from_golix(GIDC.unpack(gidc1))

logger = logging.getLogger(__name__)


# ###############################################
# Testing
# ###############################################


class WSRemoteTest(unittest.TestCase):
    ''' Test the remote persistence protocol using websockets.
    '''
    
    @classmethod
    def setUpClass(cls):
        ''' Set up all of the various stuff. And things.
        '''
        # Set up the remote server.
        cls.server_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'server'}
        )
        cls.server_protocol = RemotePersistenceProtocol()
        cls.server = BasicServer(connection_cls=WSConnection)
        cls.server_commander.register_task(
            cls.server,
            msg_handler = cls.server_protocol,
            host = 'localhost',
            port = 5358,
            # debug = True
        )
        
        # Set up a remote client.
        cls.client1_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'client1'}
        )
        cls.client1_protocol = RemotePersistenceProtocol()
        cls.client1 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = cls.client1_protocol
        )
        cls.client1_commander.register_task(
            cls.client1,
            host = 'localhost',
            port = 5358,
            tls = False
        )
        
        # Set up a second remote client.
        cls.client2_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'client2'}
        )
        cls.client2_protocol = RemotePersistenceProtocol()
        cls.client2 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = cls.client2_protocol
        )
        cls.client2_commander.register_task(
            cls.client2,
            host = 'localhost',
            port = 5358,
            tls = False
        )
        
        cls.server_commander.start()
        cls.client1_commander.start()
        cls.client2_commander.start()
        
    @classmethod
    def tearDownClass(cls):
        cls.client1_commander.stop_threadsafe_nowait()
        cls.client2_commander.stop_threadsafe_nowait()
        cls.server_commander.stop_threadsafe_nowait()
        
    def setUp(self):
        ''' Do any per-test fixturing.
        '''
        self.server_percore = PersistenceCore.__fixture__()
        self.server_librarian = LibrarianCore.__fixture__()
        self.server_postman = PostOffice.__fixture__()
        self.server_protocol.assemble(self.server_percore,
                                      self.server_librarian,
                                      self.server_postman)
        
        self.client1_percore = PersistenceCore.__fixture__()
        self.client1_librarian = LibrarianCore.__fixture__()
        self.client1_postman = PostalCore.__fixture__()
        self.client1_protocol.assemble(self.client1_percore,
                                       self.client1_librarian,
                                       self.client1_postman)
        
        self.client2_percore = PersistenceCore.__fixture__()
        self.client2_librarian = LibrarianCore.__fixture__()
        self.client2_postman = PostalCore.__fixture__()
        self.client2_protocol.assemble(self.client2_percore,
                                       self.client2_librarian,
                                       self.client2_postman)
    
    def test_ping(self):
        logger.info('STARTING REMOTE PING TEST')
        await_coroutine_threadsafe(
            coro = self.client1.ping(timeout=1),
            loop = self.client1_commander._loop
        )
        
    def test_publish(self):
        logger.info('STARTING REMOTE PUBLISH TEST')
        await_coroutine_threadsafe(
            coro = self.client1.publish(gidc1, timeout=1),
            loop = self.client1_commander._loop
        )
        
    def test_get(self):
        logger.info('STARTING REMOTE GET TEST')
        await_coroutine_threadsafe(
            coro = self.server_librarian.store(gidclite1, gidc1),
            loop = self.server_commander._loop
        )
        
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.client1.get(gidclite1.ghid, timeout=1),
                loop = self.client1_commander._loop
            ),
            gidc1
        )
        
    def test_subscribe(self):
        logger.info('STARTING REMOTE SUBSCRIBE TEST')
        ghid = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.client1.subscribe(ghid, timeout=1),
            loop = self.client1_commander._loop
        )
        
        await_coroutine_threadsafe(
            coro = self.client1.unsubscribe(ghid, timeout=1),
            loop = self.client1_commander._loop
        )
        
        # Also test with not-subscribed ghid
        await_coroutine_threadsafe(
            coro = self.client1.unsubscribe(make_random_ghid(), timeout=1),
            loop = self.client1_commander._loop
        )
    
    def test_subs_update(self):
        logger.info('STARTING REMOTE SUBS UPDATE TEST')
        await_coroutine_threadsafe(
            coro = self.client1_librarian.store(gidclite1, gidc1),
            loop = self.client1_commander._loop
        )
        
        subscription_ghid = make_random_ghid()
        # Normally this would come from the server, but it doesn't actually
        # make a difference, and this way we don't have to worry about figuring
        # out which connection is which or anything.
        await_coroutine_threadsafe(
            coro = self.client1.subscription_update(
                subscription_ghid,
                gidclite1.ghid,
                timeout = 1
            ),
            loop = self.client1_commander._loop
        )
        
    def test_subs_query(self):
        logger.info('STARTING REMOTE SUBS QUERY TEST')
        await_coroutine_threadsafe(
            coro = self.client1.query_subscriptions(timeout=1),
            loop = self.client1_commander._loop
        )
        
    def test_bindings_query(self):
        logger.info('STARTING REMOTE BINDINGS TEST')
        await_coroutine_threadsafe(
            coro = self.client1.query_bindings(make_random_ghid(), timeout=1),
            loop = self.client1_commander._loop
        )
        
    def test_debindings_query(self):
        logger.info('STARTING REMOTE DEBINDINGS TEST')
        await_coroutine_threadsafe(
            coro = self.client1.query_debindings(
                make_random_ghid(),
                timeout = 1
            ),
            loop = self.client1_commander._loop
        )
        
    def test_existence_query(self):
        logger.info('STARTING REMOTE EXISTENCE TEST')
        await_coroutine_threadsafe(
            coro = self.client1.query_existence(make_random_ghid(), timeout=1),
            loop = self.client1_commander._loop
        )
        
    def test_disconnect(self):
        logger.info('STARTING REMOTE DISCONNECT TEST')
        await_coroutine_threadsafe(
            coro = self.client1.disconnect(timeout=1),
            loop = self.client1_commander._loop
        )


@unittest.skip('DNX')
class RemoteManagerTest(unittest.TestCase):
    ''' Test the remote persistence protocol using websockets.
    '''
    
    @classmethod
    def setUpClass(cls):
        ''' Set up all of the various stuff. And things.
        '''
        # Set up the remote server.
        cls.server_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'server'}
        )
        cls.server_protocol = RemotePersistenceProtocol()
        cls.server = BasicServer(connection_cls=WSConnection)
        cls.server_commander.register_task(
            cls.server,
            msg_handler = cls.server_protocol,
            host = 'localhost',
            port = 5358,
            # debug = True
        )
        
        # Set up a remote client.
        cls.client1_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'client1'}
        )
        cls.client1_protocol = RemotePersistenceProtocol()
        cls.client1 = Remote(
            connection_cls = WSConnection,
            msg_handler = cls.client1_protocol
        )
        cls.client1_commander.register_task(
            cls.client1,
            host = 'localhost',
            port = 5358,
            tls = False
        )
        
        # Set up a second remote client.
        cls.client2_commander = TaskCommander(
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'client2'}
        )
        cls.client2_protocol = RemotePersistenceProtocol()
        cls.client2 = Remote(
            connection_cls = WSConnection,
            msg_handler = cls.client2_protocol
        )
        cls.client2_commander.register_task(
            cls.client2,
            host = 'localhost',
            port = 5358,
            tls = False
        )
        
        cls.server_commander.start()
        cls.client1_commander.start()
        cls.client2_commander.start()
        
    @classmethod
    def tearDownClass(cls):
        cls.client1_commander.stop_threadsafe_nowait()
        cls.client2_commander.stop_threadsafe_nowait()
        cls.server_commander.stop_threadsafe_nowait()
        
    def setUp(self):
        ''' Do any per-test fixturing.
        '''
        self.server_percore = PersistenceCore.__fixture__()
        self.server_librarian = LibrarianCore.__fixture__()
        self.server_postman = PostalCore.__fixture__()
        self.server_protocol.assemble(self.server_percore,
                                      self.server_librarian,
                                      self.server_postman)
        
        self.client1_percore = PersistenceCore.__fixture__()
        self.client1_librarian = LibrarianCore.__fixture__()
        self.client1_postman = PostalCore.__fixture__()
        self.client1_protocol.assemble(self.client1_percore,
                                       self.client1_librarian,
                                       self.client1_postman)
        
        self.client2_percore = PersistenceCore.__fixture__()
        self.client2_librarian = LibrarianCore.__fixture__()
        self.client2_postman = PostalCore.__fixture__()
        self.client2_protocol.assemble(self.client2_percore,
                                       self.client2_librarian,
                                       self.client2_postman)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
