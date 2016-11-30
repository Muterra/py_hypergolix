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
import random
import gc

from loopa import NoopLoop
from loopa.utils import await_coroutine_threadsafe

from queue import Empty

from golix import Ghid

from hypergolix.utils import LooperTrooper
from hypergolix.utils import Aengel
from hypergolix.utils import SetMap
from hypergolix.utils import NoContext
from hypergolix.utils import ApiID

from hypergolix.comms import _ConnectionBase
from hypergolix.core import Oracle
from hypergolix.ipc import IPCServerProtocol

from hypergolix.dispatch import Dispatcher
from hypergolix.dispatch import _Dispatchable

from hypergolix.exceptions import UnknownToken
from hypergolix.exceptions import ExistantAppError


# ###############################################
# Fixture imports
# ###############################################


from _fixtures.ghidutils import make_random_ghid


# ###############################################
# Fixture code and boilerplate
# ###############################################


logger = logging.getLogger(__name__)


class Reffable:
    ''' Noop class that supports being weakreffed.
    '''


# ###############################################
# Testing
# ###############################################


class TestDispatcher(unittest.TestCase):
    ''' Test the Dispatcher, YO!
    '''
    
    @classmethod
    def setUpClass(cls):
        # Set up some reffable stuff so we can create fixture _Dispatchables
        cls.golcore = Reffable()
        cls.ghidproxy = Reffable()
        cls.privateer = Reffable()
        cls.percore = Reffable()
        cls.librarian = Reffable()
        
        # Set up the nooploop
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
        ''' Perform test-specific setup.
        '''
        # Whoami isn't relevant here, so just ignore it.
        self.oracle = Oracle.__fixture__()
        self.ipc_protocol = IPCServerProtocol.__fixture__(whoami=None)
        # Some assembly required
        self.dispatch = Dispatcher()
        self.dispatch.assemble(self.oracle, self.ipc_protocol)
        self.dispatch.bootstrap(
            all_tokens = set(),
            startup_objs = {},
            private_by_ghid = {},
            token_lock = NoContext(),
            incoming_shares = set(),
            orphan_acks = SetMap(),
            orphan_naks = SetMap()
        )
        
    def test_app_start(self):
        ''' Test starting an application, including token creation.
        '''
        # Create a new connection and start it.
        conn = _ConnectionBase.__fixture__()
        token = await_coroutine_threadsafe(
            coro = self.dispatch.start_application(conn),
            loop = self.nooploop._loop
        )
        self.assertEqual(len(token), 4)
        
        # For the same connection, make sure attempting to start a second app
        # fails.
        with self.assertRaises(ExistantAppError):
            await_coroutine_threadsafe(
                coro = self.dispatch.start_application(conn, bytes(4)),
                loop = self.nooploop._loop
            )
            
        # For a different connection, make sure attempting to start the same
        # app also fails.
        conn2 = _ConnectionBase.__fixture__()
        with self.assertRaises(ExistantAppError):
            await_coroutine_threadsafe(
                coro = self.dispatch.start_application(conn2, token),
                loop = self.nooploop._loop
            )
        
        # Now attempt to register a different, but unknown, token for the same
        # connection.
        unk_token = token
        while unk_token == token:
            unk_token = bytes([random.randint(0, 255) for i in range(4)])
        
        with self.assertRaises(UnknownToken):
            await_coroutine_threadsafe(
                coro = self.dispatch.start_application(conn2, unk_token),
                loop = self.nooploop._loop
            )
            
        # Finally, delete the original connection, and re-register the existing
        # token under the new connection.
        del conn
        gc.collect()
        token2 = await_coroutine_threadsafe(
            coro = self.dispatch.start_application(conn2, token),
            loop = self.nooploop._loop
        )
        self.assertEqual(token, token2)
        
    def test_obj_tracking(self):
        ''' Test tracking and untracking an object for an application.
        '''
        # Create a new connection
        conn = _ConnectionBase.__fixture__()
        ghid1 = make_random_ghid()
        ghid2 = make_random_ghid()
        
        self.dispatch.track_object(conn, ghid1)
        
        self.dispatch.untrack_object(conn, ghid1)
        self.dispatch.untrack_object(conn, ghid2)
        
    def test_obj_registration_nonprivate(self):
        ''' Test registering a new, non-private object.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # Register a non-private object first
        await_coroutine_threadsafe(
            coro = self.dispatch.register_object(conn, obj.ghid, False),
            loop = self.nooploop._loop
        )
        
    def test_obj_registration_private(self):
        ''' Test registering a new, private object.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # With no token, this should raise.
        with self.assertRaises(UnknownToken):
            await_coroutine_threadsafe(
                coro = self.dispatch.register_object(conn, obj.ghid, True),
                loop = self.nooploop._loop
            )
            
        # With token defined, we're good to go.
        token = bytes([random.randint(0, 255) for i in range(4)])
        self.dispatch._token_from_conn[conn] = token
        await_coroutine_threadsafe(
            coro = self.dispatch.register_object(conn, obj.ghid, True),
            loop = self.nooploop._loop
        )
        
    def test_distr_share_1(self):
        ''' Test distributing a share with no APIs defined.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # No origin, no skip_conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share(obj.ghid, None, None),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.shares.pop()
        
        # No origin, skip_conn=conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share(obj.ghid, None, conn),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.shares.pop()
            
        # Origin, no skip_conn
        origin = make_random_ghid()
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share(obj.ghid, origin, None),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.shares.pop()
        
    def test_distr_share_2(self):
        ''' Test distributing a share with no APIs defined.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        self.dispatch._conns_from_api.add(obj.api_id, conn)
        
        # No origin, no skip_conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share(obj.ghid, None, None),
            loop = self.nooploop._loop
        )
        sharelog = self.ipc_protocol.shares.pop()
        self.assertIs(sharelog.connection, conn)
        self.assertEqual(sharelog.ghid, obj.ghid)
        
        # No origin, skip_conn=conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share(obj.ghid, None, conn),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.shares.pop()
        
    def test_distr_update_1(self):
        ''' Test distributing an update with no APIs defined.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # Not deleted, no skip_conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, False, None),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.updates.pop()
        
        # Not deleted, skip_conn=conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, False, conn),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.updates.pop()
        
    def test_distr_update_2(self):
        ''' Test distributing an update with APIs defined.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        self.dispatch._update_listeners.add(obj.ghid, conn)
        
        # No delete, no skip_conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, False, None),
            loop = self.nooploop._loop
        )
        updatelog = self.ipc_protocol.updates.pop()
        self.assertEqual(updatelog, (conn, obj.ghid))
        
        # No delete, skip_conn=conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, False, conn),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.updates.pop()
            
    def test_distr_delete_1(self):
        ''' Test distributing a deletion with no APIs defined.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        # Deleted, no skip_conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, True, None),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.deletes.pop()
        
        # Deleted, skip_conn=conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, True, conn),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.deletes.pop()
    
    def test_distr_delete_2(self):
        ''' Test distributing a deletion with APIs defined.
        '''
        # Create a new connection and object fixture
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        self.dispatch._update_listeners.add(obj.ghid, conn)
        
        # Deleted, no skip_conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, True, None),
            loop = self.nooploop._loop
        )
        deletelog = self.ipc_protocol.deletes.pop()
        self.assertEqual(deletelog, (conn, obj.ghid))
            
        # Deleted, skip_conn=conn
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_update(obj.ghid, True, conn),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.deletes.pop()
            
    def test_distr_share_success_1(self):
        ''' Test share success distribution with no tokens.
        '''
        # Create a new connection and object fixture
        recipient = make_random_ghid()
        # conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share_success(
                obj.ghid,
                recipient,
                set(),
            ),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.share_successes.pop()
            
    def test_distr_share_success_2(self):
        ''' Test share success distribution with dummy tokens.
        '''
        # Create a new connection and object fixture
        recipient = make_random_ghid()
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        token = bytes([random.randint(0, 255) for i in range(4)])
        self.dispatch._conn_from_token[token] = conn
        
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share_success(
                obj.ghid,
                recipient,
                {token},
            ),
            loop = self.nooploop._loop
        )
        shsulog = self.ipc_protocol.share_successes.pop()
        self.assertEqual(shsulog, (conn, obj.ghid, recipient))
            
    def test_distr_share_failure_1(self):
        ''' Test share failure distribution with no tokens.
        '''
        # Create a new connection and object fixture
        recipient = make_random_ghid()
        # conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share_failure(
                obj.ghid,
                recipient,
                set(),
            ),
            loop = self.nooploop._loop
        )
        with self.assertRaises(IndexError):
            self.ipc_protocol.share_failures.pop()
            
    def test_distr_share_failure_2(self):
        ''' Test share failure distribution with dummy tokens.
        '''
        # Create a new connection and object fixture
        recipient = make_random_ghid()
        conn = _ConnectionBase.__fixture__()
        obj = _Dispatchable.__fixture__(
            ghid = make_random_ghid(),
            dynamic = True,
            author = make_random_ghid(),
            legroom = 7,
            api_id = ApiID(bytes([random.randint(0, 255) for i in range(64)])),
            state = bytes([random.randint(0, 255) for i in range(4)]),
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            dispatch = self.dispatch,
            ipc_protocol = self.ipc_protocol
        )
        self.oracle.add_object(obj.ghid, obj)
        
        token = bytes([random.randint(0, 255) for i in range(4)])
        self.dispatch._conn_from_token[token] = conn
        
        await_coroutine_threadsafe(
            coro = self.dispatch.distribute_share_failure(
                obj.ghid,
                recipient,
                {token},
            ),
            loop = self.nooploop._loop
        )
        shfalog = self.ipc_protocol.share_failures.pop()
        self.assertEqual(shfalog, (conn, obj.ghid, recipient))
            
    def test_startup_objs(self):
        ''' Test share failure distribution with dummy tokens.
        '''
        # Create a new connection and object fixture
        ghid = make_random_ghid()
        conn = _ConnectionBase.__fixture__()
        conn_2 = _ConnectionBase.__fixture__()
        token = bytes([random.randint(0, 255) for i in range(4)])
        self.dispatch._token_from_conn[conn] = token
        self.dispatch._all_known_tokens.add(token)
        unk_token = token
        while unk_token == token:
            unk_token = bytes([random.randint(0, 255) for i in range(4)])
        
        # With no registered token...
        with self.assertRaises(UnknownToken):
            await_coroutine_threadsafe(
                coro = self.dispatch.register_startup(conn_2, ghid),
                loop = self.nooploop._loop
            )
            
        # With registered token...
        await_coroutine_threadsafe(
            coro = self.dispatch.register_startup(conn, ghid),
            loop = self.nooploop._loop
        )
        
        # Now try getting that back and ensure symmetry
        self.assertEqual(
            await_coroutine_threadsafe(
                coro = self.dispatch.get_startup_obj(token),
                loop = self.nooploop._loop
            ),
            ghid
        )
        
        # Now try that again with a different, unknown token
        with self.assertRaises(UnknownToken):
            await_coroutine_threadsafe(
                coro = self.dispatch.get_startup_obj(unk_token),
                loop = self.nooploop._loop
            )
            
        # Now try deregistering it with the right token
        await_coroutine_threadsafe(
            coro = self.dispatch.deregister_startup(conn),
            loop = self.nooploop._loop
        )
        
        # And again without one
        with self.assertRaises(UnknownToken):
            await_coroutine_threadsafe(
                coro = self.dispatch.get_startup_obj(conn_2),
                loop = self.nooploop._loop
            )
        
        
@unittest.skip('DNX')
class TestDispatchable(unittest.TestCase):
    def test_dispatchable(self):
        raise NotImplementedError()
            
    def test_notify(self):
        raise NotImplementedError()
        # Test update notification
        ghid = make_random_ghid()
        self.dispatch.notify(ghid)
        argbuffer = self.ipccore.arg_buffer.popleft()
        self.assertEqual(ghid, argbuffer[0])
        callsheetbuffer = self.ipccore.callsheet_buffer.popleft()
        self.assertEqual(self.ipccore.endpoints, callsheetbuffer)
        callerbuffer = self.ipccore.caller_buffer.popleft()
        self.assertEqual(callerbuffer, self.ipccore.send_update)
        
        # Test deletion notification
        ghid = make_random_ghid()
        self.dispatch.notify(ghid, deleted=True)
        argbuffer = self.ipccore.arg_buffer.popleft()
        self.assertEqual(ghid, argbuffer[0])
        callsheetbuffer = self.ipccore.callsheet_buffer.popleft()
        self.assertEqual(self.ipccore.endpoints, callsheetbuffer)
        callerbuffer = self.ipccore.caller_buffer.popleft()
        self.assertEqual(callerbuffer, self.ipccore.send_delete)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
