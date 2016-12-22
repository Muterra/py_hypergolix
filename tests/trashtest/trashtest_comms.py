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

import logging
import unittest
import warnings
import collections
import threading
import time
import asyncio
import random
import traceback
import os
import sys
from contextlib import contextmanager

from hypergolix.utils import Aengel

from loopa import TaskCommander
from loopa.utils import await_coroutine_threadsafe

from hypergolix.comms import RequestResponseProtocol
from hypergolix.comms import request
from hypergolix.comms import BasicServer
from hypergolix.comms import MsgBuffer
from hypergolix.comms import WSConnection
from hypergolix.comms import WSBeatingConn
from hypergolix.comms import ConnectionManager

from hypergolix.exceptions import RequestFinished


# ###############################################
# Fixtures
# ###############################################


logger = logging.getLogger(__name__)


ERROR_LOOKUP = {
    b'\x00\x00': Exception,
    b'\x00\x00': ValueError
}


class TestParrot(metaclass=RequestResponseProtocol, error_codes=ERROR_LOOKUP):
    STATIC_RESPONSE = b'\xFF'
    
    @request(b'!P')
    async def parrot(self, connection, msg):
        self.flag.clear()
        return msg
        
    @parrot.request_handler
    async def parrot(self, connection, body):
        return body
        
    @parrot.response_handler
    async def parrot(self, connection, response, exc):
        self.result = response
        self.flag.set()
    
    @request(b'+P')
    async def incr_parrot(self, connection, msg):
        self.flag.clear()
        return msg
        
    @incr_parrot.request_handler
    async def incr_parrot(self, connection, body):
        return self.STATIC_RESPONSE + body
        
    @incr_parrot.response_handler
    async def incr_parrot(self, connection, response, exc):
        self.result = response
        self.flag.set()
        
    @request(b'!S')
    async def static(self, connection):
        return b''
        
    @static.request_handler
    async def static(self, connection, body):
        return self.STATIC_RESPONSE
        
    @parrot.response_handler
    async def parrot(self, connection, response, exc):
        self.result = response
        self.flag.set()
        
    @request(b'!!')
    async def announce(self, connection):
        self.flag.clear()
        return b''
        
    @announce.request_handler
    async def announce(self, connection, body):
        self.connections.append(connection)
        return b''
        
    @request(b'FF')
    async def make_fail(self, connection):
        ''' Intentionally evoke server failure.
        '''
        return b''
        
    @make_fail.request_handler
    async def make_fail(self, connection, body):
        raise ValueError()
        
    @request(b'XD')
    async def make_death(self, connection):
        ''' Intentionally evoke server failure that has non-specific
        error code.
        '''
        return b''
        
    @make_death.request_handler
    async def make_death(self, connection, body):
        raise RuntimeError()
        
    @request(b'N1')
    async def make_nest_1(self, connection, counter):
        ''' Nest commands.
        '''
        return counter.to_bytes(length=4, byteorder='big')
        
    @make_nest_1.request_handler
    async def make_nest_1(self, connection, body):
        await self.make_nest_2(connection, body)
        return body
        
    @make_nest_1.response_handler
    async def make_nest_1(self, connection, response, exc):
        if exc is not None:
            raise exc
            
        return int.from_bytes(response, byteorder='big')
        
    @request(b'N2')
    async def make_nest_2(self, connection, packed_counter):
        return packed_counter
        
    @make_nest_2.request_handler
    async def make_nest_2(self, connection, body):
        counter = int.from_bytes(body, byteorder='big')
        self.nested_counter = counter
        return body
        
    def check_result(self, timeout=1):
        result_available = self.flag.wait(timeout=timeout)
        self.flag.clear()
        
        if result_available:
            return self.result
    
    def __init__(self, name=None, *args, **kwargs):
        # We really shouldn't be using strong references, but whatever, it's
        # just a test.
        self.connections = []
        self.flag = threading.Event()
        self._name = name
        self._incoming_counter = 0
        
        super().__init__(*args, **kwargs)
        
        
TEST_ITERATIONS = 10


# ###############################################
# Testing
# ###############################################
        
        
class WSBasicTrashTest(unittest.TestCase):
        
    def setUp(self):
        # Use a different thread for each of the clients/server
        self.server_commander = TaskCommander(
            reusable_loop = False,
            # We do need this to be threaded so we can handle testing stuff
            # independently
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'server'}
        )
        self.server_protocol = TestParrot()
        self.server = BasicServer(connection_cls=WSConnection)
        self.server_commander.register_task(
            self.server,
            msg_handler = self.server_protocol,
            host = 'localhost',
            port = 9318
        )
        
        self.client1_commander = TaskCommander(
            reusable_loop = False,
            # We do need this to be threaded so we can handle testing stuff
            # independently
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'client1'}
        )
        self.client1_protocol = TestParrot()
        self.client1 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = self.client1_protocol,
            autoretry = False
        )
        self.client1_commander.register_task(
            self.client1,
            host = 'localhost',
            port = 9318,
            tls = False
        )
        
        self.client2_commander = TaskCommander(
            reusable_loop = False,
            # We do need this to be threaded so we can handle testing stuff
            # independently
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'client2'}
        )
        self.client2_protocol = TestParrot()
        self.client2 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = self.client2_protocol,
            autoretry = False
        )
        self.client2_commander.register_task(
            self.client2,
            host = 'localhost',
            port = 9318,
            tls = False
        )
        
        self.server_commander.start()
        await_coroutine_threadsafe(
            coro = self.server_commander.await_init(),
            loop = self.server_commander._loop
        )
        
        self.client1_commander.start()
        await_coroutine_threadsafe(
            coro = self.client1_commander.await_init(),
            loop = self.client1_commander._loop
        )
        
        self.client2_commander.start()
        await_coroutine_threadsafe(
            coro = self.client2_commander.await_init(),
            loop = self.client2_commander._loop
        )
                
    def tearDown(self):
        logger.critical('Entering test shutdown.')
        self.client2_commander.stop_threadsafe(timeout=.5)
        self.client1_commander.stop_threadsafe(timeout=.5)
        # Wait for the server to stop or we may accidentally try to have an
        # overlapping binding to the socket.
        self.server_commander.stop_threadsafe(timeout=.5)
        time.sleep(.1)
        
    def test_client1(self):
        logger.info('Starting client1 test.')
        for ii in range(TEST_ITERATIONS):
            # Generate pseudorandom bytes w/ length 25
            msg = bytes([random.randint(0, 255) for i in range(0, 25)])
            
            # Test a standart parrot
            await_coroutine_threadsafe(
                coro = self.client1.parrot(msg, timeout=1),
                loop = self.client1_commander._loop
            )
            self.assertEqual(msg, self.client1_protocol.check_result())
            
            # Test a version of parrot that modifies the input
            await_coroutine_threadsafe(
                coro = self.client1.incr_parrot(msg, timeout=1),
                loop = self.client1_commander._loop
            )
            self.assertEqual(
                self.client1_protocol.STATIC_RESPONSE + msg,
                self.client1_protocol.check_result()
            )
            
            # Test a static response
            response = await_coroutine_threadsafe(
                coro = self.client1.static(timeout=1),
                loop = self.client1_commander._loop
            )
            self.assertEqual(response, self.client1_protocol.STATIC_RESPONSE)
            
        for ii in range(TEST_ITERATIONS):
            # Generate pseudorandom bytes w/ length 25
            msg = bytes([random.randint(0, 255) for i in range(0, 25)])
            
            # Test a standart parrot
            await_coroutine_threadsafe(
                coro = self.client2.parrot(msg, timeout=1),
                loop = self.client2_commander._loop
            )
            self.assertEqual(msg, self.client2_protocol.check_result())
            
            # Test a version of parrot that modifies the input
            await_coroutine_threadsafe(
                coro = self.client2.incr_parrot(msg, timeout=1),
                loop = self.client2_commander._loop
            )
            self.assertEqual(
                self.client2_protocol.STATIC_RESPONSE + msg,
                self.client2_protocol.check_result()
            )
            
            # Test a static response
            response = await_coroutine_threadsafe(
                coro = self.client2.static(timeout=1),
                loop = self.client2_commander._loop
            )
            self.assertEqual(response, self.client2_protocol.STATIC_RESPONSE)
            
        logger.info('Finished client1 test.')
        
    def test_server(self):
        ''' Test sending messages from the server to the client.
        '''
        logger.info('Starting server test.')
        # First we need to get the server to record the connections. This is
        # a hack, but, well, we're not intending to have things behave this way
        # normally.
        await_coroutine_threadsafe(
            coro = self.client1.announce(timeout=1),
            loop = self.client1_commander._loop
        )
        await_coroutine_threadsafe(
            coro = self.client2.announce(timeout=1),
            loop = self.client2_commander._loop
        )
        
        # Pick a connection at random for each iteration. Double the test
        # iterations so that we get approximately that many iterations for each
        # connection.
        for ii in range(TEST_ITERATIONS * 2):
            connection = random.choice(self.server_protocol.connections)
            # Generate pseudorandom bytes w/ length 25
            msg = bytes([random.randint(0, 255) for i in range(0, 25)])
            
            await_coroutine_threadsafe(
                coro = self.server_protocol.parrot(
                    connection,
                    msg,
                    timeout = 1
                ),
                loop = self.server_commander._loop
            )
            self.assertEqual(msg, self.server_protocol.check_result())
        
        logger.info('Exiting server test.')
            
    def test_failures(self):
        logger.info('Starting failure test.')
        # This kind of failure has a specific error defined
        with self.assertRaises(ValueError):
            await_coroutine_threadsafe(
                coro = self.client1.make_fail(timeout=1),
                loop = self.client1_commander._loop
            )
            
        # This kind of error does not, and should fall back to exception.
        with self.assertRaises(Exception):
            await_coroutine_threadsafe(
                coro = self.client1.make_death(timeout=1),
                loop = self.client1_commander._loop
            )
        logger.info('Exiting failure test.')
    
    @unittest.skip('DNX')
    def test_nest(self):
        counter = random.randint(0, 255)
        
        result = await_coroutine_threadsafe(
            coro = self.client1.make_nest_1(counter, timeout=1),
            loop = self.client1_commander._loop
        )
        self.assertEqual(self.server_protocol.nested_counter, counter)
        self.assertEqual(result, counter)
        
        
class WSHeartbeatTest(unittest.TestCase):
        
    def setUp(self):
        # Use a different thread for each of the clients/server
        self.server_commander = TaskCommander(
            reusable_loop = False,
            # We do need this to be threaded so we can handle testing stuff
            # independently
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'server'}
        )
        self.server_protocol = TestParrot()
        self.server = BasicServer(connection_cls=WSConnection)
        self.server_commander.register_task(
            self.server,
            msg_handler = self.server_protocol,
            host = 'localhost',
            port = 9318
        )
        
        self.client1_commander = TaskCommander(
            reusable_loop = False,
            # We do need this to be threaded so we can handle testing stuff
            # independently
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'client1'}
        )
        self.client1_protocol = TestParrot()
        self.client1 = ConnectionManager(
            connection_cls = WSBeatingConn,
            msg_handler = self.client1_protocol,
            autoretry = False
        )
        self.client1_commander.register_task(
            self.client1,
            host = 'localhost',
            port = 9318,
            tls = False
        )
        
        self.client2_commander = TaskCommander(
            reusable_loop = False,
            # We do need this to be threaded so we can handle testing stuff
            # independently
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'client2'}
        )
        self.client2_protocol = TestParrot()
        self.client2 = ConnectionManager(
            connection_cls = WSBeatingConn,
            msg_handler = self.client2_protocol,
            autoretry = False
        )
        self.client2_commander.register_task(
            self.client2,
            host = 'localhost',
            port = 9318,
            tls = False
        )
        
        self.server_commander.start()
        await_coroutine_threadsafe(
            coro = self.server_commander.await_init(),
            loop = self.server_commander._loop
        )
        
        self.client1_commander.start()
        await_coroutine_threadsafe(
            coro = self.client1_commander.await_init(),
            loop = self.client1_commander._loop
        )
        
        self.client2_commander.start()
        await_coroutine_threadsafe(
            coro = self.client2_commander.await_init(),
            loop = self.client2_commander._loop
        )
                
    def tearDown(self):
        logger.critical('Entering test shutdown.')
        self.client2_commander.stop_threadsafe(timeout=.5)
        self.client1_commander.stop_threadsafe(timeout=.5)
        # Wait for the server to stop or we may accidentally try to have an
        # overlapping binding to the socket.
        self.server_commander.stop_threadsafe(timeout=.5)
        time.sleep(.1)
        
    def test_client1(self):
        logger.info('Starting client1 test.')
        for ii in range(TEST_ITERATIONS):
            # Generate pseudorandom bytes w/ length 25
            msg = bytes([random.randint(0, 255) for i in range(0, 25)])
            
            # Test a standart parrot
            await_coroutine_threadsafe(
                coro = self.client1.parrot(msg, timeout=1),
                loop = self.client1_commander._loop
            )
            self.assertEqual(msg, self.client1_protocol.check_result())
            
            # Test a version of parrot that modifies the input
            await_coroutine_threadsafe(
                coro = self.client1.incr_parrot(msg, timeout=1),
                loop = self.client1_commander._loop
            )
            self.assertEqual(
                self.client1_protocol.STATIC_RESPONSE + msg,
                self.client1_protocol.check_result()
            )
            
            # Test a static response
            response = await_coroutine_threadsafe(
                coro = self.client1.static(timeout=1),
                loop = self.client1_commander._loop
            )
            self.assertEqual(response, self.client1_protocol.STATIC_RESPONSE)
            
        for ii in range(TEST_ITERATIONS):
            # Generate pseudorandom bytes w/ length 25
            msg = bytes([random.randint(0, 255) for i in range(0, 25)])
            
            # Test a standart parrot
            await_coroutine_threadsafe(
                coro = self.client2.parrot(msg, timeout=1),
                loop = self.client2_commander._loop
            )
            self.assertEqual(msg, self.client2_protocol.check_result())
            
            # Test a version of parrot that modifies the input
            await_coroutine_threadsafe(
                coro = self.client2.incr_parrot(msg, timeout=1),
                loop = self.client2_commander._loop
            )
            self.assertEqual(
                self.client2_protocol.STATIC_RESPONSE + msg,
                self.client2_protocol.check_result()
            )
            
            # Test a static response
            response = await_coroutine_threadsafe(
                coro = self.client2.static(timeout=1),
                loop = self.client2_commander._loop
            )
            self.assertEqual(response, self.client2_protocol.STATIC_RESPONSE)
            
        logger.info('Finished client1 test.')
        
    def test_server(self):
        ''' Test sending messages from the server to the client.
        '''
        logger.info('Starting server test.')
        # First we need to get the server to record the connections. This is
        # a hack, but, well, we're not intending to have things behave this way
        # normally.
        await_coroutine_threadsafe(
            coro = self.client1.announce(timeout=1),
            loop = self.client1_commander._loop
        )
        await_coroutine_threadsafe(
            coro = self.client2.announce(timeout=1),
            loop = self.client2_commander._loop
        )
        
        # Pick a connection at random for each iteration. Double the test
        # iterations so that we get approximately that many iterations for each
        # connection.
        for ii in range(TEST_ITERATIONS * 2):
            connection = random.choice(self.server_protocol.connections)
            # Generate pseudorandom bytes w/ length 25
            msg = bytes([random.randint(0, 255) for i in range(0, 25)])
            
            await_coroutine_threadsafe(
                coro = self.server_protocol.parrot(
                    connection,
                    msg,
                    timeout = 1
                ),
                loop = self.server_commander._loop
            )
            self.assertEqual(msg, self.server_protocol.check_result())
        
        logger.info('Exiting server test.')


def fileno(file_or_fd):
    fd = getattr(file_or_fd, 'fileno', lambda: file_or_fd)()
    if not isinstance(fd, int):
        raise ValueError("Expected a file (`.fileno()`) or a file descriptor")
    return fd


@contextmanager
def stdout_redirected(to=os.devnull, stdout=None):
    if stdout is None:
        stdout = sys.stdout

    stdout_fd = fileno(stdout)
    # copy stdout_fd before it is overwritten
    # NOTE: `copied` is inheritable on Windows when duplicating a standard
    # stream
    with os.fdopen(os.dup(stdout_fd), 'wb') as copied:
        stdout.flush()  # flush library buffers that dup2 knows nothing about
        try:
            os.dup2(fileno(to), stdout_fd)  # $ exec >&to
        except ValueError:  # filename
            with open(to, 'wb') as to_file:
                os.dup2(to_file.fileno(), stdout_fd)  # $ exec > to
        try:
            yield stdout    # allow code to be run with the redirected stdout
        finally:
            # restore stdout to its previous value
            # NOTE: dup2 makes stdout_fd inheritable unconditionally
            stdout.flush()
            os.dup2(copied.fileno(), stdout_fd)  # $ exec >&copied
            
            
def merged_stderr_stdout():  # $ exec 2>&1
    return stdout_redirected(to=sys.stdout, stdout=sys.stderr)


if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
    
    # with open('std.py', 'w') as f:
    #     with stdout_redirected(to=f), merged_stderr_stdout():
    #         unittest.main()
