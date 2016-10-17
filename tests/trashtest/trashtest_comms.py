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
import IPython
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
from hypergolix.comms import ConnectionManager

from hypergolix.exceptions import RequestFinished


# ###############################################
# Fixtures
# ###############################################


class TestParrot(metaclass=RequestResponseProtocol):
    @request(b'!P')
    async def parrot(self, connection, msg):
        self.flag.clear()
        return msg
        
    @parrot.request_handler
    async def parrot(self, connection, body):
        return body
        
    @parrot.response_handler
    async def parrot(self, response, exc):
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
            debug = True,
            name = 'server'
        )
        self.server_protocol = TestParrot()
        self.server = BasicServer(connection_cls=WSConnection)
        self.server_commander.register_task(
            self.server,
            msg_handler = self.server_protocol,
            host = 'localhost',
            port = 9318,
            # debug = True
        )
        
        self.client1_commander = TaskCommander(
            reusable_loop = False,
            # We do need this to be threaded so we can handle testing stuff
            # independently
            threaded = True,
            debug = True,
            name = 'client1'
        )
        self.client1_protocol = TestParrot()
        self.client1 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = self.client1_protocol,
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
            debug = True,
            name = 'client2'
        )
        self.client2_protocol = TestParrot()
        self.client2 = ConnectionManager(
            connection_cls = WSConnection,
            msg_handler = self.client2_protocol,
        )
        self.client2_commander.register_task(
            self.client2,
            host = 'localhost',
            port = 9318,
            tls = False
        )
        self.server_commander.start()
        self.client1_commander.start()
        self.client2_commander.start()
        
    def test_client1(self):
        for ii in range(TEST_ITERATIONS):
            msg = ''.join([chr(random.randint(0, 255)) for i in range(0, 25)])
            msg = msg.encode('utf-8')
            
            await_coroutine_threadsafe(
                coro = self.client1.parrot(msg, timeout=1),
                loop = self.client1_commander._loop
            )
            self.assertEqual(msg, self.client1_protocol.check_result())
            
        for ii in range(TEST_ITERATIONS):
            msg = ''.join([chr(random.randint(0, 255)) for i in range(0, 25)])
            msg = msg.encode('utf-8')
            
            await_coroutine_threadsafe(
                coro = self.client2.parrot(msg, timeout=1),
                loop = self.client2_commander._loop
            )
            self.assertEqual(msg, self.client2_protocol.check_result())
        
    def test_server(self):
        ''' Test sending messages from the server to the client.
        '''
        # First we need to get the server to record the connections. This is
        # a hack, but, well, we're not intending to have things behave this way
        # normally.
        await_coroutine_threadsafe(
            coro = self.client1.announce(),
            loop = self.client1_commander._loop
        )
        await_coroutine_threadsafe(
            coro = self.client2.announce(),
            loop = self.client2_commander._loop
        )
        
        # Pick a connection at random for each iteration. Double the test
        # iterations so that we get approximately that many iterations for each
        # connection.
        for ii in range(TEST_ITERATIONS * 2):
            connection = random.choice(self.server_protocol.connections)
            msg = ''.join([chr(random.randint(0, 255)) for i in range(0, 25)])
            msg = msg.encode('utf-8')
            
            await_coroutine_threadsafe(
                coro = self.server_protocol.parrot(
                    connection,
                    msg,
                    timeout = 1
                ),
                loop = self.server_commander._loop
            )
            self.assertEqual(msg, self.server_protocol.check_result())
                
    def tearDown(self):
        self.client2_commander.stop_threadsafe_nowait()
        self.client1_commander.stop_threadsafe_nowait()
        self.server_commander.stop_threadsafe_nowait()


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
    logutils.autoconfig(loglevel='info')
    
    from hypergolix.utils import TraceLogger
    with TraceLogger(interval=10):
        unittest.main()
    # unittest.main()
    
    # with open('std.py', 'w') as f:
    #     with stdout_redirected(to=f), merged_stderr_stdout():
    #         unittest.main()
