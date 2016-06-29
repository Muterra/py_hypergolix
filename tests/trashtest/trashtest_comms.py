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
import collections
import threading
import time
import asyncio
import random
import traceback
import logging

from hypergolix.utils import Aengel

from hypergolix.comms import WSBasicServer
from hypergolix.comms import WSBasicClient
from hypergolix.comms import Autoresponder
from hypergolix.comms import Autocomms

from hypergolix.exceptions import RequestFinished


# ###############################################
# Fixtures
# ###############################################


class TestParrot(Autoresponder):
    def __init__(self, name=None, *args, **kwargs):
        req_handlers = {
            # Parrot
            b'!P': self.parrot,
        }
        
        self._name = name
        self._incoming_counter = 0
        
        super().__init__(
            req_handlers = req_handlers, 
            failure_code = b'-S', 
            success_code = b'+S', 
            *args, **kwargs)
        
    async def parrot(self, session, msg):
        # print('Msg from client ' + str(connection.connid) + ': ' + repr(msg))
        return msg
        
        
TEST_ITERATIONS = 1


SERVER_FLAG = threading.Event()
SERVER_CHECK = collections.deque()
async def server_notifier(connection, msg):
    SERVER_FLAG.set()
    SERVER_CHECK.appendleft(msg)
def server_notification_checker(timeout=1):
    result = SERVER_FLAG.wait(timeout)
    SERVER_FLAG.clear()
    return result

CLIENT1_FLAG = threading.Event()
CLIENT1_CHECK = collections.deque()
async def client1_notifier(connection, msg):
    CLIENT1_FLAG.set()
    CLIENT1_CHECK.appendleft(msg)
def client1_notification_checker(timeout=1):
    result = CLIENT1_FLAG.wait(timeout)
    CLIENT1_FLAG.clear()
    return result

CLIENT2_FLAG = threading.Event()
CLIENT2_CHECK = collections.deque()
async def client2_notifier(connection, msg):
    CLIENT2_FLAG.set()
    CLIENT2_CHECK.appendleft(msg)
def client2_notification_checker(timeout=1):
    result = CLIENT2_FLAG.wait(timeout)
    CLIENT2_FLAG.clear()
    return result


# ###############################################
# Testing
# ###############################################
        
        
class WSBasicTrashTest(unittest.TestCase):
    # @classmethod
    # def setUpClass(cls):
    #     cls.server = WSBasicServer(
    #         host = 'localhost',
    #         port = 9318,
    #         receiver = server_notifier,
    #         threaded = True,
    #         # debug = True
    #     )
        
    #     cls.client1 = WSBasicClient(
    #         host = 'localhost', 
    #         port = 9318, 
    #         receiver = client1_notifier,
    #         threaded = True,
    #         # debug = True
    #     )
        
    #     cls.client2 = WSBasicClient(
    #         host = 'localhost', 
    #         port = 9318, 
    #         receiver = client2_notifier,
    #         threaded = True,
    #         # debug = True
    #     )
        
    def setUp(self):
        self.server = WSBasicServer(
            host = 'localhost',
            port = 9318,
            receiver = server_notifier,
            threaded = True,
            # debug = True
        )
        
        self.client1 = WSBasicClient(
            host = 'localhost', 
            port = 9318, 
            receiver = client1_notifier,
            threaded = True,
            # debug = True
        )
        
        self.client2 = WSBasicClient(
            host = 'localhost', 
            port = 9318, 
            receiver = client2_notifier,
            threaded = True,
            # debug = True
        )
        
    def test_client1(self):
        for ii in range(TEST_ITERATIONS):
            msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
            msg = msg.encode('utf-8')
            
            self.client1.send_threadsafe(msg)
            self.assertTrue(server_notification_checker())
            self.assertEqual(msg, SERVER_CHECK.pop())
        
    def test_client2(self):
        for ii in range(TEST_ITERATIONS):
            msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
            msg = msg.encode('utf-8')
            
            self.client2.send_threadsafe(msg)
            self.assertTrue(server_notification_checker())
            self.assertEqual(msg, SERVER_CHECK.pop())
        
    def test_server(self):
        # Okay so on the one hand, this is bad. On the other hand, we're never
        # going to just rattle off server connections and shoot things off.
        # But, that being said, be careful because there's a race condition 
        # between connid assignment and adding the connection to the lookup.
        # See: async def new_connection in comms.py.
        time.sleep(.1)
        
        for connection in list(self.server._connections.values()):
            
            connection.send_threadsafe(msg=b'test')
            if client1_notification_checker():
                note_checker = client1_notification_checker
                val_checker = CLIENT1_CHECK
            elif client2_notification_checker():
                note_checker = client2_notification_checker
                val_checker = CLIENT2_CHECK
            else:
                raise RuntimeError('Could not successfully retrieve client.')
            val_checker.pop()
            
            for ii in range(TEST_ITERATIONS):
                msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
                msg = msg.encode('utf-8')
                
                connection.send_threadsafe(msg)
                self.assertTrue(note_checker())
                self.assertEqual(msg, val_checker.pop())
                
    def tearDown(self):
        time.sleep(.75)
        self.client1.stop_threadsafe()
        self.client2.stop_threadsafe()
        self.server.stop_threadsafe()
        
    # @classmethod
    # def tearDownClass(cls):
    #     cls.client1.stop_threadsafe()
    #     cls.client2.stop_threadsafe()
    #     cls.server.stop_threadsafe()
    #     # time.sleep(5)
        
        
class WSAutoTrashtest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.aengel = Aengel()
        
        cls.server = Autocomms(
            autoresponder_class = TestParrot,
            connector_class = WSBasicServer,
            connector_kwargs = {
                'host': 'localhost',
                'port': 9319,
            },
            debug = True,
            aengel = cls.aengel,
        )
        
        cls.client1 = Autocomms(
            autoresponder_class = TestParrot,
            autoresponder_args = ('OneTrueMorty',),
            connector_class = WSBasicClient,
            connector_kwargs = {
                'host': 'localhost',
                'port': 9319,
            },
            debug = True,
            aengel = cls.aengel,
        )
        
        cls.client2 = Autocomms(
            autoresponder_class = TestParrot,
            autoresponder_args = ('HammerMorty',),
            connector_class = WSBasicClient,
            connector_kwargs = {
                'host': 'localhost',
                'port': 9319,
            },
            debug = True,
            aengel = cls.aengel,
        )
        
    def test_client1(self):
        for ii in range(TEST_ITERATIONS):
            msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
            msg = msg.encode('utf-8')
            response = self.client1.send_threadsafe(
                session = self.client1.any_session, 
                msg = msg,
                request_code = b'!P'
            )
            self.assertEqual(msg, response)
        
    def test_client2(self):
        for ii in range(TEST_ITERATIONS):
            msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
            msg = msg.encode('utf-8')
            response = self.client2.send_threadsafe(
                session = self.client2.any_session, 
                msg = msg,
                request_code = b'!P'
            )
            self.assertEqual(msg, response)
        
    def test_server(self):
        for ii in range(TEST_ITERATIONS):
            for session in self.server.sessions:
                msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
                msg = msg.encode('utf-8')
                response = self.server.send_threadsafe(
                    session = session, 
                    msg = msg,
                    request_code = b'!P'
                )
                self.assertEqual(msg, response)
        # pass
        # self.server._halt()
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        
    @classmethod
    def tearDownClass(cls):
        cls.aengel.stop()
        # time.sleep(5)

import os
import sys
from contextlib import contextmanager

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
    #NOTE: `copied` is inheritable on Windows when duplicating a standard stream
    with os.fdopen(os.dup(stdout_fd), 'wb') as copied: 
        stdout.flush()  # flush library buffers that dup2 knows nothing about
        try:
            os.dup2(fileno(to), stdout_fd)  # $ exec >&to
        except ValueError:  # filename
            with open(to, 'wb') as to_file:
                os.dup2(to_file.fileno(), stdout_fd)  # $ exec > to
        try:
            yield stdout # allow code to be run with the redirected stdout
        finally:
            # restore stdout to its previous value
            #NOTE: dup2 makes stdout_fd inheritable unconditionally
            stdout.flush()
            os.dup2(copied.fileno(), stdout_fd)  # $ exec >&copied
            
def merged_stderr_stdout():  # $ exec 2>&1
    return stdout_redirected(to=sys.stdout, stdout=sys.stderr)

from hypergolix.utils import TraceLogger

if __name__ == "__main__":
    # # Override the module-level logger definition to root
    # logger = logging.getLogger()
    # # For now, log to console
    # log_handler = logging.StreamHandler()
    # log_handler.setLevel(logging.DEBUG)
    # logger.addHandler(log_handler)
    logging.basicConfig(filename='logs/comms.log', level=logging.DEBUG)
    
    unittest.main()
    # with open('std.py', 'w') as f:
    #     with stdout_redirected(to=f), merged_stderr_stdout():
    #         unittest.main()