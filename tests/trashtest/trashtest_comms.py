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

from hypergolix.comms import WSBasicServer
from hypergolix.comms import WSBasicClient
from hypergolix.comms import WSReqResServer
from hypergolix.comms import WSReqResClient

from hypergolix.exceptions import RequestFinished


class BareTestServer(WSBasicServer):
    def __init__(self, *args, **kwargs):
        self._incoming_counter = 0
        super().__init__(*args, **kwargs)
        
        self.producer_thread = threading.Thread(
            target = self.producer,
            daemon = True,
        )
        self.producer_thread.start()
        self.consumer_thread = threading.Thread(
            target = self.consumer,
            daemon = True,
        )
        self.consumer_thread.start()
    
    @asyncio.coroutine
    def init_connection(self, *args, **kwargs):
        ''' Does anything necessary to initialize a connection.
        '''
        # print('----Starting connection.')
        connection = yield from super().init_connection(*args, **kwargs)
        print('Connection established, Morty #', str(connection.connid), '.')
        return connection
        
    def producer(self):
        ''' Produces anything needed to send to the connection. Must 
        return bytes.
        '''
        while True:
            # This clearly doesn't scale, but we wouldn't normally be iterating
            # across all connections to send out something.
            for connection in list(self._connections.values()):
                time.sleep(random.randint(1,4))
                print('Get it together, Morty #', str(connection.connid), '.')
                time.sleep(.5)
                # buuuuuuurp
                self.send_threadsafe(connection, b'B' + (b'u' * random.randint(1, 14)) + b'rp')
        
    def consumer(self):
        ''' Consumes the msg produced by the websockets receiver 
        listening to the connection.
        '''
        while True:
            connection, msg = self.receive_blocking()
            print('Shuddup Morty #', str(connection.connid), '.')
        
    @asyncio.coroutine
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
    
    
class BareTestClient(WSBasicClient):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._incoming_counter = 0
        self._name = name
        
        self.producer_thread = threading.Thread(
            target = self.producer,
            daemon = True,
        )
        self.producer_thread.start()
        self.consumer_thread = threading.Thread(
            target = self.consumer,
            daemon = True,
        )
        self.consumer_thread.start()
    
    @asyncio.coroutine
    def init_connection(self, *args, **kwargs):
        ''' Does anything necessary to initialize a connection.
        '''
        print('Connection established, Rick.')
        return (yield from super().init_connection(*args, **kwargs))
        
    def producer(self):
        ''' Produces anything needed to send to the connection. Must 
        return bytes.
        '''
        while True:
            time.sleep(random.randint(2,7))
            print('Rick, ', self._name, ' wants attention!')
            self.send_threadsafe(self.connection, b'Goodbye, Moonman.')
        
    def consumer(self):
        ''' Consumes the msg produced by the websockets receiver 
        listening to the connection.
        '''
        while True:
            self._incoming_counter += 1
            connection, msg = self.receive_blocking()
            print(
                'For the ', self._incoming_counter, 
                'th time, Rick just told me, ', self._name, ', ', msg
            )
        
    @asyncio.coroutine
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc


class ReqResTestServer(WSReqResServer):
    def __init__(self, *args, **kwargs):
        req_handlers = {
            # Parrot
            b'!P': self.parrot,
        }
        # # Successful
        # b'+S': self.__success,
        # # Unsuccessful
        # b'-S': self.__failure,
        
        self._incoming_counter = 0
        super().__init__(
            req_handlers = req_handlers, 
            failure_code = b'-S', 
            success_code = b'+S', 
            *args, **kwargs)
        
        self.producer_thread = threading.Thread(
            target = self.producer,
            daemon = True,
        )
        self.producer_thread.start()
        
    def __success(self, connection, token, response):
        self.notify_response(connection, token, response)
        raise RequestFinished()
        
    def __failure(self, connection, token, response):
        response = RuntimeError('REQUEST FAILED!')
        self.notify_response(connection, token, response)
        raise RequestFinished()
        
    def parrot(self, connection, token, response):
        print('Msg from client ' + repr(connection) + ': ' + repr(response))
        return b'Successful parrot.', b'+S'
    
    @asyncio.coroutine
    def init_connection(self, *args, **kwargs):
        ''' Does anything necessary to initialize a connection.
        '''
        # print('----Starting connection.')
        connection = yield from super().init_connection(*args, **kwargs)
        print('Connection established: client', str(connection.connid))
        return connection
        
    def producer(self):
        ''' Produces anything needed to send to the connection. Must 
        return bytes.
        '''
        while True:
            # This clearly doesn't scale, but we wouldn't normally be iterating
            # across all connections to send out something.
            for connection in list(self._connections.values()):
                time.sleep(random.randint(2,7))
                print('Requesting parrot from connection', str(connection.connid))
                self.send_threadsafe(connection, b'B' + (b'u' * random.randint(1, 14)) + b'rp', b'!P')
        
    @asyncio.coroutine
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_autoresponder_exc(self, exc, token):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
        return repr(exc)
    
    
class ReqResTestClient(WSReqResClient):
    def __init__(self, name, *args, **kwargs):
        req_handlers = {
            # Parrot
            b'!P': self.parrot,
        }
        # # Successful
        # b'+S': self.__success,
        # # Unsuccessful
        # b'-S': self.__failure,
        
        self._name = name
        self._incoming_counter = 0
        
        super().__init__(
            req_handlers = req_handlers, 
            failure_code = b'-S', 
            success_code = b'+S', 
            *args, **kwargs)
        
        self.producer_thread = threading.Thread(
            target = self.producer,
            daemon = True,
        )
        self.producer_thread.start()
        
    def __success(self, connection, token, response):
        print('Received success!')
        self.notify_response(connection, token, response)
        raise RequestFinished()
        
    def __failure(self, connection, token, response):
        print('Received failure!')
        response = RuntimeError('REQUEST FAILED!')
        self.notify_response(connection, token, response)
        raise RequestFinished()
        
    def parrot(self, connection, token, response):
        print('Msg from client ' + repr(connection) + ': ' + repr(response))
        return b'Successful parrot.', b'+S'
    
    @asyncio.coroutine
    def init_connection(self, *args, **kwargs):
        ''' Does anything necessary to initialize a connection.
        '''
        # print('----Starting connection.')
        connection = yield from super().init_connection(*args, **kwargs)
        print('Connection established: server.')
        return connection
        
    def producer(self):
        ''' Produces anything needed to send to the connection. Must 
        return bytes.
        '''
        while True:
            # This clearly doesn't scale, but we wouldn't normally be iterating
            # across all connections to send out something.
            time.sleep(random.randint(2,7))
            print('Requesting parrot from server.')
            self.send_threadsafe(self.connection, b'Gro' + (b's' * random.randint(2, 14)), b'!P')
        
    @asyncio.coroutine
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_autoresponder_exc(self, exc, token):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
        return repr(exc)


# ###############################################
# Testing
# ###############################################
        
        
class BareWebsocketsTrashTest(unittest.TestCase):
    def setUp(self):
        self.server = ReqResTestServer(
            host = 'localhost',
            port = 9318,
            threaded = True,
            debug = True
        )
        
        # print('-----Server running.')
        time.sleep(1)
        
        self.client1 = ReqResTestClient(
            host = 'ws://localhost', 
            port = 9318, 
            name = 'OneTrueMorty',
            threaded = True,
            debug = True
        )
        
        # Simulate connection offset between the two
        # print('-----Client1 running.')
        time.sleep(15)
        
        self.client2 = ReqResTestClient(
            host = 'ws://localhost', 
            port = 9318, 
            name = 'HammerMorty',
            threaded = True,
            debug = True
        )
        # print('-----Client2 running.')
        
    def test_comms(self):
        # pass
        # self.server._halt()
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # Start an interactive IPython interpreter with local namespace, but
        # suppress all IPython-related warnings.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            IPython.embed()
        
    
    # def tearDown(self):
    #     self.server._halt()
        

if __name__ == "__main__":
    unittest.main()