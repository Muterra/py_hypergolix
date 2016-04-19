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

import abc
import asyncio
import websockets
from websockets.exceptions import ConnectionClosed
import threading
import collections.abc

# Note: this is used exclusively for connection ID generation in _Websocketeer
import random

from .exceptions import RequestFinished
from .exceptions import RequestUnknown


class _WSConnection:
    ''' Bookkeeping object for a single websocket connection (client or
    server).
    
    This should definitely use slots, to save on server memory usage.
    '''
    def __init__(self, loop, websocket, path=None, connid=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.websocket = websocket
        self.path = path
        self.connid = connid
        self._ws_loop = loop
        
        # This is our outgoing comms queue.
        self.outgoing_q = asyncio.Queue(loop=loop)
        
        self.cts = threading.Event()
        
    @asyncio.coroutine
    def close(self):
        ''' Wraps websocket.close.
        '''
        yield from self.websocket.close()
        
    def send_threadsafe(self, msg):
        ''' Threadsafe wrapper to add things to the outgoing queue.
        '''
        sender = asyncio.run_coroutine_threadsafe(
            coro = self.send(msg),
            loop = self._ws_loop
        )
        
        # Block on completion of coroutine and then raise any created exception
        exc = sender.exception()
        if exc:
            raise exc
            
        return True
        
    @asyncio.coroutine
    def send(self, msg):
        ''' NON THREADSAFE wrapper to add things to the outgoing queue.
        '''
        yield from self.outgoing_q.put(msg)
        
    @asyncio.coroutine
    def _await_send(self):
        ''' NON THREADSAFE wrapper to get things from the outgoing queue.
        '''
        return (yield from self.outgoing_q.get())
        

class WSBase(metaclass=abc.ABCMeta):
    ''' Common stuff for websockets clients and servers.
    '''
    def __init__(self, threaded, host, port, debug=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._ws_port = port
        self._ws_host = host
        
        if threaded:
            self._ws_loop = asyncio.new_event_loop()
            self._ws_loop.set_debug(debug)
            self.incoming_q = asyncio.Queue(loop=self._ws_loop)
            # Set up a shutdown event
            self._init_shutdown = asyncio.Event(loop=self._ws_loop)
            # Set up a (daemon) thread for the websockets process
            self._ws_thread = threading.Thread(
                target = self.ws_run,
                daemon = True
            )
            self._ws_thread.start()
            
        else:
            self._ws_loop = asyncio.get_event_loop()
            self._ws_loop.set_debug(debug)
            self.incoming_q = asyncio.Queue(loop=self._ws_loop)
            # Set up a shutdown event
            self._init_shutdown = asyncio.Event(loop=self._ws_loop)
            # Declare the thread as nothing.
            self._ws_thread = None
            
    def new_connection(self, *args, **kwargs):
        ''' Wrapper for creating a new connection. Mostly here to make
        subclasses simpler.
        '''
        return _WSConnection(*args, **kwargs)
        
    @asyncio.coroutine
    def _await_receive(self, connection, msg):
        ''' NON THREADSAFE wrapper to put things into the incoming 
        queue.
        '''
        return (
            yield from self.incoming_q.put(
                # We're putting on a tuple.
                (connection, msg)
            )
        )
        
    def receive_blocking(self):
        ''' Performs a blocking synchronous call to receive the first 
        item in the incoming queue.
        Returns connection, msg tuple.
        '''
        receiver = asyncio.run_coroutine_threadsafe(
            coro = self.receive(),
            loop = self._ws_loop
        )
        
        # Block on completion of coroutine and then raise any created exception
        exc = receiver.exception()
        if exc:
            raise exc
            
        # Note: return (connection, msg) tuple.
        return receiver.result()
        
    @asyncio.coroutine
    def receive(self):
        ''' NON THREADSAFE coroutine for waiting on an incoming message.
        Returns connection, msg tuple.
        '''
        # Note: return (connection, msg) tuple.
        return (yield from self.incoming_q.get())
        
    @asyncio.coroutine
    def receive_threadsafe(self):
        ''' Threadsafe coroutine for waiting on an incoming message. DO
        NOT CALL THIS FROM THE SAME EVENT LOOP AS THE WEBSOCKETS CLIENT!
        Returns connection, msg tuple.
        '''
        raise NotImplementedError(
            'Sorry, haven\'t had a chance to implement this yet and haven\'t '
            'personally had a use for it?'
        )
        
    def send_threadsafe(self, connection, msg, *args, **kwargs):
        ''' Threadsafe wrapper to add things to the outgoing queue. 
        Don't necessarily directly wrap connection.send, in case our 
        version of send is overridden or extended in a subclass.
        
        Passes extra args and kwargs to self.send.
        '''
        sender = asyncio.run_coroutine_threadsafe(
            coro = self.send(connection, msg, *args, **kwargs),
            loop = self._ws_loop
        )
        
        # Block on completion of coroutine and then raise any created exception
        exc = sender.exception()
        if exc:
            raise exc
            
        return True
        
    @asyncio.coroutine
    def send(self, connection, msg):
        ''' Wraps connection.send.
        '''
        return (yield from connection.send(msg))
        
    @asyncio.coroutine
    def _await_send(self, connection):
        ''' Wraps connection._await_send
        '''
        return (yield from connection._await_send())
            
    @property
    def _ws_loc(self):
        return self._ws_host + ':' + str(self._ws_port) + '/'
            
    @abc.abstractmethod
    def ws_run(self):
        ''' Threaded stuff and things. MUST be called via super().
        '''
        if self._ws_thread is not None:
            asyncio.set_event_loop(self._ws_loop)
        
    def halt(self):
        ''' Sets the shutdown flag, killing the connection and client.
        '''
        self._ws_loop.call_soon_threadsafe(self._init_shutdown.set)
        
    @asyncio.coroutine
    def _ws_connect(self, websocket, path=None):
        ''' This handles an entire websockets connection.
        
        Note: could move the shutdown listener outside of the while loop
        for performance optimization.
        '''
        print('Socket connected.')
        
        connection = yield from self.init_connection(websocket, path)
        
        # Signal that the connection is live.
        connection.cts.set()
        
        try:
            while not self._init_shutdown.is_set():
                listener = asyncio.ensure_future(websocket.recv())
                producer = asyncio.ensure_future(connection._await_send())
                interrupter = asyncio.ensure_future(self._init_shutdown.wait())
                
                finished, pending = yield from asyncio.wait(
                    fs = [producer, listener, interrupter],
                    return_when = asyncio.FIRST_COMPLETED
                )
                
                # We have exactly two tasks, so no need to iterate on them.
                # Finished is a set with exactly one item, so...
                finished = finished.pop()
                
                # Manage canceling the other task
                for task in pending:
                    task.cancel()
                
                # If it was the producer, send it out
                if producer is finished:
                    yield from self.handle_producer_exc(connection, finished.exception())
                    yield from websocket.send(finished.result())
                    
                # If it was the listener, consume it
                elif listener is finished:
                    exc = finished.exception()
                    # Make sure the connection is still live
                    if isinstance(exc, ConnectionClosed):
                        raise exc
                    # If so, handle any actual exception
                    else:
                        yield from self.handle_listener_exc(connection, exc)
                    # No exception, so continue on our business
                    yield from self._await_receive(
                        connection = connection, 
                        msg = finished.result()
                    )
                
                # If it was the interrupter, yield to cleanup.
                # Actually, just don't do anything. We won't execute the next
                # while loop, so just let it close out below.
                else:
                    pass
                    # yield from self._conn_cleanup()
                    
        finally:
            print('Listener successfully shut down.')
            yield from connection.close()
            print('Connection closed.')
            print('Stopping loop.')
        
    @asyncio.coroutine
    def catch_interrupt(self):
        ''' Workaround for Windows not passing signals well for doing
        interrupts.
        
        Standard websockets stuff.
        
        Deprecated? Currently unused anyways.
        '''
        while not self._shutdown:
            yield from asyncio.sleep(5)
            
    @asyncio.coroutine
    def _conn_cleanup(self):
        ''' This handles a single websocket REQUEST, not an entire 
        connection.
        '''
        print('Got shutdown signal.')
        # self._ws_loop.stop()
        # print('Stopped loop.')
        
    @asyncio.coroutine
    @abc.abstractmethod
    def init_connection(self, websocket, path=None):
        ''' Does anything necessary to initialize a connection.
        
        Must return a _WSConnection object.
        '''
        pass
        
    @asyncio.coroutine
    @abc.abstractmethod
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        pass
        
    @asyncio.coroutine
    @abc.abstractmethod
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the listener task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        pass
        
        
class _ReqResWSConnection(_WSConnection):
    ''' A request/response websockets connection.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Lookup for request token -> event
        self.pending_requests = {}
        # Lookup for request token -> response
        self.pending_responses = {}
        
        self._req_lock = asyncio.Lock()
        
    @asyncio.coroutine
    def _gen_req_token(self):
        ''' Gets a new (well, currently unused) request token. Sets it
        in pending_requests to prevent race conditions.
        '''
        yield from self._req_lock
        try:
            token = self._gen_unused_token()
            # Do this just so we can release the lock ASAP
            self.pending_requests[token] = None
        finally:
            self._req_lock.release()
            
        return token
            
    def _gen_unused_token(self):
        ''' Recursive search for unused token. THIS IS NOT THREADSAFE
        NOR ASYNC SAFE! Must be called from within parent lock.
        '''
        # Get a random-ish (no need for CSRNG) 16-bit token
        token = random.getrandbits(16)
        if token in self.pending_requests:
            token = self._get_unused_token()
        return token
        
    @asyncio.coroutine
    def send(self, msg, version, expect_reply=True):
        ''' NON THREADSAFE wrapper to add things to the outgoing queue.
        If expect_reply, Will wait for a response to return.
        
        Version should be int-like.
        
        Returns response, version if expect_reply
        Returns None, None otherwise
        '''
        # Get a token and pack the message.
        token = yield from self._gen_req_token()
        packed_msg = (
            version.to_bytes(length=1, byteorder='big', signed=False) + 
            token.to_bytes(length=2, byteorder='big', signed=False) +
            msg
        )
        
        # Create an event to signal response waiting and then put the outgoing
        # in the send queue
        if expect_reply:
            self.pending_requests[token] = asyncio.Event(loop=self._ws_loop)
        yield from self.outgoing_q.put(packed_msg)
        
        # Now wait for the response and then cleanup
        if expect_reply:
            # instrumentation
            print('Waiting for reply.')
            yield from self.pending_requests[token].wait()
            response, version = self.pending_responses[token]
            del self.pending_responses[token]
        else:
            response = None
            version = None
        # We still need to remove the response token regardless -- gen_token 
        # sets it to None to avoid a race condition.
        del self.pending_requests[token]
        
        return response, version
        
    @asyncio.coroutine
    def send_nowait(self, msg, version):
        ''' NON THREADSAFE wrapper to add things to the outgoing queue.
        Does not wait for a response to return. Returns a future.
        '''
        raise NotImplementedError('Not implemented yet.')
        
    @asyncio.coroutine
    def _await_send(self):
        ''' NON THREADSAFE wrapper to get things from the outgoing queue.
        '''
        return (yield from self.outgoing_q.get())
        
        
class ReqResWSBase(WSBase):
    ''' Builds a request/response framework on top of the underlying 
    order-independent websockets implementation.
    
    req_handler will be passed connection, token, body.
    
    req_handlers should be a mapping:
        key(2 bytes): callable
        
    the request callable should return: res body, res code tuple, OR it 
    should raise RequestFinished to denote the end of a req/res chain.
    
    Note that a request handler will never wait for a reply from its 
    response (ie, reply recursion is impossible).
    '''
    def __init__(self, req_handlers, failure_code, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.req_handlers = req_handlers
        self.failure_code = failure_code
        
        # Use the default executor.
        self._receipt_executor = None
        
    @property
    def failure_code(self):
        return self._failure_code
        
    @failure_code.setter
    def failure_code(self, value):
        if value not in self.req_handlers:
            raise ValueError('Failure code must exist in self.req_handlers.')
        self._failure_code = value
        
    @property
    def req_handlers(self):
        return self._req_handlers
        
    @req_handlers.setter
    def req_handlers(self, value):
        ''' Does duck/type checking for setting request/response codes.
        '''
        # First make sure it's a mapping
        if not isinstance(value, collections.abc.Mapping):
            raise TypeError('Reqres codes must be a mapping.')
        # Now make sure all keys are len(2) and bytes compatible
        for code in value:
            try:
                if len(code) != 2:
                    raise ValueError()
                # Try turning it into bytes
                bytes(code)
            except (TypeError, ValueError) as e:
                raise TypeError(
                    'Reqres codes must be bytes-compatible objects of len 2.'
                ) from e
        # Finally, make sure all values are callable
        for handler in value.values():
            if not callable(handler):
                raise TypeError('All reqres code handlers must be callable.')
                
        self._req_handlers = value
        
    def new_connection(self, *args, **kwargs):
        ''' Override default implementation to return a ReqRes conn.
        '''
        return _ReqResWSConnection(*args, **kwargs)
        
    @asyncio.coroutine
    def send(self, connection, msg, req_code, expect_reply=True):
        ''' Overrides super().send to wrap with a request code and to
        pass a version to connection.send.
        '''
        # Raise now if req_code unknown; we won't be able to handle a response
        if req_code not in self.req_handlers:
            raise RequestUnknown(repr(req_code))
        # Note that the reqres_codes setter handles checking to make sure the
        # code is of length 2.
        
        # instrumentation
        print('About to send.')
        
        # Hard-coded zero version.
        return (yield from connection.send(
            msg = req_code + msg,
            version = 0,
            expect_reply = expect_reply
        ))
        
        # instrumentation
        print('Successfully sent.')
            
    @asyncio.coroutine            
    def _receipthandler(self, connection, msg):
        ''' Handles the receipt of a msg from connection without
        blocking the event loop or the receive task.
        '''
        # Pre-initialize so that any failure to assign doesn't break things
        version = None
        token = None
        req_code = None
        body = None
        
        try:
            # Pull out the version, token, body from msg
            version = int.from_bytes(
                bytes = msg[0:1], 
                byteorder = 'big', 
                signed = False
            )
            token = int.from_bytes(
                bytes = msg[1:3], 
                byteorder = 'big', 
                signed = False
            )
            req_code = msg[3:5]
            body = raw_response[5:]
            
            if req_code not in self.req_handlers:
                raise RequestUnknown(repr(req_code))
            else:
                res_handler = self.req_handlers[req_code]
                
            # If we don't raise RequestFinished, get the response body and code
            res_body, res_code = yield from self._ws_loop.run_in_executor(
                executor = self._receipt_executor,
                # Call the handler for this request code
                func = res_handler,
                # Pass it the connection and the body.
                args = (connection, token, body)
            )
            
        # This denotes the end of a req/res chain; we don't need to reply back.
        except RequestFinished as e:
            pass
                
        # Any other exception means we failed.
        except Exception as e:
            msg = yield from self.handle_autoresponder_exc(
                exc = e,
                token = token,
            )
            yield from self.send(
                connection,
                msg = msg,
                req_code = self.failure_code,
                expect_reply = False
            )
            
        # No error; formulate response.
        else:
            yield from self.send(
                connection, 
                msg = res_body, 
                req_code = res_code,
                expect_reply = False
            )
            
    def notify_response(self, connection, token, response):
        ''' Alerts a waiting sender of an available response.
        '''
        # Lookup for request token -> response
        connection.pending_responses[token] = response
        # Lookup for request token -> event
        connection.pending_requests[token].set()
            
    @asyncio.coroutine
    def autoresponder(self):
        ''' Handle all incoming messages.
        Should be called via run_coroutine_threadsafe, and then 
        (preferably) canceled when the client/server stops. However, 
        should also auto-stop when shutdown flag set.
        '''
        interrupter = asyncio.ensure_future(self._init_shutdown.wait())
        
        while not self._init_shutdown.is_set():
            consumer = asyncio.ensure_future(self.receive())
            
            finished, pending = yield from asyncio.wait(
                fs = [consumer, interrupter],
                return_when = asyncio.FIRST_COMPLETED
            )
            
            # We have exactly two tasks, so no need to iterate on them.
            # Finished is a set with exactly one item, so...
            finished = finished.pop()
            
            # If it was the producer, send it out
            if consumer is finished:
                # instrumentation
                print('Autoresponding.')
                
                # This is straight-up wrong, the method doesn't even exist
                # yield from self.handle_consumer_exc(finished.exception())
                # We could store this somewhere (with connection maybe?) but 
                # why would we? It's currently unused anyways.
                
                # instrumentation
                print('No exception.')
                
                asyncio.ensure_future(
                    self._receipthandler(connection, msg), 
                    loop = self._ws_loop
                )
                
                # instrumentation
                print('Resuming listening from autoresponse.')
                
            # Got a shutdown. Cancel the consumer.
            else:
                consumer.cancel()
        
    @asyncio.coroutine
    @abc.abstractmethod
    def handle_autoresponder_exc(self, exc, token):
        ''' Handles an exception created by the autoresponder.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        
        Must return the body of the message to reply with.
        '''
        pass
            
    @abc.abstractmethod
    def ws_run(self):
        ''' Add the autoresponder execution. MUST be called via super().
        '''
        super().ws_run()
        self._autoresponder_task = asyncio.run_coroutine_threadsafe(
            self.autoresponder(),
            loop = self._ws_loop
        )
        
        
class WSBasicServer(WSBase):
    ''' Generic websockets server.
    '''
    def __init__(self, threaded, *args, **kwargs):
        ''' 
        Note: birthdays must be > 1000, or it will be ignored, and will
        default to a 40-bit space.
        '''
        # When creating new connection ids,
        # Select a pseudorandom number from approx 40-bit space. Should have 1%
        # collision probability at 150k connections and 25% at 800k
        self._birthdays = 2 ** 40
        self._connections = {}
        self._ctr = threading.Event()
        
        # Make sure to call this last, lest we drop immediately into a thread.
        super().__init__(threaded=threaded, *args, **kwargs)
        
        # Start listening for subscription responses as soon as possible 
        # (but wait until then to return control of the thread to caller).
        if threaded:
            self._ctr.wait()
        
    @property
    def connections(self):
        ''' Access the connections dict.
        '''
        return self._connections
        
    @asyncio.coroutine
    def init_connection(self, websocket, path):
        ''' Generates a new connection object for the current conn.
        
        Must be called from super() if overridden.
        '''
        # Note that this overhead happens only once per connection.
        yield from self._admin_lock
        try:
            # Grab a connid and initialize it before releasing
            connid = self._new_connid()
            # Go ahead and set it to None so we block for absolute minimum time
            self._connections[connid] = None
        finally:
            self._admin_lock.release()
        
        connection = self.new_connection(
            loop = self._ws_loop, 
            websocket = websocket,
            path = path,
            connid = connid
        )
        self._connections[connid] = connection
        
        return connection
                
    def _new_connid(self):
        ''' Creates a new connection ID. Does not need to use CSRNG, so
        let's avoid depleting entropy.
        
        THIS IS NOT COOP SAFE! Must be called with a lock to avoid a 
        race condition. Release the lock AFTER registering the connid.
        
        Standard websockets stuff.
        '''
        # Select a pseudorandom number from approx 40-bit space. Should have 1%
        # collision probability at 150k connections and 25% at 800k
        connid = random.randint(0, self._birthdays)
        if connid in self._connections:
            connid = self._new_connid()
        return connid
    
    def ws_run(self):
        ''' Starts a LocalhostPersister server. Runs until the heat 
        death of the universe (or an interrupt is generated somehow).
        '''
        # Must be called to set local event loop when threaded.
        super().ws_run()
        
        # This is used for getting connection ID numbers.
        self._admin_lock = asyncio.Lock(loop=self._ws_loop)
        
        # Serve is a coroutine. This should happen before setting CTR
        self._ws_future = asyncio.ensure_future(
            websockets.serve(
                self._ws_connect, 
                self._ws_host, 
                self._ws_port
            ),
            loop = self._ws_loop
        )
        # Do this once the loop starts up
        # self._ws_loop.call_soon(self._ctr.set)
        self._ctr.set()
        # Go johnny go!
        server = self._ws_loop.run_until_complete(self._ws_future)
        self._ws_loop.run_until_complete(server.wait_closed())
        
        # Close down the loop. It should have stopped on its own.
        # self._ws_loop.stop()
        self._ws_loop.close()
            
        # print('XXXXXX Loop closed.')
        
        # Figure out what our exception is, if anything, and raise it
        exc = self._ws_future.exception()
        if exc is not None:
            raise exc
        
        
class WSBasicClient(WSBase):
    ''' Generic websockets client.
    
    Note that this doesn't block or anything. You're free to continue on
    in the thread where this was created, and if you don't, it will 
    close down.
    '''    
    def __init__(self, threaded, *args, **kwargs):
        super().__init__(threaded=threaded, *args, **kwargs)
        # First create a connection without a websocket. We'll add that later.
        # This seems a bit janky.
        self._connection = self.new_connection(self._ws_loop, None)
        
        # Start listening for subscription responses as soon as possible 
        # (but wait until then to return control of the thread to caller).
        if threaded:
            self._connection.cts.wait()
            
    @property
    def connection(self):
        ''' Read-only access to our connection.
        '''
        return self._connection
     
    @asyncio.coroutine
    def ws_client(self):
        ''' Client coroutine. Initiates a connection with server.
        '''
        self._websocket = yield from websockets.connect(self._ws_loc)
        # Don't forget to update the actual websocket.
        self._connection.websocket = self._websocket
        
        result = yield from self._ws_connect(self._websocket)
        
        return result
        
    @asyncio.coroutine
    def init_connection(self, websocket, path):
        ''' Returns self._connection.
        
        Must be called from super() if overridden.
        '''
        return self._connection
    
    def ws_run(self):
        ''' Starts running the listener.
        '''
        # Must be called to set local event loop when threaded.
        super().ws_run()
        
        # _ws_future is useful for blocking during halt.
        self._ws_future = asyncio.ensure_future(
            self.ws_client(), 
            loop = self._ws_loop
        )
        self._ws_loop.run_until_complete(self._ws_future)
        
        # Close down the loop. It should have stopped on its own.
        # self._ws_loop.stop()
        self._ws_loop.close()
        
        # Figure out what our exception is, if anything, and raise it
        exc = self._ws_future.exception()
        if exc is not None:
            raise exc
            
            
class WSReqResServer(WSBasicServer, ReqResWSBase):
    ''' An autoresponding request/response server for websockets.
    '''
    pass
            
            
class WSReqResClient(WSBasicClient, ReqResWSBase):
    ''' An autoresponding request/response client for websockets.
    '''
    pass