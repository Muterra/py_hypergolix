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

# Note: this is used exclusively for connection ID generation in _Websocketeer
import random


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
        
        # These are our communication queues.
        self.outgoing_q = asyncio.Queue(loop=loop)
        self.incoming_q = asyncio.Queue(loop=loop)
        
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
            coro = self.outgoing_q.put(msg),
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
        
    @asyncio.coroutine
    def _await_receive(self, msg):
        ''' NON THREADSAFE wrapper to put things into the incoming 
        queue.
        '''
        return (yield from self.incoming_q.put(msg))
        
    @asyncio.coroutine
    def _pop_incoming_nowait(self):
        ''' Wrapper to call get from within the event loop.
        '''
        return self.incoming_q.get_nowait()
        
    def receive_nowait(self):
        ''' Threadsafe wrapper to immediately return the first item in
        the incoming queue. If none available, raises QueueEmpty.
        '''
        receiver = asyncio.run_coroutine_threadsafe(
            coro = self._pop_incoming_nowait(),
            loop = self._ws_loop
        )
        
        # Block on completion of coroutine and then raise any created exception
        exc = sender.exception()
        if exc:
            raise exc
            
        return receiver.result()
        
    def receive_blocking(self):
        ''' Performs a blocking synchronous call to receive the first 
        item in the incoming queue.
        '''
        receiver = asyncio.run_coroutine_threadsafe(
            coro = self.incoming_q.get(),
            loop = self._ws_loop
        )
        
        # Block on completion of coroutine and then raise any created exception
        exc = receiver.exception()
        if exc:
            raise exc
            
        return receiver.result()
        
    @asyncio.coroutine
    def receive(self):
        ''' NON THREADSAFE coroutine for waiting on an incoming message.
        '''
        return (yield from self.incoming_q.get())
        
    @asyncio.coroutine
    def receive_threadsafe(self):
        ''' Threadsafe coroutine for waiting on an incoming message. DO
        NOT CALL THIS FROM THE SAME EVENT LOOP AS THE WEBSOCKETS CLIENT!
        '''
        raise NotImplementedError(
            'Sorry, haven\'t had a chance to implement this yet and haven\'t '
            'personally had a use for it?'
        )
        

class WSBase(metaclass=abc.ABCMeta):
    ''' Common stuff for websockets clients and servers.
    '''
    def __init__(self, threaded, host, port, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._ws_port = port
        self._ws_host = host
        
        if threaded:
            self._ws_loop = asyncio.new_event_loop()
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
            # Set up a shutdown event
            self._init_shutdown = asyncio.Event(loop=self._ws_loop)
            # Declare the thread as nothing.
            self._ws_thread = None
            
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
                    yield from self.handle_listener_exc(connection, finished.exception())
                    yield from connection._await_receive(finished.result())
                
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
        
    # @asyncio.coroutine
    # @abc.abstractmethod
    # def producer(self):
    #     ''' Produces anything needed to send to the connection. Must 
    #     return bytes.
    #     '''
    #     pass
        
    # @asyncio.coroutine
    # @abc.abstractmethod
    # def consumer(self, msg):
    #     ''' Consumes the msg produced by the websockets receiver 
    #     listening to the connection.
    #     '''
    #     pass
        
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
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        pass
        
        
class Websocketeer(metaclass=abc.ABCMeta):
    ''' Generic websockets server.
    '''
    def __init__(self, port, *args, **kwargs):
        ''' 
        Note: birthdays must be > 1000, or it will be ignored, and will
        default to a 40-bit space.
        '''
        # When creating new connection ids,
        # Select a pseudorandom number from approx 40-bit space. Should have 1%
        # collision probability at 150k connections and 25% at 800k
        self._birthdays = 2 ** 40
        
        # Make sure to call this last, lest we drop immediately into a thread.
        super().__init__(*args, **kwargs)
        
        self._ws_port = port
        self._admin_lock = asyncio.Lock()
        self._shutdown = False
        self._connections = {}
        
    @property
    def connections(self):
        ''' Access the connections dict.
        '''
        return self._connections
    
    @asyncio.coroutine
    def _connection_handler(self, websocket, path):
        ''' Handles a single websocket CONNECTION. Does NOT handle a 
        single websocket exchange.
        '''
        # Note that this overhead happens only once per connection.
        yield from self._admin_lock
        try:
            # Grab a connid and initialize it before releasing
            connid = self._new_connid()
            self._connections[connid] = None
        finally:
            self._admin_lock.release()
        
        yield from self.init_connection(websocket, connid)
        
        while True:
            listener = asyncio.ensure_future(websocket.recv())
            producer = asyncio.ensure_future(self.producer(connid))
            
            finished, unfinished = yield from asyncio.wait(
                fs = [producer, listener],
                return_when = asyncio.FIRST_COMPLETED
            )
            
            # We have exactly two tasks, so no need to iterate on them.
            finished = finished.pop()
            unfinished = unfinished.pop()
            
            # Manage canceling the other task and handling exceptions
            self._log_exc(finished.exception())
            unfinished.cancel()
            
            # If it was the producer, send it out
            if producer is finished:
                yield from websocket.send(finished.result())
            # If it was the listener, consume it
            else:
                yield from self.consumer(finished.result(), connid)
                
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
                    
    def _log_exc(self, exc):
        ''' Handles any potential internal exceptions from tasks during
        a connection. If it's a ConnectionClosed, raise it to break the
        parent loop.
        
        Standard websockets stuff.
        '''
        if exc is None:
            return
        else:
            print(exc)
            if isinstance(exc, ConnectionClosed):
                raise exc
        
    @asyncio.coroutine
    def catch_interrupt(self):
        ''' Workaround for Windows not passing signals well for doing
        interrupts.
        
        Standard websockets stuff.
        '''
        while not self._shutdown:
            yield from asyncio.sleep(5)
    
    def run(self):
        ''' Starts a LocalhostPersister server. Runs until the heat 
        death of the universe (or an interrupt is generated somehow).
        
        Standard websockets stuff.
        '''
        # This is so we can support running in a separate thread
        try:
            self._loop = asyncio.get_event_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        # This is just normal stuff
        port = int(self._ws_port)
        # Serve is a coroutine.
        serve = websockets.serve(self._connection_handler, 'localhost', port)
        self._intrrptng_cow = asyncio.ensure_future(
            self.catch_interrupt(), 
            loop=self._loop
        )
        
        try:
            self._server_future = self._loop.run_until_complete(serve)
            _intrrpt_future = self._loop.run_until_complete(self._intrrptng_cow)
            # I don't think this is actually getting called, but the above is 
            # evidently still fixing the windows scheduler bug thig... Odd
            _block_on_result(_intrrpt_future)
            
        # Catch and handle errors that fully propagate
        except KeyboardInterrupt as e:
            # Note that this will still call _halt.
            return
            
        # Also call halt on exit.
        finally:
            self._halt()
            
    def _halt(self):
        ''' Standard websockets stuff.
        '''
        try:
            self._shutdown = True
            self._intrrptng_cow.cancel()
            # Restart the loop to close down the loop
            self._loop.stop()
            self._loop.run_forever()
        finally:
            self._loop.close()
        
    @asyncio.coroutine
    @abc.abstractmethod
    def init_connection(self, websocket, connid):
        ''' Does anything necessary to initialize a connection. Has 
        access to self.connections[connid], which will contain None.
        '''
        pass
        
    @asyncio.coroutine
    @abc.abstractmethod
    def producer(self, connid):
        ''' Produces anything needed to send to the connection indicated
        by connid. Must return bytes.
        '''
        pass
        
    @asyncio.coroutine
    @abc.abstractmethod
    def consumer(self, msg, connid):
        ''' Consumes the msg produced by the websockets receiver 
        listening at connid.
        '''
        pass
        
        
class Websockee(WSBase):
    ''' Generic websockets client.
    
    Note that this doesn't block or anything. You're free to continue on
    in the thread where this was created, and if you don't, it will 
    close down.
    '''    
    def __init__(self, threaded, *args, **kwargs):
        super().__init__(threaded, *args, **kwargs)
        # First create a connection without a websocket. We'll add that later.
        # This seems a bit janky.
        self._connection = _WSConnection(self._ws_loop, None)
        
        # Start listening for subscription responses as soon as possible 
        # (but wait until then to return control of the thread to caller).
        if threaded:
            self._connection.cts.wait()
     
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