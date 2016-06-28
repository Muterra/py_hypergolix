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
import collections
import traceback
import functools
import weakref

# Note: this is used exclusively for connection ID generation in _Websocketeer
import random

from .exceptions import RequestError
from .exceptions import RequestFinished
from .exceptions import RequestUnknown

from .utils import _BijectDict
from .utils import LooperTrooper
from .utils import run_coroutine_loopsafe
from .utils import await_sync_future
from .utils import call_coroutine_threadsafe


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


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
        self._loop = loop
        
        # This is our outgoing comms queue.
        self.outgoing_q = asyncio.Queue(loop=loop)
        
    async def close(self):
        ''' Wraps websocket.close.
        '''
        await self.websocket.close()
        
    async def send(self, msg):
        ''' NON THREADSAFE wrapper to send a message. Must be called 
        from the same event loop as the websocket.
        '''
        await self.websocket.send(msg)
        
    async def send_loopsafe(self, msg):
        ''' Loopsafe send.
        '''
        await run_coroutine_loopsafe(
            coro = self.send(msg),
            target_loop = self._loop
        )
        
    def send_threadsafe(self, msg):
        ''' Threadsafe send.
        '''
        call_coroutine_threadsafe(
            coro = self.send(msg),
            loop = self._loop
        )
        
    def __hash__(self):
        ''' Just use connid.
        This way, we can use connections as lookup objects.
        '''
        return hash(self.connid)
        

class ConnectorBase(LooperTrooper):
    ''' Common stuff for websockets clients and servers.
    
    Todo: refactor websockets stuff into a mix-in so that this class can
    be used for different transports.
    '''
    def __init__(self, host, port, receiver, *args, **kwargs):
        ''' Yeah, the usual.
        host -> str: hostname for the server
        port -> int: port for the server
        receiver -> coro: handler for incoming objects. Must have async def 
            receiver.receive(), which will be passed the connection, message
        connection_class -> type: used to create new connections. Defaults to 
            _WSConnection.
        '''
        self._ws_port = port
        self._ws_host = host
        self._receiver = receiver
            
        super().__init__(*args, **kwargs)
            
    @property
    def _ws_loc(self):
        return 'ws://' + self._ws_host + ':' + str(self._ws_port) + '/'
        
    @property
    def connection_factory(self):
        ''' Proxy for connection factory to allow saner subclassing.
        '''
        return _WSConnection
    
    async def new_connection(self, websocket, path, *args, **kwargs):
        ''' Wrapper for creating a new connection. Mostly here to make
        subclasses simpler.
        '''
        logger.debug('New connection: ' + str(args) + ' ' + str(kwargs))
        return self.connection_factory(
            loop = self._loop,
            websocket = websocket, 
            path = path, 
            *args, **kwargs
        )
        
    async def _handle_connection(self, websocket, path=None):
        ''' This handles a single websockets connection.
        '''
        connection = await self.new_connection(websocket, path)
        
        try:
            while True:
                msg = await websocket.recv()
                await self._receiver(connection, msg)
                
        except ConnectionClosed:
            pass
                    
        except Exception:
            await connection.close()
            raise
            
        else:
            await connection.close()
        
        
class WSBasicServer(ConnectorBase):
    ''' Generic websockets server.
    '''
    def __init__(self, birthday_bits=40, *args, **kwargs):
        ''' 
        Note: birthdays must be > 1000, or it will be ignored, and will
        default to a 40-bit space.
        '''
        # When creating new connection ids,
        # Select a pseudorandom number from approx 40-bit space. Should have 1%
        # collision probability at 150k connections and 25% at 800k
        self._birthdays = 2 ** birthday_bits
        self._connections = {}
        self._connid_lock = None
        self._server = None
        
        # Make sure to call this last, lest we drop immediately into a thread.
        super().__init__(*args, **kwargs)
        
    @property
    def connections(self):
        ''' Access the connections dict.
        '''
        return self._connections
        
    async def new_connection(self, websocket, path, *args, **kwargs):
        ''' Generates a new connection object for the current conn.
        
        Must be called from super() if overridden.
        '''
        # Note that this overhead happens only once per connection.
        async with self._connid_lock:
            # Grab a connid and initialize it before releasing
            connid = self._new_connid()
            # Go ahead and set it to None so we block for absolute minimum time
            self._connections[connid] = None
        
        connection = await super().new_connection(
            websocket = websocket, 
            path = path, 
            connid = connid,
            *args, **kwargs
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
        
    async def loop_init(self):
        await super().loop_init()
        self._connid_lock = asyncio.Lock()
        
    async def loop_run(self):
        self._server = await websockets.serve(
            self._handle_connection, 
            self._ws_host, 
            self._ws_port
        )
        await self._server.wait_closed()
        
    async def loop_stop(self):
        # Todo: add in logic to gracefully handle all of the connection objects
        # Note that if we error out before calling loop_run (for example, if 
        # the server fails to start), we actually don't have a server to close.
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        
        await super().loop_stop()
        
        
class WSBasicClient(ConnectorBase):
    ''' Generic websockets client.
    
    Note that this doesn't block or anything. You're free to continue on
    in the thread where this was created, and if you don't, it will 
    close down.
    '''    
    async def loop_init(self):
        self._ctx = asyncio.Event()
        await super().loop_init()
        
    async def new_connection(self, *args, **kwargs):
        ''' Wraps super().new_connection() to store it as 
        self._connection.
        '''
        connection = await super().new_connection(*args, **kwargs)
        self._connection = connection
        return connection
        
    async def loop_run(self):
        ''' Client coroutine. Initiates a connection with server.
        '''
        async with websockets.connect(self._ws_loc) as websocket:
            try:
                self._ctx.set()
                await self._handle_connection(websocket)
            except ConnectionClosed as exc:
                # For now, if the connection closes, just stop everything. We 
                # could also set it up to retry a few times or something. But 
                # for now, just close it.
                self.stop()
        
    async def send(self, msg):
        ''' NON THREADSAFE wrapper to send a message. Must be called 
        from the same event loop as the websocket.
        '''
        await self._ctx.wait()
        await self._connection.send(msg)
        
    async def send_loopsafe(self, msg):
        ''' Threadsafe wrapper to send a message from a different event
        loop.
        '''
        await run_coroutine_loopsafe(
            coro = self.send(msg), 
            target_loop = self._loop
        )
        
    def send_threadsafe(self, msg):
        ''' Threadsafe wrapper to send a message. Must be called 
        synchronously.
        '''
        call_coroutine_threadsafe(
            coro = self.send(msg), 
            loop = self._loop
        )
        
        
class _AutoresponderSession:
    ''' A request/response websockets connection.
    MUST ONLY BE CREATED INSIDE AN EVENT LOOP.
    '''
    def __init__(self):
        # Lookup for request token -> queue(maxsize=1)
        self.pending_responses = {}
        self._req_lock = asyncio.Lock()
        
    async def _gen_req_token(self):
        ''' Gets a new (well, currently unused) request token. Sets it
        in pending_responses to prevent race conditions.
        '''
        async with self._req_lock:
            token = self._gen_unused_token()
            # Do this just so we can release the lock ASAP
            self.pending_responses[token] = None
            
        return token
            
    def _gen_unused_token(self):
        ''' Recursive search for unused token. THIS IS NOT THREADSAFE
        NOR ASYNC SAFE! Must be called from within parent lock.
        '''
        # Get a random-ish (no need for CSRNG) 16-bit token
        token = random.getrandbits(16)
        if token in self.pending_responses:
            token = self._get_unused_token()
        return token
        
        
class Autoresponder(LooperTrooper):
    ''' Automated Request-Response system built on an event loop.
    
    each req_handler will be passed connection, token, body.
    
    req_handlers should be a mapping:
        key(2 bytes): awaitable
        
    the request callable should return: res body, res code tuple, OR it 
    should raise RequestFinished to denote the end of a req/res chain.
    
    Note that a request handler will never wait for a reply from its 
    response (ie, reply recursion is impossible).
    '''
    # def __init__(self, req_handlers, failure_code, *args, **kwargs):
    def __init__(self, req_handlers, success_code, failure_code, 
    error_lookup=None, *args, **kwargs):
        # # Use the default executor.
        # self._receipt_executor = None
        
        # Hard-code a version number for now
        self._version = 0
        
        # Assign the error lookup
        if error_lookup is None:
            error_lookup = {}
        if b'\x00\x00' in error_lookup:
            raise ValueError(
                'Cannot override generic error code 0x00.'
            )
        self._error_lookup = _BijectDict(error_lookup)
        
        # Set incoming (request) handlers
        self.req_handlers = req_handlers
        # self.req_handlers[success_code] = self.unpack_success
        # self.req_handlers[failure_code] = self.unpack_failure
        # Set success/failure handlers and codes
        self.response_handlers = {
            success_code: self.pack_success,
            failure_code: self.pack_failure,
        }
        self._success_code = success_code
        self._failure_code = failure_code
        
        # Call a loose LBYL type check on all handlers.
        self._check_handlers()
        
        # Lookup connection -> session
        # Use a weak key dictionary for automatic cleanup when the connections
        # have no more strong references.
        self._session_lookup = weakref.WeakKeyDictionary()
        self._connection_lookup = weakref.WeakKeyDictionary()
        self._session_lock = None
        
        # This needs to be called last, otherwise we set up the event loop too
        # early.
        super().__init__(*args, **kwargs)
        
    def _check_handlers(self):
        ''' Does duck/type checking for setting request/response codes.
        '''
        # We don't need to join req_handlers and response_handlers, because
        # response_handlers were set with the same failure/success codes.
        for code in set(self.req_handlers):
            try:
                if len(code) != 2:
                    raise ValueError()
                # Try turning it into bytes
                bytes(code)
            except (TypeError, ValueError) as exc:
                raise TypeError(
                    'Codes must be bytes-compatible objects of len 2.'
                ) from exc
        for handler in (list(self.req_handlers.values()) + 
        list(self.response_handlers.values())):
            if not callable(handler):
                raise TypeError('Handlers must be callable.')
        
    def _check_request_code(self, request_code):
        # Raise now if req_code unknown; we won't be able to handle a response
        if (request_code not in self.req_handlers and 
        request_code != self._success_code and 
        request_code != self._failure_code):
            raise RequestUnknown(repr(request_code))
        # Note that the reqres_codes setter handles checking to make sure the
        # code is of length 2.
        
    @property
    def error_lookup(self):
        ''' Error_lookup itself cannot be changed, but its contents
        absolutely may.
        '''
        return self._error_lookup
        
    def _pack_request(self, version, token, req_code, body):
        ''' Extracts version, token, request code, and body from a msg.
        '''
        if len(req_code) != 2:
            raise ValueError('Improper request code while packing request.')
        
        # Pull out the version, token, body from msg
        version = version.to_bytes(length=1, byteorder='big', signed=False)
        token = token.to_bytes(length=2, byteorder='big', signed=False)
        
        return version + token + req_code + body
        
    def _unpack_request(self, msg):
        ''' Extracts version, token, request code, and body from a msg.
        '''
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
        body = msg[5:]
        
        return version, token, req_code, body
                
    def pack_success(self, their_token, data):
        ''' Packs data into a "success" response.
        '''
        token = their_token.to_bytes(length=2, byteorder='big', signed=False)
        if not isinstance(data, bytes):
            data = bytes(data)
        return token + data
        
    def unpack_success(self, data):
        ''' Unpacks data from a "success" response.
        Note: Currently inefficient for large responses.
        '''
        token = int.from_bytes(data[0:2], byteorder='big', signed=False)
        data = data[2:]
        return token, data
        
    def pack_failure(self, their_token, exc):
        ''' Packs an exception into a "failure" response.
        
        NOTE: MUST BE CAREFUL NOT TO LEAK LOCAL INFORMATION WHEN SENDING
        ERRORS AND ERROR CODES. Sending the body of an arbitrary error
        exposes information!
        '''
        token = their_token.to_bytes(length=2, byteorder='big', signed=False)
        try:
            code = self.error_lookup[type(exc)]
            body = str(exc).encode('utf-8')
        except KeyError:
            code = b'\x00\x00'
            body = repr(exc).encode('utf-8')
        except:
            code = b'\x00\x00'
            body = b'Failure followed by exception while handling failure.'
        return token + code + body
        
    def unpack_failure(self, data):
        ''' Unpacks data from a "failure" response and raises the 
        exception that generated it (or something close to it).
        '''
        token = int.from_bytes(data[0:2], byteorder='big', signed=False)
        code = data[2:4]
        body = data[4:].decode('utf-8')
        
        try:
            exc = self.error_lookup[code]
        except KeyError:
            exc = RequestError
            
        exc = exc(body)
        return token, exc
                       
    def _get_recv_handler(self, req_code, body):
        ''' Handles the receipt of a msg from connection without
        blocking the event loop or the receive task.
        '''
        try:
            res_handler = self.req_handlers[req_code]
        except KeyError as exc:
            raise RequestUnknown(repr(req_code)) from exc
        
        return res_handler
                
    async def loop_init(self, *args, **kwargs):
        ''' Set a waiter that won't be called until the loop is closed,
        so that the receivers can spawn handlers, instead of us needing
        to dynamically add them or something silly.
        '''
        self._dumblock = asyncio.Event()
        self._session_lock = asyncio.Lock()
                
    async def loop_run(self):
        ''' Will be run ad infinitum until shutdown. Aka, will do 
        literally nothing until closed, and let receivers and senders
        spawn workers.
        '''
        await self._dumblock.wait()
        
    async def loop_stop(self):
        ''' Just for good measure, set (and then delete) the dumblock.
        '''
        self._dumblock.set()
        del self._dumblock
        
    async def receiver(self, connection, msg):
        ''' Called from the ConnectorBase to handle incoming messages.
        Note that this must run in a different event loop.
        '''
        await run_coroutine_loopsafe(
            coro = self.spawn_handler(connection, msg),
            target_loop = self._loop,
        )
        
    async def spawn_handler(self, connection, msg):
        ''' Creates a task to handle the message from the connection.
        '''
        # We can track these later. For now, just create them, and hope it all
        # works out in the end. Hahahahahaha right. What about cancellation?
        fut = asyncio.ensure_future(self.autoresponder(connection, msg))
        # Todo: consider adding contextual information, like part of (or all)
        # of the message.
        fut.add_done_callback(self._handle_autoresponder_complete)
        
    def _handle_autoresponder_complete(self, fut):
        ''' Added to the autoresponder future as a done callback to 
        handle any autoresponder exceptions.
        '''
        # For now, we just need to catch and log any exceptions.
        if fut.exception():
            exc = fut.exception()
            logger.warning(
                ('Unhandled exception while autoresponding! ' +
                repr(exc) + '\n') + 
                ''.join(traceback.format_tb(exc.__traceback__))
            )
        
    async def autoresponder(self, connection, msg):
        ''' Actually manages responding to or receiving messages from
        connections.
        
        Needs to take care of its own shit totally unsupervised, because
        it's going to be abandoned immediately by spawn_handler.
        '''
        # Just in case we fail to extract a token:
        their_token = 0
    
        # If unpacking the request raises, so be it. We'll get an unhandled
        # exception warning in asyncio. It won't harm program flow. If they are 
        # waiting for a response, it's just too bad that they didn't format the 
        # request correctly. The alternative is that we get stuck in a 
        # potentially endless loop of bugged-out sending garbage.
        
        version, their_token, req_code, body = self._unpack_request(msg)
        session = self._session_lookup[connection]

        if req_code == self._success_code:
            token, response = self.unpack_success(body)
            await self._wake_sender(session, token, response)
            
        elif req_code == self._failure_code:
            token, response = self.unpack_failure(body)
            await self._wake_sender(session, token, response)
            
        else:
            await self._handle_request(session, req_code, their_token, body)
        
    async def _handle_request(self, session, req_code, their_token, body):
        ''' Handles a request, as opposed to a "success" or "failure" 
        response.
        '''
        try:
            res_handler = self._get_recv_handler(req_code, body)
            # Note: we should probably wrap the res_handler into something
            # that tolerates no return value (probably by wrapping None 
            # into b'')
            response_msg = await res_handler(session, body)
            response = self.pack_success(
                their_token = their_token, 
                data = response_msg,
            )
            response_code = self._success_code
            
        except Exception as exc:
            # Instrumentation
            logger.info(
                'Exception while autoresponding to request: \n' + ''.join(
                traceback.format_exc())
            )
            response = self.pack_failure(their_token, exc)
            response_code = self._failure_code
            # # Should this actually raise? I don't think so?
            # raise
        
        else:
            logger.debug(
                'SUCCESS ' + str(req_code) + 
                ' FROM SESSION ' + hex(id(session)) + ' ' + 
                str(body[:10])
            )
        
        finally:
            await self.send(
                session = session,
                msg = response,
                request_code = response_code,
                await_reply = False
            )
            
    def session_factory(self):
        ''' Added for easier subclassing. Returns a session object.
        '''
        return _AutoresponderSession()
        
    @property
    def sessions(self):
        ''' Returns an iterable of all current sessions.
        '''
        return iter(self._connection_lookup)
        
    @property
    def any_session(self):
        ''' Returns an arbitrary session.
        
        Mostly useful for Autoresponders that have only one session 
        (for example, any client in a client/server setup).
        '''
        # Connection lookup maps session -> connection.
        # First treat keys like an iterable
        # Then grab the "first" of those and return it.
        return next(iter(self._connection_lookup))
        
    async def _generate_session_loopsafe(self, connection):
        ''' Loopsafe wrapper for generating sessions.
        '''
        await run_coroutine_loopsafe(
            self._generate_session(connection),
            target_loop = self._loop
        )
            
    async def _generate_session(self, connection):
        ''' Gets the session for the passed connection, or creates one
        if none exists.
        
        Might be nice to figure out a way to bypass the lock on lookup,
        and only use it for setting.
        '''
        async with self._session_lock:
            try:
                session = self._session_lookup[connection]
            except KeyError:
                session = self.session_factory()
                self._session_lookup[connection] = session
                self._connection_lookup[session] = weakref.proxy(connection)
                
        return session
        
    async def send(self, session, msg, request_code, await_reply=True):
        ''' Creates a request or response.
        
        If await_reply=True, will wait for response and then return its
            result.
        If await_reply=False, will immediately return a tuple to access the 
            result: (asyncio.Task, connection, token)
        '''
        version = self._version
        # Get a token and pack the message.
        token = await session._gen_req_token()
        
        packed_msg = self._pack_request(version, token, request_code, msg)
        
        # Create an event to signal response waiting and then put the outgoing
        # in the send queue
        session.pending_responses[token] = asyncio.Queue(maxsize=1)
            
        # Send the message
        connection = self._connection_lookup[session]
        await connection.send_loopsafe(packed_msg)
        
        # Start waiting for a response
        response_future = asyncio.ensure_future(
            self._await_response(session, token)
        )
        
        # Wait for the response if desired.
        # Note that this CANNOT be changed to return the future itself, as that
        # would break when wrapping with loopsafe or threadsafe calls.
        if await_reply:
            await asyncio.wait_for(response_future, timeout=None)
            
            if response_future.exception():
                raise response_future.exception()
            else:
                return response_future.result()
            
        else:
            return response_future
            
    async def send_loopsafe(self, session, msg, request_code, await_reply=True):
        ''' Call send, but wait in a different event loop.
        
        Note that if await_reply is False, the result will be inaccessible.
        '''
        # Problem: if await_reply is False, we'll return a Task from a 
        # different event loop. That's a problem.
        # Solution: for now, discard the future and return None.
        result = await run_coroutine_loopsafe(
            coro = self.send(session, msg, request_code, await_reply),
            target_loop = self._loop
        )
            
        if not await_reply:
            self._loop.call_soon_threadsafe(
                result.add_done_callback,
                self._cleanup_ignored_response
            )
            result = None
            
        return result
        
    def send_threadsafe(self, session, msg, request_code, await_reply=True):
        ''' Calls send, in a synchronous, threadsafe way.
        '''
        # See above re: discarding the result if not await_reply.
        result = call_coroutine_threadsafe(
            coro = self.send(session, msg, request_code, await_reply),
            loop = self._loop
        )
            
        if not await_reply:
            self._loop.call_soon_threadsafe(
                result.add_done_callback,
                self._cleanup_ignored_response
            )
            result = None
            
        return result
            
    def _cleanup_ignored_response(self, fut):
        ''' Called when loopsafe and threadsafe sends don't wait for a 
        result.
        '''
        # For now, we just need to catch and log any exceptions.
        if fut.exception():
            exc = fut.exception()
            logger.warning(
                ('Unhandled exception in ignored response! ' +
                repr(exc) + '\n'
                ) + ''.join(traceback.format_tb(exc.__traceback__))
            )
        else:
            logger.debug('Response received, but ignored.')
        
    async def _await_response(self, session, token):
        ''' Waits for a response and then cleans up the response stuff.
        '''
        try:
            response = await session.pending_responses[token].get()

            # If the response was, in fact, an exception, raise it.
            if isinstance(response, Exception):
                raise response
        
        finally:
            del session.pending_responses[token]
            
        return response
        
    async def _wake_sender(self, session, token, response):
        ''' Attempts to deliver a response to the token at session.
        '''
        try:
            waiter = session.pending_responses[token]
            await waiter.put(response)
        except KeyError:
            # Silence KeyErrors that indicate token was not being waited for.
            # No cleanup necessary, since it already doesn't exist.
            logger.info('Received an unexpected or unawaited response.')

            
def _args_normalizer(args):
    if args is None:
        return []
    else:
        return args
    
    
def _kwargs_normalizer(kwargs):
    if kwargs is None:
        return {}
    else:
        return kwargs
    
    
def _generate_threadnames(*prefixes):
    ''' Generates a matching set of unique threadnames, of the form
    prefix[0] + '-1', prefix[1] + '-1', etc.
    '''
    ctr = 0
    names = []
    
    # Get existing thread NAMES (not the threads themselves!)
    existing_threadnames = set()
    for t in threading.enumerate():
        existing_threadnames.add(t.name)
        
    while len(names) != len(prefixes):
        candidates = [prefix + '-' + str(ctr) for prefix in prefixes]
        # Check the intersection of candidates and existing names
        if len(existing_threadnames & set(candidates)) > 0:
            ctr += 1
        else:
            names.extend(candidates)
            
    return names


def Autocomms(autoresponder_class, connector_class, autoresponder_args=None, 
    autoresponder_kwargs=None, connector_args=None, connector_kwargs=None, 
    aengel=None, debug=False):
    ''' Marries an autoresponder to a Client/Server. Assigns the client/
    server as autoresponder.connector attribute, and returns the 
    autoresponder.
    
    Aengel note: stop the connector first, then the autoresponder.
    ''' 
    autoresponder_name, connector_name = \
        _generate_threadnames('autoresponder', 'connector')
        
    autoresponder_args = _args_normalizer(autoresponder_args)
    autoresponder_kwargs = _kwargs_normalizer(autoresponder_kwargs)
    connector_args = _args_normalizer(connector_args)
    connector_kwargs = _kwargs_normalizer(connector_kwargs)
    
    # Note that default behavior for LooperTrooper is to prepend the 
    # cancellation to the Aengel, so we do not need to reorder.

    autoresponder = autoresponder_class(
        threaded = True,
        debug = debug,
        thread_name = autoresponder_name,
        *autoresponder_args,
        **autoresponder_kwargs,
        aengel = aengel,
    )
    
    class LinkedConnector(connector_class):
        async def new_connection(self, *args, **kwargs):
            connection = await super().new_connection(*args, **kwargs)
            await autoresponder._generate_session_loopsafe(connection)
            return connection
    
    connector = LinkedConnector(
        receiver = autoresponder.receiver,
        threaded = True,
        debug = debug,
        thread_name = connector_name,
        aengel = aengel,
        *connector_args,
        **connector_kwargs,
    )
    autoresponder.connector = connector
    
    return autoresponder