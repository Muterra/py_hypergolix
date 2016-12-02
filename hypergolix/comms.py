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

import logging
import asyncio
import websockets
import traceback
import weakref
import base64
import loopa
# Note that time is only used for request timing
import time
# Note that random is only used for:
# 1. exponential backoff
# 2. fixturing connection recv
import random

from collections import namedtuple

# Internal deps
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api
from .hypothetical import fixture_noop
from .hypothetical import fixture_return

from .exceptions import RequestError
from .exceptions import RequestFinished
from .exceptions import RequestUnknown
from .exceptions import ConnectionClosed

from .utils import _BijectDict
from .utils import LooperTrooper
from .utils import run_coroutine_loopsafe
from .utils import await_sync_future
from .utils import call_coroutine_threadsafe
from .utils import _generate_threadnames
from .utils import ensure_equal_len


# ###############################################
# Boilerplate
# ###############################################


__all__ = [
    # 'ManagedTask',
    # 'TaskLooper',
    # 'TaskCommander',
    # 'Aengel'
]


logger = logging.getLogger(__name__)


# ###############################################
# Globals
# ###############################################


# ALL_CONNECTIONS = weakref.WeakSet()


# def close_all_connections():
#     # Iterate over ALL_CONNECTIONS until it's empty
#     while ALL_CONNECTIONS:
#         connection = ALL_CONNECTIONS.pop()
#         # Cannot call close, because the event loop won't be running.
#         connection.terminate()
        

# atexit.register(close_all_connections)


# ###############################################
# Lib
# ###############################################
        
        
class BasicServer(loopa.ManagedTask):
    ''' Generic server. This isn't really a looped task, just a managed
    one.
    '''
    
    def __init__(self, connection_cls, *args, **kwargs):
        self.connection_cls = connection_cls
        super().__init__(*args, **kwargs)
        
    async def task_run(self, *args, **kwargs):
        await self.connection_cls.serve_forever(*args, **kwargs)


class _ConnectionBase(metaclass=API):
    ''' Defines common interface for all connections.
    
    TODO: modify this such that the return is ALWAYS weakly referenced,
    but still hashable, etc. Within that proxy, wrap all ReferenceErrors
    with ConnectionClosed errors.
    '''
    
    @public_api
    def __init__(self, *args, **kwargs):
        ''' Log the creation of the connection.
        '''
        super().__init__(*args, **kwargs)
        # Create a reference to ourselves so that we can manage our own
        # lifetime through self.terminate()
        self._ref = self
        # If termination gets any more complicated, add this in so we can
        # register self.terminate in an atexit call.
        # ALL_CONNECTIONS.add(self)
        
        # Memoize our repr as the hex repr of our id(self)
        self._repr = '<' + type(self).__name__ + ' ' + hex(id(self)) + '>'
        # Memoize a urlsafe base64 string of our id(self) for str
        self._str = str(
            # Change the bytes int to base64
            base64.urlsafe_b64encode(
                # Convert our id(self) to a bytes integer equal to the maximum
                # length of ID on a 64-bit system
                id(self).to_bytes(byteorder='big', length=8)
            ),
            # And then re-cast as a native str
            'utf-8'
        )
        # And log our creation.
        logger.info('CONN ' + str(self) + ' CREATED.')
        
    @__init__.fixture
    def __init__(self, msg_iterator=None, *args, **kwargs):
        ''' Add in an isalive flag and an iterator to simulate message
        reciept.
        '''
        super(_ConnectionBase.__fixture__, self).__init__(*args, **kwargs)
        self._isalive = True
        self._msg_iterator = msg_iterator
        
    def terminate(self):
        ''' Remove our circular reference, which (assuming all other
        refs were weak) will result in our garbage collection.
        Idempotent.
        '''
        # Note: if this gets any more complicated, then we should register an
        # atext cleanup to call terminate. For now, just let GC remove it.
        try:
            del self._ref
            
        except AttributeError:
            logger.debug(
                'Attempted to terminate connection twice. Check for other ' +
                'strong references to the connection.'
            )
            
    def __bool__(self):
        ''' Return True if the connection is still active and False
        otherwise.
        '''
        return hasattr(self, '_ref')
        
    def __repr__(self):
        ''' Make a slightly nicer version of repr, since connections are
        ephemeral and we cannot reproduce them anyways.
        '''
        # Example: <_AutoresponderSession 0x52b2978>
        return self._repr
        
    def __str__(self):
        ''' Make an even more concise version of str, just giving the
        id, as a constant-length base64 string.
        '''
        return self._str
        
    async def listener(self, receiver):
        ''' Once a connection has been created, call this to listen for
        a message until the connection is terminated. Put it in a loop
        to listen forever.
        '''
        try:
            msg = await self.recv()
        
        except ConnectionClosed:
            logger.info(
                'CONN ' + str(self) + ' closed at listener.'
            )
            raise
            
        else:
            logger.debug('CONN ' + str(self) + ' message received.')
            
            try:
                # When we pass to the receiver, make sure we give them a strong
                # reference, so hashing and stuff continues to work.
                await receiver(self, msg)
                
            except asyncio.CancelledError:
                raise
            
            except Exception:
                logger.error(
                    'CONN ' + str(self) + ' Listener receiver ' +
                    'raised w/ traceback:\n' + ''.join(traceback.format_exc())
                )
                
    async def listen_forever(self, receiver):
        ''' Listens until the connection terminates.
        '''
        # Wait until terminate is called.
        while self:
            # Juuust in case things go south, add this to help cancellation.
            await asyncio.sleep(0)
            await self.listener(receiver)
        
    @classmethod
    @fixture_api
    async def serve_forever(cls, msg_handler, *args, **kwargs):
        ''' Starts a server for this kind of connection. Should handle
        its own return, and be cancellable via task cancellation.
        '''
        
    @classmethod
    @fixture_api
    async def new(cls, *args, **kwargs):
        ''' Creates and returns a new connection. Intended to be called
        by clients; servers may call __init__ directly.
        '''
        return cls(*args, **kwargs)
    
    @fixture_api
    async def close(self):
        ''' Closes the existing connection, performing any necessary
        cleanup.
        '''
        self._isalive = False
    
    @fixture_api
    async def send(self, msg):
        ''' Does whatever is needed to send a message.
        '''
        # Noop fixture.
    
    @fixture_api
    async def recv(self):
        ''' Waits for first available message and returns it.
        '''
        # Floating point [0.0, 1.0]
        scalar = random.random()
        # Wait somewhere between 0 and .1 seconds
        delay = scalar * .1
        
        if self._msg_iterator is None:
            # Figure out how long to make the message -- somewhere between
            # 32 bytes and a kilobibite, inclusive
            length = random.randrange(32, 1024)
            # Make the message itself
            msg = bytes([random.randint(0, 255) for i in range(length)])
        else:
            msg = next(self._msg_iterator)
            
        if self._isalive:
            # Wait the delay and then return
            await asyncio.sleep(delay)
            return msg
        
        else:
            raise ConnectionClosed()
        
        
class _WSLoc(namedtuple('_WSLoc', ('host', 'port', 'tls'))):
    ''' Utility class for managing websockets server locations. Provides
    unambiguous, canonical way to refer to WS targets, with a string
    representation suitable for use in websockets.connect (etc).
    '''
    
    def __str__(self):
        ''' Converts the representation into something that can be used
        to create websockets.
        '''
        if self.tls:
            ws_protocol = 'wss://'
        else:
            ws_protocol = 'ws://'
            
        return ws_protocol + self.host + ':' + str(self.port) + '/'


class WSConnection(_ConnectionBase):
    ''' Bookkeeping object for a single websocket connection (client or
    server).
    
    This should definitely use slots, to save on server memory usage.
    '''
    
    def __init__(self, websocket, path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.websocket = websocket
        self.path = path
        
    @classmethod
    async def serve_forever(cls, msg_handler, host, port, tls=False):
        ''' Starts a server for this kind of connection. Should handle
        its own return, and be cancellable via task cancellation.
        '''
        async def wrapped_msg_handler(websocket, path):
            ''' We need an intermediary that will feed the conn_handler
            actual _WSConnection objects.
            '''
            self = weakref.proxy(cls(websocket, path))
            # Make sure we don't take a strong reference to the connection!
            await self.listen_forever(msg_handler)
        
        server = await websockets.serve(
            wrapped_msg_handler,
            host,
            port,
            # max_size = 5 * (2 ** 20)    # Max incoming msg size 5 MiB
        )
        
        try:
            await server.wait_closed()
            
        except Exception:
            server.close()
            await server.wait_closed()
            raise
        
    @classmethod
    async def new(cls, host, port, tls):
        ''' Creates and returns a new connection. Intended to be called
        by clients; servers may call __init__ directly.
        '''
        loc = _WSLoc(host, int(port), bool(tls))
        # If this raises, we don't need to worry about closing, because the
        # websocket won't exist.
        websocket = await websockets.connect(
            str(loc),
            # max_size = 5 * (2 ** 20)    # Max incoming msg size 5 MiB
        )
        return cls(websocket=websocket)
        
    async def close(self):
        ''' Wraps websocket.close and calls self.terminate().
        '''
        try:
            # This call is idempotent, so we don't need to worry about
            # accidentally calling it twice.
            await self.websocket.close()
        finally:
            # And force us to be GC'd
            self.terminate()
        
    async def send(self, msg):
        ''' Send from the same event loop as the websocket.
        '''
        try:
            return (await self.websocket.send(msg))
        
        # If the connection is closed, call our own close (and therefore
        # self-destruct)
        except websockets.exceptions.ConnectionClosed as exc:
            try:
                # await self.close()
                raise ConnectionClosed() from exc
                
            finally:
                self.terminate()
        
    async def recv(self):
        ''' Receive from the same event loop as the websocket.
        '''
        try:
            return (await self.websocket.recv())
        
        # If the connection is closed, call our own close (and therefore
        # self-destruct)
        except websockets.exceptions.ConnectionClosed as exc:
            try:
                # await self.close()
                raise ConnectionClosed() from exc
                
            finally:
                self.terminate()
    
    
class MsgBuffer(loopa.TaskLooper):
    ''' Buffers incoming messages, handling them as handlers become
    available. Intended to be put between a Listener and a ProtoDef.
    
    MsgBuffers are primarily intended to be a "server"-side construct;
    it would be unexpected for requests to happen at such a rate that
    the upstream buffers fill for the requestor ("client").
    
    They can also function as the server-side equivalent of a
    ConnectionManager, in that ProtoDef requests can be queued through
    the MsgBuffer instead of the ProtoDef itself.
    '''
    
    def __init__(self, handler, *args, **kwargs):
        ''' Inits the incoming queue to avoid AttributeErrors.
        
        For now, anyways, the handler must be a protodef object.
        '''
        self._recv_q = None
        self._send_q = None
        self._handler = handler
        
        # Very quick and easy way of injecting all of the handler methods into
        # self. Short of having one queue per method, we need to wrap it
        # anyways to buffer the actual method call.
        for name in handler._RESPONDERS:
            async def wrap_request(*args, _wrapped_name=name, **kwargs):
                await self._send_q.put((_wrapped_name, args, kwargs))
            setattr(self, name, wrap_request)
        
        super().__init__(*args, **kwargs)
    
    async def __call__(self, msg):
        ''' Schedules a message to receive.
        '''
        await self._recv_q.put(msg)
    
    async def loop_init(self):
        ''' Creates the incoming queue.
        '''
        self._recv_q = asyncio.Queue()
        self._send_q = asyncio.Queue()
        
    async def loop_run(self):
        ''' Awaits the receive queue and then runs a handler for it.
        '''
        incoming = asyncio.ensure_future(self._recv_q.get())
        outgoing = asyncio.ensure_future(self._send_q.get())
        
        finished, pending = await asyncio.wait(
            fs = {incoming, outgoing},
            return_when = asyncio.FIRST_COMPLETED
        )
        
        # Immediately cancel all pending, to prevent them finishing in the
        # background and dropping things.
        for task in pending:
            task.cancel()
        
        # Technically, both of these can be finished.
        try:
            if incoming in finished:
                connection, msg = incoming.result()
                await self._handler(connection, msg)
                
            # Cannot elif because they may both finish simultaneously enough
            # to be returned together
            if outgoing in finished:
                request_name, args, kwargs = outgoing.result()
                method = getattr(self._handler, request_name)
                await method(*args, **kwargs)
                
        except asyncio.CancelledError:
            raise
        
        except Exception:
            logger.error(
                'MsgBuffer raised while handling task w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
        
    async def loop_stop(self):
        ''' Clears the incoming queue.
        '''
        self._recv_q = None
        self._send_q = None
    
    
class ConnectionManager(loopa.TaskLooper, metaclass=loopa.utils.Triplicate):
    ''' Use this client-side to automatically establish (and, whenever
    necessary, re-establish) a connection with a server. This adds
    exponential backoff for every consecutive failed connection attempt.
    
    ConnectionManagers will handle listening to the connection, and will
    dispatch any incoming requests to protocol_def. They do not buffer
    the message processing; ie, they will only handle one incoming
    message at a time. They also handle closing connections.
    
    Finally, ConnectionManagers may be used to invoke any requests that
    are defined at the msg_handler, and will automatically pass them the
    current connection.
    '''
    # Minimum delay before retrying a connection, in seconds
    MIN_RETRY_DELAY = .01
    # Maximum delay before retrying a connection, in seconds (1 hour)
    MAX_RETRY_DELAY = 3600
    
    def __init__(self, connection_cls, msg_handler, conn_init=None,
                 conn_close=None, *args, **kwargs):
        ''' We need to assign our connection class.
        
        conn_init, if defined, will be awaited (as a background task)
        every time the connection itself is created.
        
        conn_close, if defined, will be awaited (but not as a background
        task) every time the connection itself has terminated -- AFTER
        the closure.
        '''
        super().__init__(*args, **kwargs)
        
        self.connection_cls = connection_cls
        self.protocol_def = msg_handler
        self.conn_init = conn_init
        self.conn_close = conn_close
        self._connection = None
        self._conn_available = None
        self._conn_args = None
        self._conn_kwargs = None
        self._consecutive_attempts = 0
        
        # Very quick and easy way of injecting all of the handler methods into
        # self. Short of having one queue per method, we need to wrap it
        # anyways to buffer the actual method call.
        for name in msg_handler._RESPONDERS:
            async def wrap_request(*args, _method=name, **kwargs):
                ''' Pass all requests to our perform_request method.
                '''
                return (await self.perform_request(_method, args, kwargs))
            
            setattr(self, name, wrap_request)
    
    async def loop_init(self, *args, **kwargs):
        ''' *args and **kwargs will be passed to the connection class.
        '''
        self._send_q = asyncio.Queue()
        self._conn_available = asyncio.Event()
        self._conn_args = args
        self._conn_kwargs = kwargs
        self._consecutive_attempts = 0
        self._connection = None
        
    async def loop_stop(self):
        ''' Reset whether or not we have a connection available.
        '''
        self._send_q = None
        self._conn_available = None
        self._conn_args = None
        self._conn_kwargs = None
        self._connection = None
        
    async def loop_run(self):
        ''' Creates a connection and listens forever.
        '''
        # Attempt to connect.
        try:
            # Technically this violates the idea that connections should be
            # wholly self-sustained, but we want to be able to explicitly close
            # them and pass some strong references to them later. Maybe. I
            # think, anyways. Okay, not totally sure.
            connection = await self.connection_cls.new(
                *self._conn_args,
                **self._conn_kwargs
            )
            
        # Need to catch this specifically, lest we accidentally do this forever
        except asyncio.CancelledError:
            raise
            
        # The connection failed. Wait before reattempting it.
        except Exception:
            logger.error(
                'Failed to establish connection with traceback:\n' +
                ''.join(traceback.format_exc())
            )
            # Do this first, because otherwise randrange errors (and also
            # otherwise it isn't technically binary exponential backoff)
            self._consecutive_attempts += 1
            backoff = random.randrange((2 ** self._consecutive_attempts) - 1)
            backoff = max(self.MIN_RETRY_DELAY, backoff)
            backoff = min(self.MAX_RETRY_DELAY, backoff)
            await asyncio.sleep(backoff)
            
        # We successfully connected. Awesome.
        else:
            # Reset the attempts counter and set that a connection is available
            self._consecutive_attempts = 0
            # See note above re: strong/weak references
            self._connection = connection
            self._conn_available.set()
            
            try:
                # If we need to do something to establish the connection, do
                # that here. We don't need to worry about race conditions with
                # task starting, because we don't cede execution flow control
                # to the loop until we hit the next await.
                if self.conn_init is not None:
                    loopa.utils.make_background_future(
                        self.conn_init(self, connection)
                    )
                
                # We don't expect clients to have a high enough message volume
                # to justify a buffer, so directly invoke the message handler
                await connection.listen_forever(receiver=self.protocol_def)
            
            # No matter what happens, when this dies we need to clean up the
            # connection and tell downstream that we cannot send anymore.
            finally:
                self._conn_available.clear()
                await connection.close()
                
                if self.conn_close is not None:
                    await self.conn_close(self, connection)
            
    async def perform_request(self, request_name, args, kwargs):
        ''' Make the given request using the protocol_def, but wait
        until a connection exists.
        '''
        # Wait for the connection to be available.
        method = getattr(self.protocol_def, request_name)
        await self.await_connection()
        return (await method(self._connection, *args, **kwargs))
        
    def has_connection(self):
        # Hmmm
        return self._conn_available.is_set()
    
    @loopa.utils.triplicated
    async def await_connection(self):
        ''' Wait for a connection to be available.
        '''
        # There is a potential race condition here with loop_init. The quick
        # and dirty solution to it is to yield control to the event loop
        # repeatedly until it appears. Also, go ahead and add in a bit of a
        # delay for good measure.
        while self._conn_available is None:
            await asyncio.sleep(.01)
            
        await self._conn_available.wait()


class RequestResponseProtocol(type):
    ''' Metaclass for defining a simple request/response protocol.
    '''
    
    def __new__(mcls, clsname, bases, namespace, *args, success_code=b'AK',
                failure_code=b'NK', error_codes=tuple(), default_version=b'',
                **kwargs):
        ''' Modify the existing namespace to include success codes,
        failure codes, the responders, etc. Ensure every request code
        has both a requestor and a request handler.
        '''
    
        # Insert the mixin into the base classes, so that the user-defined
        # class can override stuff, but so that all of our handling stuff is
        # still defined. BUT, make sure bases doesn't already have it, in
        # either the defined bases or their parents.
        for base in bases:
            if base == _ReqResMixin:
                break
            elif issubclass(base, _ReqResMixin):
                break
        
        else:
            bases = (_ReqResMixin, *bases)
        
        # Check all of the request definitions for handlers and gather their
        # names
        req_defs = {}
        all_codes = {success_code, failure_code}
        for name, value in namespace.items():
            # These are requestors
            # Get the request code and handler, defaulting to None if undefined
            req_code = getattr(value, '_req_code', None)
            handler = getattr(value, '_request_handler_coro', None)
            
            # Ensure we've fully-defined the request system
            incomplete_def = (req_code is not None) and (handler is None)
            
            if incomplete_def:
                raise TypeError(
                    'Cannot create protocol without handler for ' +
                    str(req_code) + ' requests.'
                )
            
            # Enforce no overwriting of success code and failure code
            elif req_code in {success_code, failure_code}:
                raise ValueError(
                    'Protocols cannot overload success or failure codes.'
                )
                
            elif req_code is not None:
                # Valid request definition. Add the handler as a class attr
                req_defs[name] = req_code
                all_codes.add(req_code)
        
        # All of the request/response codes need to be the same length
        msg_code_len = ensure_equal_len(
            all_codes,
            msg = 'Inconsistent request/success/failure code lengths.'
        )
        
        # As do all of the error codes
        error_codes = _BijectDict(error_codes)
        error_code_len = ensure_equal_len(
            error_codes,
            msg = 'Inconsistent error code length.'
        )
        
        # Create the class
        cls = super().__new__(mcls, clsname, bases, namespace, *args, **kwargs)
        
        # Add a version identifier (or whatever else it could be)
        cls._VERSION_STR = default_version
        cls._VERSION_LEN = len(default_version)
        
        # Add any and all error codes as a class attr
        cls._ERROR_CODES = error_codes
        cls._ERROR_CODE_LEN = error_code_len
        
        # Add the success code and failure code
        cls._MSG_CODE_LEN = msg_code_len
        cls._SUCCESS_CODE = success_code
        cls._FAILURE_CODE = failure_code
        
        # Support bidirectional lookup for request code <--> request attr name
        cls._RESPONDERS = _BijectDict(req_defs)
        
        # Now do anything else we need to modify the thingajobber
        return cls
        
    def __init__(self, *args, success_code=b'AK', failure_code=b'NK',
                 error_codes=tuple(), default_version=b'', **kwargs):
        # Since we're doing everything in __new__, at least right now, don't
        # even bother with this.
        super().__init__(*args, **kwargs)
        
        
class RequestResponseAPI(API, RequestResponseProtocol):
    ''' Combine the metaclass for a hypothetical.API with the
    RequestResponseProtocol.
    '''
    pass
        
        
class _RequestToken(int):
    ''' Add a specific bytes representation for the int for token
    packing, and modify str() to be a fixed length.
    '''
    _PACK_LEN = 2
    _MAX_VAL = (2 ** (8 * _PACK_LEN) - 1)
    # Set the string length to be that of the largest possible value
    _STR_LEN = len(str(_MAX_VAL))
    
    def __new__(*args, **kwargs):
        # Note that the magic in int() happens in __new__ and not in __init__
        self = int.__new__(*args, **kwargs)
        
        # Enforce positive integers only
        if self < 0:
            raise ValueError('_RequestToken must be positive.')
        
        # And enforce the size limit imposed by the pack length
        elif self > self._MAX_VAL:
            raise ValueError('_RequestToken too large for pack length.')
            
        return self
        
    def __bytes__(self):
        # Wrap int.to_bytes()
        return self.to_bytes(
            length = self._PACK_LEN,
            byteorder = 'big',
            signed = False
        )
        
    def __str__(self):
        ''' Make str representation fixed-length.
        '''
        var_str = super().__str__()
        return var_str.rjust(self._STR_LEN, '0')
        
    @classmethod
    def from_bytes(cls, bytes):
        ''' Fixed-length recreation.
        '''
        plain_int = super().from_bytes(bytes, 'big', signed=False)
        return cls(plain_int)
        
        
class _BoundReq(namedtuple('_BoundReq', ('obj', 'requestor', 'request_handler',
                                         'response_handler', 'code'))):
    ''' Make the request definition callable, so that the descriptor
    __get__ can be used to directly invoke the requestor.
    
    Instantiation is also responsible for binding the object to the
    requestor or handler.
    
    self[0] == self.obj
    self[1] == self.requestor
    self[2] == self.request_handler
    self[3] == self.response_handler
    self[4] == self.code
    '''
    
    def __call__(self, connection, *args, **kwargs):
        ''' Get the object's wrapped_requestor, passing it the unwrapped
        request method (which needs an explicit self passing) and any
        *args and **kwargs that we were invoked with.
        '''
        return self.obj.wrap_requestor(
            connection,
            *args,
            requestor = self.requestor,
            response_handler = self.response_handler,
            code = self.code,
            **kwargs
        )
        
    def handle(self, *args, **kwargs):
        ''' Pass through the call to the handler and inject the bound
        object instance into the invocation thereof.
        '''
        return self.request_handler(self.obj, *args, **kwargs)
        
        
def request(code):
    ''' Decorator to dynamically create a descriptor for converting
    stuff into request codes.
    '''
    code = bytes(code)
    
    class ReqResDescriptor:
        ''' The descriptor for a request/response definition.
        '''
        
        def __init__(self, request_coro, request_handler=None,
                     response_handler=None, code=code):
            ''' Create the descriptor. This is going to be done from a
            decorator, so it will be passed the request coro.
            
            Memoize the request code first though.
            '''
            self._req_code = code
            self._request_coro = request_coro
            self._response_handler_coro = response_handler
            self._request_handler_coro = request_handler
            
        def __get__(self, obj, objtype=None):
            ''' This happens any time someone calls obj.request_coro. In
            other words, this is the action of creating a request.
            '''
            if obj is None:
                return self
            else:
                return _BoundReq(
                    obj,
                    self._request_coro,
                    self._request_handler_coro,
                    self._response_handler_coro,
                    self._req_code
                )
                
        def request_handler(self, handler_coro):
            ''' This is called by @request_coro.request_handler as a
            decorator for the response coro.
            '''
            # Modify our internal structure and return ourselves.
            self._request_handler_coro = handler_coro
            return self
            
        def response_handler(self, handler_coro):
            ''' This is called by @request_coro.response_handler as a
            decorator for the request coro.
            '''
            # Modify our internal structure and return ourselves.
            self._response_handler_coro = handler_coro
            return self
            
    # Don't forget to return the descriptor as the result of the decorator!
    return ReqResDescriptor
        
        
class _ReqResMixin:
    ''' Extends req/res protocol definitions to support calling.
    '''
    
    def __init__(self, *args, **kwargs):
        ''' Add in a weakkeydictionary to track connection responses.
        '''
        # Lookup: connection -> {token1: queue1, token2: queue2...}
        self._responses = weakref.WeakKeyDictionary()
        super().__init__(*args, **kwargs)
        
    def _ensure_responseable(self, connection):
        ''' Make sure a connection is capable of receiving a response.
        Note that this is NOT threadsafe, but it IS asyncsafe.
        '''
        if connection not in self._responses:
            self._responses[connection] = {}
    
    async def __call__(self, connection, msg):
        ''' Called for all incoming requests. Handles the request, then
        sends the response.
        '''
        # First unpack the request. Catch bad version numbers and stuff.
        try:
            code, token, body = await self.unpackit(msg)
            msg_id = 'CONN ' + str(connection) + ' REQ ' + str(token)
            
        # Log the bad request and then return, ignoring it.
        except ValueError:
            logger.error(
                'CONN ' + str(connection) + ' FAILED w/ bad version: ' +
                str(msg[:10])
            )
            return
            
        # This block dispatches the call. We handle **everything** within this
        # coroutine, so it could be an ACK or a NAK as well as a request.
        if code == self._SUCCESS_CODE:
            logger.debug(
                msg_id + ' SUCCESS received w/ partial body: ' + str(body[:10])
            )
            response = (body, None)
            
        # For failures, result=None and failure=Exception()
        elif code == self._FAILURE_CODE:
            logger.debug(
                msg_id + ' FAILURE received w/ partial body: ' + str(body[:10])
            )
            response = (None, self._unpack_failure(body))
            
        # Handle a new request then.
        else:
            logger.debug(msg_id + ' starting.')
            await self.handle_request(connection, code, token, body)
            # Important to avoid trying to awaken a pending response
            return
            
        # We arrive here only by way of a response (and not a request), so we
        # need to awaken the requestor.
        self._ensure_responseable(connection)
        try:
            await self._responses[connection][token].put(response)
        except KeyError:
            logger.warning(msg_id + ' token unknown.')
        except Exception:
            logger.error(
                msg_id + ' FAILED TO AWAKEN SENDER:\n' +
                traceback.format_exc()
            )
            raise
        else:
            logger.debug(msg_id + ' sender awoken.')
        
    async def packit(self, code, token, body):
        ''' Serialize a message.
        '''
        # Token is an actual int, so bytes()ing it tries to make that many
        # bytes instead of re-casting it (which is very inconvenient)
        return self._VERSION_STR + code + bytes(token) + body
        
    async def unpackit(self, msg):
        ''' Deserialize a message.
        '''
        offset = 0
        field_lengths = [
            self._VERSION_LEN,
            self._MSG_CODE_LEN,
            _RequestToken._PACK_LEN
        ]
        
        results = []
        for field_length in field_lengths:
            end = offset + field_length
            results.append(msg[offset:end])
            offset = end
        # Don't forget the body
        results.append(msg[offset:])
        
        # This method feels very inelegant and inefficient but, for now, meh.
        version, code, token, body = results
        
        # Raise if bad version.
        if version != self._VERSION_STR:
            raise ValueError('Incorrect version.')
            
        token = _RequestToken.from_bytes(token)
        
        return code, token, body
            
    async def handle_request(self, connection, code, token, body):
        ''' Handles an incoming request, for which we need to send a
        response.
        '''
        # Make a description of our request.
        req_id = 'CONN ' + str(connection) + ' REQ ' + str(token)
        
        # First make sure we have a responder for the sent code.
        try:
            req_code_attr = self._RESPONDERS[code]
            handler = getattr(self, req_code_attr)
        
        # No responder. Pack a failed response with RequestUnknown.
        except KeyError:
            result = self._pack_failure(RequestUnknown(repr(code)))
            logger.warning(
                req_id + ' FAILED w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
            response = await self.packit(
                self._FAILURE_CODE,
                token,
                result
            )
            
        # Have a handler...
        else:
            # Attempt response
            try:
                logger.debug(req_id + ' has handler ' + req_code_attr)
                result = await handler.handle(connection, body)
                response = await self.packit(self._SUCCESS_CODE, token, result)
                
            except asyncio.CancelledError:
                raise
            
            # Response attempt failed. Pack a failed response with the
            # exception instead.
            except Exception as exc:
                result = self._pack_failure(exc)
                logger.warning(
                    req_id + ' FAILED w/ traceback:\n' +
                    ''.join(traceback.format_exc())
                )
                response = await self.packit(
                    self._FAILURE_CODE,
                    token,
                    result
                )
                
            # Only log a success if we actually had one
            else:
                logger.info(
                    req_id + ' SUCCESSFULLY HANDLED w/ partial response ' +
                    str(response[:10])
                )
            
        # Attempt to actually send the response
        try:
            await connection.send(response)
            
        except asyncio.CancelledError:
            raise
            
        # Unsuccessful. Log the failure.
        except Exception:
            logger.error(
                req_id + ' FAILED TO SEND RESPONSE w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
        
        else:
            logger.info(
                req_id + ' completed.'
            )
        
    def _pack_failure(self, exc):
        ''' Converts an exception into an error code and reply body
        '''
        # This will get the most-specific error code available for the
        # exception. It's not perfect, but it's a good way to allow smart
        # catching and re-raising down the road.
        search_path = type(exc).mro()
        
        for cls in search_path:
            # Try assigning an error code.
            try:
                error_code = self._ERROR_CODES[cls]
                
            # No error code was found; continue to the exception's
            # next-most-specific base class
            except KeyError:
                pass
                
            # We found an error code, so stop searching.
            else:
                # For privacy/security reasons, don't pack the traceback.
                reply_body = str(exc).encode('utf-8')
                break
        
        # We searched all defined error codes and got no match. Don't
        # return an error code.
        else:
            return b''
                
        return error_code + reply_body
        
    def _unpack_failure(self, body):
        ''' Converts a failure back into an exception.
        '''
        if body == b'':
            result = RequestError()
        
        else:
            try:
                # Extract the error code and message from the body
                error_code = body[:self._ERROR_CODE_LEN]
                error_msg = str(body[self._ERROR_CODE_LEN:], 'utf-8')
                
                result = self._ERROR_CODES[error_code](error_msg)
                
            except KeyError:
                logger.warning(
                    'Improper error code in NAK body: ' + str(body)
                )
                result = RequestError(str(body))
                
            except IndexError:
                logger.warning(
                    'Improper NAK body length: ' + str(body)
                )
                result = RequestError(str(body))
                
            except Exception:
                logger.warning(
                    'Improper NAK body: ' + str(body) + ' w/ traceback:\n' +
                    ''.join(traceback.format_exc())
                )
                result = RequestError(str(body))
            
        return result
        
    async def wrap_requestor(self, connection, *args, requestor,
                             response_handler, code, timeout=None, **kwargs):
        ''' Does anything necessary to turn a requestor into something
        that can actually perform the request.
        '''
        # We already have the code, just need the token and body
        # Note that this will automatically ensure we have a self._responses
        # key, so we don't need to call _ensure_responseable later.
        token = self._new_request_token(connection)
        # Make a message ID for brevity later
        msg_id = 'CONN ' + str(connection) + ' REQ ' + str(token)
        # First log entry into wrap requestor.
        logger.info(msg_id + ' starting request ' + str(code))
        
        # Note the use of an explicit self!
        body = await requestor(self, connection, *args, **kwargs)
        # Pack the request
        request = await self.packit(code, token, body)
        
        # With all of that successful, create a queue for the response, send
        # the request, and then await the response.
        waiter = asyncio.Queue(maxsize=1)
        try:
            self._responses[connection][token] = waiter
            # For diagnostic purposes (and because it's negligently expensive),
            # time the duration of the request.
            start = time.monotonic()
            await connection.send(request)
            logger.debug(msg_id + ' sent. Awaiting response.')
            
            # Wait for the response
            response, exc = await asyncio.wait_for(waiter.get(), timeout)
            end = time.monotonic()
            
            logger.info(
                'CONN {!s} REQ {!s} response took {:.3f} seconds.'.format(
                    connection, token, end - start
                )
            )
            
            # If a response handler was defined, use it!
            if response_handler is not None:
                logger.debug(
                    'CONN {!s} REQ {!s} response using BYOB handler.'.format(
                        connection, token
                    )
                )
                # Again, note use of explicit self.
                return (
                    await response_handler(
                        self,
                        connection,
                        response,
                        exc
                    )
                )
                
            # Otherwise, make sure we have no exception, raising if we do
            elif exc is not None:
                raise exc
                
            # There's no define response handler, but the request succeeded.
            # Return the response without modification.
            else:
                logger.debug(
                    'CONN {!s} REQ {!s} response using stock handler.'.format(
                        connection, token
                    )
                )
                return response
                
        finally:
            # Log exit from wrap requestor.
            logger.info(msg_id + ' exiting request ' + str(code))
            del waiter
            del self._responses[connection][token]
            
    def _new_request_token(self, connection):
        ''' Generates a request token for the connection.
        '''
        self._ensure_responseable(connection)
        # Get a random-ish (no need for CSRNG) 16-bit token
        token = random.getrandbits(16)
        # Repeat until unique
        while token in self._responses[connection]:
            token = random.getrandbits(16)
        token = _RequestToken(token)
        # Now create an empty entry in the _responses entry (to avoid a race
        # condition) and return the token
        self._responses[connection][token] = None
        return token
        
        
########################################################################
# THIS IS ALL OF THE OLD STUFF!!!
########################################################################
        
        
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
        self._has_session = None
        
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
        exc_msg = ''.join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        str_token = str(their_token)
        token = their_token.to_bytes(length=2, byteorder='big', signed=False)
        
        logger.info(
            'REQUEST FAILED:    ' + str_token +
            ' Packing exception:\n' + exc_msg
        )
        
        try:
            code = self.error_lookup[type(exc)]
            body = exc_msg.encode('utf-8')
            
        except KeyError:
            code = b'\x00\x00'
            body = exc_msg.encode('utf-8')
            logger.debug(
                'Request ' + str_token + ' has no matching response code ' +
                'for exception.'
            )
            
        except Exception:
            exc2_msg = ''.join(traceback.format_exc())
            code = b'\x00\x00'
            body = (exc_msg + '\n\nFOLLOWED BY\n\n' + exc2_msg).encode('utf-8')
            logger.debug(
                'Request ' + str_token + ' failed to pack exc w/traceback:\n' +
                exc_msg + '\n\nFOLLOWED BY\n\n' + exc2_msg
            )
            
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
        self._has_session = asyncio.Event()
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
            
        except asyncio.CancelledError:
            raise
            
        except Exception as exc:
            # Instrumentation
            logger.info(
                'FAILED ' + str(req_code) + ' FROM SESSION ' +
                hex(id(session)) + ' w/ traceback\n' +
                ''.join(traceback.format_exc())
            )
            response = self.pack_failure(their_token, exc)
            response_code = self._failure_code
            # # Should this actually raise? I don't think so?
            # raise
        
        else:
            logger.debug(
                'SUCCESS ' + str(req_code) +
                ' FROM SESSION ' + hex(id(session)) + ' ' +
                str(body[:25])
            )
        
        finally:
            await self.send(
                session = session,
                msg = response,
                request_code = response_code,
                # We're smart enough to know not to wait for a reply on ack/nak
                # await_reply = False
            )
        
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
        
        # Start waiting for a response if it isn't an ack or nak
        is_acknak = request_code == self._success_code
        is_acknak |= request_code == self._failure_code
        if not is_acknak:
            response_future = asyncio.ensure_future(
                self._await_response(session, token)
            )
        
            # Wait for the response if desired.
            # Note that this CANNOT be changed to return the future itself, as 
            # that would break when wrapping with loopsafe or threadsafe calls.
            if await_reply:
                await asyncio.wait_for(response_future, timeout=None)
                
                if response_future.exception():
                    raise response_future.exception()
                else:
                    return response_future.result()
                
            else:
                logger.debug(
                    'Response unawaited; returning future for ' + repr(session) + 
                    ' request: ' + str(request_code) + ' ' + str(msg[:10])
                )
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
                
        except ConnectionClosed as exc:
            logger.debug(
                'Session closed while awaiting response: \n' + ''.join(
                traceback.format_exc())
            )
        
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
        try:
            return next(iter(self._connection_lookup))
        except StopIteration as exc:
            raise RuntimeError(
                'No session available. Must first connect to client/server.'
            ) from exc
        
    async def await_session_async(self):
        ''' Waits for a session to be available.
        '''
        await self._has_session.wait()
        
    async def await_session_loopsafe(self):
        ''' Waits for a session to be available (loopsafe).
        '''
        await run_coroutine_loopsafe(
            coro = self.await_session_async(),
            target_loop = self._loop,
        )
        
    def await_session_threadsafe(self):
        ''' Waits for a session to be available (threadsafe)
        '''
        return call_coroutine_threadsafe(
            coro = self.await_session_async(),
            loop = self._loop,
        )
        
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
                logger.debug(
                    'Connection missing in session lookup: ' + str(connection)
                )
                session = self.session_factory()
                self._session_lookup[connection] = session
                self._connection_lookup[session] = weakref.proxy(connection)
                
        # Release anyone waiting for a session
        self._has_session.set()
                
        return session
        
    async def _close_session_loopsafe(self, connection):
        ''' Loopsafe wrapper for closing sessions.
        '''
        await run_coroutine_loopsafe(
            self._close_session(connection),
            target_loop = self._loop
        )
        
    def _close_session_threadsafe(self, connection):
        ''' Threadsafe wrapper for closing sessions.
        '''
        call_coroutine_threadsafe(
            coro = self._close_session(connection),
            loop = self._loop
        )
        
    async def _close_session(self, connection):
        ''' Loopsafe wrapper for closing sessions.
        '''
        try:
            session = self._session_lookup[connection]
            del self._session_lookup[connection]
            del self._connection_lookup[session]
            await session.close()
        except KeyError as exc:
            logger.error(
                'KeyError while closing session:\n' +
                ''.join(traceback.format_exc())
            )

            
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
    
    
class AutoresponseConnector:
    ''' Connects an autoresponder to a connector. Use it by subclassing
    both the connector and this class, for example:
    
    class WSAutoServer(AutoresponseConnector, WSBasicServer):
        pass
    '''
    def __init__(self, autoresponder, *args, **kwargs):
        # I am actually slightly concerned about future name collisions here
        self.__autoresponder = autoresponder
        super().__init__(receiver=autoresponder.receiver, *args, **kwargs)
        
    async def new_connection(self, *args, **kwargs):
        ''' Bridge the connection with a newly created session.
        '''
        connection = await super().new_connection(*args, **kwargs)
        await self.__autoresponder._generate_session_loopsafe(connection)
        return connection
    
    async def _handle_connection(self, *args, **kwargs):
        ''' Close the session with the connection.
        '''
        connection = await super()._handle_connection(*args, **kwargs)
        await self.__autoresponder._close_session_loopsafe(connection)


def Autocomms(autoresponder_class, connector_class, autoresponder_args=None, 
    autoresponder_kwargs=None, autoresponder_name=None, connector_args=None, 
    connector_kwargs=None, connector_name=None, aengel=None, debug=False):
    ''' Marries an autoresponder to a Client/Server. Assigns the client/
    server as autoresponder.connector attribute, and returns the 
    autoresponder.
    
    Aengel note: stop the connector first, then the autoresponder.
    ''' 
    if autoresponder_name is None:
        autoresponder_name = 'auto:re'
    if connector_name is None:
        connector_name = 'connect'
    autoresponder_name, connector_name = \
        _generate_threadnames(autoresponder_name, connector_name)
        
    autoresponder_args = _args_normalizer(autoresponder_args)
    autoresponder_kwargs = _kwargs_normalizer(autoresponder_kwargs)
    connector_args = _args_normalizer(connector_args)
    connector_kwargs = _kwargs_normalizer(connector_kwargs)
    
    # Note that default behavior for LooperTrooper is to prepend the 
    # cancellation to the Aengel, so we do not need to reorder.

    autoresponder = autoresponder_class(
        *autoresponder_args,
        **autoresponder_kwargs,
        threaded = True,
        debug = debug,
        thread_name = autoresponder_name,
        aengel = aengel,
    )
    
    class LinkedConnector(connector_class):
        def stop(self):
            autoresponder.stop_threadsafe_nowait()
            super().stop()
        
        async def new_connection(self, *args, **kwargs):
            ''' Bridge the connection with a newly created session.
            '''
            connection = await super().new_connection(*args, **kwargs)
            await autoresponder._generate_session_loopsafe(connection)
            return connection
        
        async def _handle_connection(self, *args, **kwargs):
            ''' Close the session with the connection.
            '''
            connection = await super()._handle_connection(*args, **kwargs)
            await autoresponder._close_session_loopsafe(connection)
    
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
        

class WSBase:
    ''' Common stuff for websockets clients and servers.
    '''
    
    def __init__(self, host, port, receiver, tls=True, *args, **kwargs):
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
        
        if tls:
            self._ws_protocol = 'wss://'
        else:
            self._ws_protocol = 'ws://'
        
        self._ws_tls = bool(tls)
        self._receiver = receiver
            
        super().__init__(*args, **kwargs)
            
    @property
    def _ws_loc(self):
        return (
            self._ws_protocol +
            self._ws_host + ':' +
            str(self._ws_port) + '/'
        )
        
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
                await asyncio.sleep(0)
                msg = await websocket.recv()
                # Should this be split off into own looper so we can get on
                # with receiving?
                await self._receiver(connection, msg)
                
        except ConnectionClosed:
            logger.info('Connection closed: ' + str(connection.connid))
                    
        except Exception:
            logger.error(
                'Error running connection! Cleanup not called.\n' +
                ''.join(traceback.format_exc())
            )
            raise
            
        finally:
            await connection.close()
            
        # In case subclasses want to do any cleanup
        return connection
        
        
class WSBasicClient(WSBase, loopa.TaskLooper):
    ''' Generic websockets client.
    
    Note that this doesn't block or anything. You're free to continue on
    in the thread where this was created, and if you don't, it will
    close down.
    '''
    
    def __init__(self, threaded, *args, **kwargs):
        super().__init__(threaded=threaded, *args, **kwargs)
        
        self._retry_counter = 0
        self._retry_max = 60
        
        # If we're threaded, we need to wait for the clear to transmit flag
        if threaded:
            call_coroutine_threadsafe(
                coro = self._ctx.wait(),
               loop = self._loop,
            )
    
    async def loop_init(self):
        self._ctx = asyncio.Event()
        await super().loop_init()
        
    async def loop_stop(self):
        self._ctx = None
        await super().loop_stop()
        
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
                self._loop.call_soon(self._ctx.set)
                await self._handle_connection(websocket)
                
            except ConnectionClosed as exc:
                logger.warning('Connection closed!')
                # For now, if the connection closes, just dumbly retry
                self._retry_counter += 1
                
                if self._retry_counter <= self._retry_max:
                    await asyncio.sleep(.1)
                    
                else:
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
    
    TODO: merge with connections
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
            token = self._gen_unused_token()
        return token
        
    async def close(self):
        ''' Perform any needed cleanup.
        '''
        # Awaken any further pending responses that they're doomed
        for waiter in self.pending_responses.values():
            await waiter.put(ConnectionClosed())
            
    def __repr__(self):
        # Example: <_AutoresponderSession 0x52b2978>
        clsname = type(self).__name__
        sess_str = str(hex(id(self)))
        return '<' + clsname + ' ' + sess_str + '>'
        
    def __str__(self):
        # Example: _AutoresponderSession(0x52b2978)
        clsname = type(self).__name__
        sess_str = str(hex(id(self)))
        return clsname + '(' + sess_str + ')'
