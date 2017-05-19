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
import certifi
import ssl
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
from .exceptions import ProtocolVersionError

from .utils import _BijectDict
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


try:
    SSL_VERIFICATION_CONTEXT = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
except AttributeError:
    SSL_VERIFICATION_CONTEXT = ssl.SSLContext(protocol=ssl.PROTOCOL_SSLv23)

SSL_VERIFICATION_CONTEXT.load_verify_locations(cafile=certifi.where())
SSL_VERIFICATION_CONTEXT.verify_mode = ssl.CERT_REQUIRED
SSL_VERIFICATION_CONTEXT.check_hostname = True
    

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
            
    @classmethod
    def desc_str(cls, *args, **kwargs):
        ''' Override this to explain where the connection is supposed to
        go, for use in debug scenarios.
        
        *args and **kwargs should match those used in cls.new()
        '''
        return cls.__name__ + '(*' + str(args) + ', **' + str(kwargs) + ')'
        
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
                logger.debug('CONN ' + str(self) + ' receive cancelled.')
                raise
            
            except Exception:
                logger.error(
                    'CONN ' + str(self) + ' Listener receiver ' +
                    'raised w/ traceback:\n' + ''.join(traceback.format_exc())
                )
                raise
                
    async def listen_forever(self, receiver):
        ''' Listens until the connection terminates.
        '''
        # Wait until terminate is called.
        try:
            while self:
                # Juuust in case things go south, add this to help
                # cancellation.
                await asyncio.sleep(0)
                await self.listener(receiver)
        
        # Catch this so we don't log a huge traceback on it.
        except ConnectionClosed as exc:
            logger.debug('CONN ' + str(self) + ' close message: ' + str(exc))
        
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
        
        Close MUST be idempotent.
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
    # Needed or we'll accidentally create a __dict__
    __slots__ = []
    
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
    def desc_str(cls, host, port, tls):
        ''' Override this to explain where the connection is supposed to
        go, for use in debug scenarios.
        '''
        loc = _WSLoc(host, int(port), bool(tls))
        return cls.__name__ + '(' + str(loc) + ')'
        
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
        
        try:
            server = await websockets.server.serve(
                wrapped_msg_handler,
                host,
                port,
                max_size = 10 * (2 ** 20)    # Max incoming msg size 10 MiB
            )
            
            try:
                await server.wait_closed()
            
            except Exception:
                server.close()
                await server.wait_closed()
                raise
                
        except asyncio.CancelledError:
            # Don't log being cancelled; it's expected close behavior.
            raise
            
        except Exception as exc:
            logger.error(
                'INTERNAL SERVER ERROR. Closing server w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
            logger.debug('Error args:' + str(exc.args))
        
    @classmethod
    async def new(cls, host, port, tls):
        ''' Creates and returns a new connection. Intended to be called
        by clients; servers may call __init__ directly.
        '''
        loc = _WSLoc(host, int(port), bool(tls))
        
        # If this raises, we don't need to worry about closing, because the
        # websocket won't exist.
        if tls:
            websocket = await websockets.client.connect(
                str(loc),
                ssl = SSL_VERIFICATION_CONTEXT,
                max_size = 10 * (2 ** 20)    # Max incoming msg size 10 MiB
            )
        
        else:
            websocket = await websockets.client.connect(
                str(loc),
                max_size = 10 * (2 ** 20)    # Max incoming msg size 10 MiB
            )
        
        return cls(websocket=websocket)
        
    async def close(self):
        ''' Wraps websocket.close and calls self.terminate().
        '''
        try:
            # The close() call is idempotent, but it will raise (for example) a
            # ConnectionResetError if something went screwy. So check for
            # availability first.
            if self.websocket.open:
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
                raise ConnectionClosed(
                    'code: ' + str(exc.code) + '; reason: ' + exc.reason
                ) from exc
                
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
                raise ConnectionClosed(
                    'code: ' + str(exc.code) + '; reason: ' + exc.reason
                ) from exc
                
            finally:
                self.terminate()


class WSBeatingConn(WSConnection):
    ''' A Websockets connection that overrides standard listen_forever
    to pong on a regular heartbeat interval.
    '''
    
    def __init__(self, *args, heartbeat_interval=57, **kwargs):
        super().__init__(*args, **kwargs)
        self.heartbeat_interval = heartbeat_interval
    
    async def listen_forever(self, receiver):
        ''' Override standard connection listen_forever to pong every
        self.heartbeat_interval to prevent connections from being
        artificially terminated.
        '''
        # Wait until terminate is called.
        listener = None
        try:
            while self:
                # Reuse the same listener from loop iteration to iteration
                if listener is None:
                    listener = asyncio.ensure_future(self.listener(receiver))
                
                # No matter what, we need a new heartbeat
                heartbeat = asyncio.ensure_future(
                    asyncio.sleep(self.heartbeat_interval))
                
                done, pending = await asyncio.wait(
                    fs = {listener, heartbeat},
                    return_when = asyncio.FIRST_COMPLETED
                )
                
                # The listener finished before the heartbeat
                if listener in done:
                    # We don't need this heartbeat anymore
                    heartbeat.cancel()
                    # Get the listener result / raise its exception so asyncio
                    # keeps quiet
                    listener.exception()
                    listener.result()
                    # Reset the listener to None so the next loop iteration
                    # makes a new listener
                    listener = None
                
                # We haven't gotten a message since the heartbeat_interval, so
                # we need to send a keepalive
                else:
                    logger.debug('CONN ' + str(self) + ' sending heartbeat.')
                    # Don't want asyncio to yell at us for not collecting our
                    # debts / results
                    heartbeat.result()
                    # But don't cancel the listener - we can reuse it next loop
                    # around. Instead, just send the pong.
                    await self.websocket.pong()
        
        # Catch this so we don't log a huge traceback on it.
        except ConnectionClosed as exc:
            logger.debug('CONN ' + str(self) + ' close message: ' + str(exc))
    
    
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
            logger.debug('Msgbuffer cancelling pending.')
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
            logger.debug('MsgBuffer cancelled.')
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
                 conn_close=None, *args, autoretry=True, **kwargs):
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
        # This determines whether or not we even try to reestablish the
        # connection.
        self.autoretry = autoretry
        
        # Very quick and easy way of injecting all of the handler methods into
        # self. Short of having one queue per method, we need to wrap it
        # anyways to buffer the actual method call.
        for name in msg_handler._RESPONDERS:
            async def wrap_request(*args, _method=name, **kwargs):
                ''' Pass all requests to our perform_request method.
                '''
                return (await self.perform_request(_method, args, kwargs))
            
            setattr(self, name, wrap_request)
            
    @property
    def _conn_desc(self):
        ''' Make (and cache) a description of the connection.
        '''
        conn_desc = getattr(self, '__conn_desc', None)
        
        if conn_desc is None:
            conn_desc = self.connection_cls.desc_str(*self._conn_args,
                                                     **self._conn_kwargs)
            setattr(self, '__conn_desc', conn_desc)
            
        return conn_desc
    
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
            logger.debug('Establishing connection to ' + self._conn_desc)
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
            logger.debug('Connection manager cancelled to ' + self._conn_desc)
            raise
            
        # The connection failed. Wait before reattempting it.
        except Exception as exc:
            logger.error('Failed to establish connection at ' +
                         self._conn_desc)
            logger.info('Failed connection traceback:\n' +
                        ''.join(traceback.format_exc()))
            # Do this first, because otherwise randrange errors (and also
            # otherwise it isn't technically binary exponential backoff)
            self._consecutive_attempts += 1
            backoff = random.randrange((2 ** self._consecutive_attempts) - 1)
            backoff = max(self.MIN_RETRY_DELAY, backoff)
            backoff = min(self.MAX_RETRY_DELAY, backoff)
            
            if self.autoretry:
                await asyncio.sleep(backoff)
            else:
                raise exc
            
        # We successfully connected. Awesome.
        else:
            logger.info('Connected to ' + self._conn_desc)
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
                
                logger.debug('Listening for messages at ' + self._conn_desc)
                
                # We don't expect clients to have a high enough message volume
                # to justify a buffer, so directly invoke the message handler
                await connection.listen_forever(receiver=self.protocol_def)
                
            # We want to log, but swallow-and-attempt-reconnect, errors in
            # connections.
            except ConnectionError:
                logger.warning('Connection errored: ' + self._conn_desc +
                               ' w/ traceback:\n' +
                               ''.join(traceback.format_exc()))
            
            # No matter what happens, when this dies we need to clean up the
            # connection and tell downstream that we cannot send anymore.
            finally:
                self._conn_available.clear()
                await connection.close()
                logger.info('Connection closed: ' + self._conn_desc)
                
                if self.conn_close is not None:
                    await self.conn_close(self, connection)
                    
    def __str__(self):
        ''' Make a better, compact representation of self.
        '''
        return type(self).__name__ + '(' + self._conn_desc + ')'
            
    async def perform_request(self, request_name, args, kwargs):
        ''' Make the given request using the protocol_def, but wait
        until a connection exists.
        '''
        # Wait for the connection to be available.
        method = getattr(self.protocol_def, request_name)
        await self.await_connection()
        return (await method(self._connection, *args, **kwargs))
    
    @property
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
    # Needed or we'll accidentally create a __dict__
    __slots__ = []
    
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
        
        # Always remove the token from connections, if it exists
        waiter = self._responses[connection].pop(token, None)
        
        if waiter is None:
            logger.warning(msg_id + ' request token unknown.')
            logger.debug(msg_id + ' code: ' + str(code))
            logger.debug(msg_id + ' body: ' + str(body[:50]))
        
        else:
            logger.debug(msg_id + ' waking sender...')
            await waiter.put(response)
        
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
            raise ProtocolVersionError(type(self).__name__ +
                                       ' received unsupported version: ' +
                                       str(version))
            
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
                logger.debug(req_id + ' handler cancelled.')
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
            logger.debug(req_id + ' response sending cancelled.')
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
            try:
                response, exc = await asyncio.wait_for(waiter.get(), timeout)
                
            except asyncio.TimeoutError:
                logger.warning(msg_id + ' timed out.')
                # A timeout is the only situation where we remove the waiter
                # *from the sender*. In all other cases, let the sender clean
                # up the mess.
                del self._responses[connection][token]
                raise
            
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
