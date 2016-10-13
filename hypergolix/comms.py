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
import loopa

from collections import namedtuple

# Note: this is used exclusively for connection ID generation in _Websocketeer
import random

from .exceptions import RequestError
from .exceptions import RequestFinished
from .exceptions import RequestUnknown
from .exceptions import SessionClosed

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
# Lib
# ###############################################


class _ConnectionBase(metaclass=abc.ABCMeta):
    ''' Defines common interface for all connections.
    '''
    
    def __init__(self, conn_id, *args, **kwargs):
        self.conn_id = conn_id
        # Only really used for req/res connections, buuuut... keep it here for
        # now.
        self.pending_responses = {}
        super().__init__(*args, **kwargs)
    
    @abc.abstractmethod
    async def close(self):
        ''' Performs any and all necessary connection cleanup.
        '''
        
    @abc.abstractmethod
    async def send(self, msg):
        ''' Does whatever is needed to send a message.
        '''
        
    @abc.abstractmethod
    async def recv(self):
        ''' Waits for first available message and returns it.
        '''


class _WSConnection(_ConnectionBase):
    ''' Bookkeeping object for a single websocket connection (client or
    server).
    
    This should definitely use slots, to save on server memory usage.
    
    TODO: merge this with autoresponder sessions
    '''
    
    def __init__(self, conn_id, websocket, path=None, *args, **kwargs):
        super().__init__(conn_id, *args, **kwargs)
        
        self.websocket = websocket
        self.path = path
        
    async def close(self):
        ''' Wraps websocket.close.
        '''
        await self.websocket.close()
        
    async def send(self, msg):
        ''' Send from the same event loop as the websocket.
        '''
        await self.websocket.send(msg)
        
    async def recv(self):
        ''' Receive from the same event loop as the websocket.
        '''
        return (await self.websocket.recv())
        

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
        
        
class WSBasicServer(WSBase, loopa.ManagedTask):
    ''' Generic websockets server.
    
    This isn't really a looped task, just a managed one.
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
        
    async def task_run(self):
        try:
            self._connid_lock = asyncio.Lock()
            self._server = await websockets.serve(
                self._handle_connection,
                self._ws_host,
                self._ws_port
            )
            await self._server.wait_closed()
        
        # Catch any cancellations in here.
        finally:
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
        
        
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
            await waiter.put(SessionClosed())
            
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


class RequestResponseProtocol(type):
    ''' Metaclass for defining a simple request/response protocol.
    '''
    
    def __new__(mcls, name, bases, namespace, success_code=b'AK',
                failure_code=b'NK', error_codes=tuple(), version=b''):
        ''' Modify the existing namespace to include success codes,
        failure codes, the responders, etc. Ensure every request code
        has both a requestor and a request handler.
        '''
    
        # Insert the mixin into the base classes, so that the user-defined
        # class can override stuff, but so that all of our handling stuff is
        # still defined.
        bases = (_ReqResMixin, *bases)
        
        # Check all of the request definitions for handlers and gather their
        # names
        req_defs = {}
        all_codes = {success_code, failure_code}
        for name, value in namespace.items():
            # These are requestors
            # Get the request code and handler, defaulting to None if undefined
            req_code = getattr(value, '_req_code', None)
            handler = getattr(value, 'handler_coro', None)
            
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
        cls = super().__new__(mcls, name, bases, namespace)
        
        # Add a version identifier (or whatever else it could be)
        cls._VERSION_STR = version
        cls._VERSION_LEN = len(version)
        
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
        
        
class _RequestToken(int):
    ''' Add a specific bytes representation for the int for token
    packing, and modify str() to be a fixed length.
    '''
    _PACK_LEN = 2
    __len__ = _PACK_LEN
    _MAX_VAL = (2 ** (8 * _PACK_LEN) - 1)
    # Set the string length to be that of the largest possible value
    _STR_LEN = len(str(max_val))
    
    def __init__(self, *args, respondable=False, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Enforce positive integers only
        if self < 0:
            raise ValueError('_RequestToken must be positive.')
        
        # And enforce the size limit imposed by the pack length
        elif self > self._MAX_VAL:
            raise ValueError('_RequestToken too large for pack length.')
            
        # Set up a queue for response waiting if desired... which, actually,
        # this is probably the wrong place to put this.
        if respondable:
            self._response = asyncio.Queue(maxsize=1)
        else:
            self._response = None
        
    def __bytes__(self):
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
        return cls(super().from_bytes(bytes, 'big', signed=False))
        
        
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
    
    def __call__(self, *args, **kwargs):
        ''' Get the object's wrapped_requestor, passing it the unwrapped
        request method (which needs an explicit self passing) and any
        *args and **kwargs that we were invoked with.
        '''
        return self.obj.wrap_requestor(
            self.requestor,
            self.response_handler,
            self.code,
            *args,
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
            # Make a copy of ourselves with the response_coro set!
            # Due to memoization, we don't need to rewrite the code
            return type(self)(
                request_coro = self._do_request,
                request_handler = handler_coro,
                response_handler = self._response_handler_coro
            )
            
        def response_handler(self, handler_coro):
            ''' This is called by @request_coro.response_handler as a
            decorator for the request coro.
            '''
            # Make a copy of ourselves with the response_coro set!
            # Due to memoization, we don't need to rewrite the code
            return type(self)(
                request_coro = self._do_request,
                request_handler = self._request_handler_coro,
                response_handler = handler_coro
            )
            
    # Don't forget to return the descriptor as the result of the decorator!
    return ReqResDescriptor
        
        
class _ReqResMixin:
    ''' Extends req/res protocol definitions to support calling.
    '''
    
    async def __call__(self, connection, msg):
        ''' Called for all incoming requests. Handles the request, then
        sends the response.
        '''
        # First unpack the request. Catch bad version numbers and stuff.
        try:
            code, token, body = await self.unpackit(msg)
            
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
            response = (result, None)
            
        # For failures, result=None and failure=Exception()
        elif code == self._FAILURE_CODE:
            response = (None, self._unpack_failure(body))
            
        # Handle a new request then.
        else:
            await self.handle_request(connection, code, token, body)
            # Important to avoid trying to awaken a pending response
            return
            
        # We arrive here only by way of a response (and not a request), so we
        # need to awaken the requestor.
        try:
            await connection.pending_responses[token].put(response)
        except KeyError:
            logger.warning(
                'CONN ' + str(connection) + ' REQ ' + str(token) +
                ' token unknown.'
            )
        
    async def packit(self, code, token, body):
        ''' Serialize a message.
        '''
        return self._VERSION_STR + code + bytes(token) + body
        
    async def unpackit(self, msg):
        ''' Deserialize a message.
        '''
        offset = 0
        field_lengths = [
            self._VERSION_LEN,
            self._MSG_CODE_LEN,
            len(_RequestToken)
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
            result = _pack_failure(RequestUnknown(repr(code)))
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
                result = await handler.handle(connection, body)
                response = await self.packit(self._SUCCESS_CODE, token, body)
            
            # Response attempt failed. Pack a failed response with the
            # exception instead.
            except Exception as exc:
                result = _pack_failure(exc)
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
                    response[:10]
                )
            
        # Attempt to actually send the response
        try:
            await connection.send(response)
            
        # Unsuccessful. Log the failure.
        except Exception:
            logger.error(
                req_id + ' FAILED TO SEND RESPONSE w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
        
        else:
            logger.info(
                req_id + ' successfully completed.'
            )
        
    def _pack_failure(self, exc):
        ''' Converts an exception into an error code and reply body
        '''
        try:
            error_code = self._ERROR_CODES[type(exc)]
            reply_body = str(exc).encode('utf-8')
            # For privacy/security reasons, don't pack the traceback.
            
        except KeyError:
            return b''
            
        else:
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
                    'Improper NAK body: ' + str(body) ' w/ traceback:\n' +
                    ''.join(traceback.format_exc())
                )
                result = RequestError(str(body))
            
        return result
        
    async def wrap_requestor(self, requestor, response_handler, code,
                             connection, timeout=None, *args, **kwargs):
        ''' Does anything necessary to turn a requestor into something
        that can actually perform the request. 
        '''
        # We already have the code, just need the token and body
        token = await session._gen_req_token()
        # Note the use of an explicit self!
        body = await requestor(self, connection, *args, **kwargs)
        # Pack the request
        request = await self.packit(code, token, body)
        
        # With all of that successful, create a queue for the response, send
        # the request, and then await the response.
        waiter = asyncio.Queue(maxsize=1)
        try:
            connection.pending_responses[token] = waiter
            await connection.send(request)
            
            # Wait for the response
            response, exc = await asyncio.wait_for(waiter.get(), timeout)
            
            # If a response handler was defined, use it!
            if response_handler is not None:
                return (await response_handler(response, exc))
                
            # Otherwise, make sure we have no exception, raising if we do
            elif exc is not None:
                raise exc
                
            # There's no define response handler, but the request succeeded.
            # Return the response without modification.
            else:
                return response
                
        finally:
            del waiter
            del connection.pending_responses[token]
        
        
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
                
        except SessionClosed as exc:
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
