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

NakError status code conventions:
-----
0x0001: Does not appear to be a Golix object.
0x0002: Failed to verify.
0x0003: Unknown or invalid author or recipient.
0x0004: Unbound GEOC; immediately garbage collected
0x0005: Existing debinding for address; (de)binding rejected.
0x0006: Invalid or unknown target.
0x0007: Inconsistent author.
0x0008: Object does not exist at persistence provider.
0x0009: Attempt to upload illegal frame for dynamic binding. Indicates 
        uploading a new dynamic binding without the root binding, or that
        the uploaded frame does not contain any existing frames in its 
        history.
'''

# Control * imports.
__all__ = [
    'MemoryPersister', 
]

# Global dependencies
import abc
import collections
import warnings
import functools
import struct
import weakref
import queue
import pathlib
import base64

import asyncio
import websockets
from websockets.exceptions import ConnectionClosed
    
# Not sure if these are being used
import time
import random
import string
import threading
import traceback
# End unsure block

from golix import ThirdParty
from golix import SecondParty
from golix import Ghid
from golix import Secret
from golix import ParseError
from golix import SecurityError

from golix.utils import generate_ghidlist_parser

from golix._getlow import GIDC
from golix._getlow import GEOC
from golix._getlow import GOBS
from golix._getlow import GOBD
from golix._getlow import GDXX
from golix._getlow import GARQ

# Local dependencies
from .exceptions import NakError
from .exceptions import MalformedGolixPrimitive
from .exceptions import VerificationFailure
from .exceptions import UnboundContainer
from .exceptions import InvalidIdentity
from .exceptions import DoesNotExist
from .exceptions import AlreadyDebound
from .exceptions import InvalidTarget
from .exceptions import PersistenceWarning
from .exceptions import RequestError
from .exceptions import InconsistentAuthor
from .exceptions import IllegalDynamicFrame

from .utils import _DeepDeleteChainMap
from .utils import _WeldedSetDeepChainMap
from .utils import _block_on_result
from .utils import _JitSetDict
from .utils import TruthyLock
from .utils import SetMap

# from .comms import WSAutoServer
# from .comms import WSAutoClient
from .comms import _AutoresponderSession
from .comms import Autoresponder


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Library
# ###############################################


ERROR_CODES = {
    b'\xFF\xFF': NakError,
    b'\x00\x01': MalformedGolixPrimitive,
    b'\x00\x02': VerificationFailure,
    b'\x00\x03': InvalidIdentity,
    b'\x00\x04': UnboundContainer,
    b'\x00\x05': AlreadyDebound,
    b'\x00\x06': InvalidTarget,
    b'\x00\x07': InconsistentAuthor,
    b'\x00\x08': DoesNotExist,
    b'\x00\x09': IllegalDynamicFrame,
}


class _PersisterBase(metaclass=abc.ABCMeta):
    ''' Base class for persistence providers.
    '''    
    @abc.abstractmethod
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        Note that this is going to (unfortunately) result in packing + 
        unpacking the object twice for ex. a MemoryPersister. At some 
        point, that should be fixed -- maybe through ex. publish_unsafe?
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def subscribe(self, ghid, callback):
        ''' Request that the persistence provider update the client on
        any changes to the object addressed by ghid. Must target either:
        
        1. Dynamic ghid
        2. Author identity ghid
        
        Upon successful subscription, the persistence provider will 
        publish to client either of the above:
        
        1. New frames to a dynamic binding
        2. Asymmetric requests with the indicated GHID as a recipient
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed ghid at the persistence provider. Removes only the
        passed callback.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected 
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def list_bindings(self, ghid):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def list_debindings(self, ghid):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise NakError
        '''
        pass
        
    @abc.abstractmethod
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def disconnect(self):
        ''' Terminates all subscriptions and requests. Not required for
        a disconnect, but highly recommended, and prevents an window of
        attack for address spoofers. Note that such an attack would only
        leak metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass


class MemoryPersister:
    ''' Basic in-memory persister.
    '''    
    def __init__(self):
        (core, doorman, enforcer, lawyer, bookie, librarian, undertaker, 
            postman) = circus_factory()
        self.core = core
        self.doorman = doorman
        self.enforcer = enforcer
        self.lawyer = lawyer
        self.bookie = bookie
        self.librarian = librarian
        self.undertaker = undertaker
        self.postman = postman
        
        self.subscribe = self.postman.subscribe
        self.unsubscribe = self.postman.unsubscribe
        # self.publish = self.core.ingest
        self.list_bindings = self.bookie.bind_status
        self.list_debindings = self.bookie.debind_status
        
    def publish(self, *args, **kwargs):
        # This is a temporary fix to force memorypersisters to notify during
        # publishing. Ideally, this would happen immediately after returning.
        self.core.ingest(*args, **kwargs)
        self.postman.do_mail_run()
        
    def ping(self):
        ''' Queries the persistence provider for availability.
        '''
        return True
        
    def get(self, ghid):
        ''' Returns a packed Golix object.
        '''
        try:
            return self.librarian.dereference(ghid)
        except KeyError as exc:
            raise DoesNotExist('0x0008: Ghid not found at persister.') from exc
        
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected 
        client.
        '''
        # TODO: figure out what to do instead of this
        return tuple(self.postman._listeners)
            
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise NakError
        '''
        if ghid in self.librarian:
            return True
        else:
            return False
        
    def disconnect(self):
        ''' Terminates all subscriptions. Not required for a disconnect, 
        but highly recommended, and prevents an window of attack for 
        address spoofers. Note that such an attack would only leak 
        metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        # TODO: figure out something different to do here.
        self.postman._listeners = {}
        return True
        
        
class DiskCachePersister(MemoryPersister):
    ''' Persister that caches to disk.
    '''    
    def __init__(self, cache_dir):
        (core, doorman, enforcer, lawyer, bookie, librarian, undertaker, 
            postman) = circus_factory(
                librarian_class = DiskLibrarian,
                librarian_kwargs = {'cache_dir': cache_dir}
            )
        self.core = core
        self.doorman = doorman
        self.enforcer = enforcer
        self.lawyer = lawyer
        self.bookie = bookie
        self.librarian = librarian
        self.undertaker = undertaker
        self.postman = postman
        
        self.subscribe = self.postman.subscribe
        self.unsubscribe = self.postman.unsubscribe
        # self.publish = self.core.ingest
        self.list_bindings = self.bookie.bind_status
        self.list_debindings = self.bookie.debind_status


class _PersisterBridgeSession(_AutoresponderSession):
    def __init__(self, transport, *args, **kwargs):
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(transport, weakref.ProxyTypes):
            self._transport = transport
        else:
            self._transport = weakref.proxy(transport)
            
        self._subscriptions = {}
        self._processing = asyncio.Event()
        
        super().__init__(*args, **kwargs)
    
    def send_subs_update(self, subscribed_ghid, notification_ghid):
        ''' Send the connection its subscription update.
        Note that this is going to be called from within an event loop,
        but not asynchronously (no await).
        
        TODO: make persisters async.
        '''
        asyncio.ensure_future(
            self.send_subs_update_ax(subscribed_ghid, notification_ghid)
        )
        # asyncio.run_coroutine_threadsafe(
        #     coro = self.send_subs_update_ax(subscribed_ghid, notification_ghid)
        #     loop = self._transport._loop
        # )
            
    async def send_subs_update_ax(self, subscribed_ghid, notification_ghid):
        ''' Deliver any subscription updates.
        
        Also, temporary workaround for not re-delivering updates for 
        objects we just sent up.
        '''
        if not self._processing.is_set():
            await self._transport.send(
                session = self,
                msg = bytes(subscribed_ghid) + bytes(notification_ghid),
                request_code = self._transport.REQUEST_CODES['send_subs_update'],
                # Note: for now, just don't worry about failures.
                await_reply = False
            )
        # await self._transport.send(
        #     session = self,
        #     msg = bytes(subscribed_ghid) + bytes(notification_ghid),
        #     request_code = self._transport.REQUEST_CODES['send_subs_update'],
        #     # Note: for now, just don't worry about failures.
        #     await_reply = False
        # )


class PersisterBridgeServer(Autoresponder):
    ''' Serialization mixins for persister bridges.
    '''
    REQUEST_CODES = {
        # Receive an update for an existing object.
        'send_subs_update': b'!!',
    }
    
    def __init__(self, persister, *args, **kwargs):
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(persister, weakref.ProxyTypes):
            self._persister = persister
        else:
            self._persister = weakref.proxy(persister)
            
        req_handlers = {
            # ping 
            b'??': self.ping_wrapper,
            # publish 
            b'PB': self.publish_wrapper,
            # get  
            b'GT': self.get_wrapper,
            # subscribe 
            b'+S': self.subscribe_wrapper,
            # unsubscribe 
            b'xS': self.unsubscribe_wrapper,
            # list subs 
            b'LS': self.list_subs_wrapper,
            # list binds 
            b'LB': self.list_bindings_wrapper,
            # list debindings
            b'LD': self.list_debindings_wrapper,
            # query (existence) 
            b'QE': self.query_wrapper,
            # disconnect 
            b'XX': self.disconnect_wrapper,
        }
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            error_lookup = ERROR_CODES,
            *args, **kwargs
        )
            
    def session_factory(self):
        ''' Added for easier subclassing. Returns the session class.
        '''
        logger.debug('Session created.')
        return _PersisterBridgeSession(
            transport = self,
        )
            
    async def ping_wrapper(self, session, request_body):
        ''' Deserializes a ping request; forwards it to the persister.
        '''
        try:
            if self._persister.ping():
                return b'\x01'
            else:
                return b'\x00'
                
                logger.warning(
                'Exception while autoresponding to request: \n' + ''.join(
                traceback.format_exc())
            )
        
        except Exception as exc:
            msg = ('Error while receiving ping.\n' + repr(exc) + '\n' + 
                ''.join(traceback.format_exc()))
            logger.error(msg)
            # return b'\x00'
            raise
            
    async def publish_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        session._processing.set()
        self._persister.publish(request_body)
        session._processing.clear()
        return b'\x01'
        # obj = self._persister.ingest(request_body)
        # self.schedule_ignore_update(obj.ghid, session)
        # ensure_future(do_future_subs_update())
            
    async def get_wrapper(self, session, request_body):
        ''' Deserializes a get request; forwards it to the persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        return self._persister.get(ghid)
            
    async def subscribe_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        
        def updater(subscribed_ghid, notification_ghid, 
                    call=session.send_subs_update):
            call(subscribed_ghid, notification_ghid)
        
        self._persister.subscribe(ghid, updater)
        session._subscriptions[ghid] = updater
        return b'\x01'
            
    async def unsubscribe_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        callback = session._subscriptions[ghid]
        unsubbed = self._persister.unsubscribe(ghid, callback)
        del session._subscriptions[ghid]
        if unsubbed:
            return b'\x01'
        else:
            return b'\x00'
            
    async def list_subs_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghidlist = list(session._subscriptions)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def list_bindings_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        ghidlist = self._persister.list_bindings(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def list_debindings_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        ghidlist = self._persister.list_debindings(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def query_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        status = self._persister.query(ghid)
        if status:
            return b'\x01'
        else:
            return b'\x00'
            
    async def disconnect_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        for sub_ghid, sub_callback in session._subscriptions.items():
            self._persister.unsubscribe(sub_ghid, sub_callback)
        session._subscriptions.clear()
        return b'\x01'
        
        
class PersisterBridgeClient(Autoresponder, _PersisterBase):
    ''' Websockets request/response persister (the client half).
    '''
            
    REQUEST_CODES = {
        # ping 
        'ping': b'??',
        # publish 
        'publish': b'PB',
        # get  
        'get': b'GT',
        # subscribe 
        'subscribe': b'+S',
        # unsubscribe 
        'unsubscribe': b'xS',
        # list subs 
        'list_subs': b'LS',
        # list binds 
        'list_bindings': b'LB',
        # list debindings
        'list_debindings': b'LD',
        # query (existence) 
        'query': b'QE',
        # disconnect 
        'disconnect': b'XX',
    }
    
    def __init__(self, *args, **kwargs):
        # Note that these are only for unsolicited contact from the server.
        req_handlers = {
            # Receive/dispatch a new object.
            b'!!': self.deliver_update_wrapper,
        }
        
        self._subscriptions = _JitSetDict()
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            error_lookup = ERROR_CODES,
            *args, **kwargs
        )
        
    async def deliver_update_wrapper(self, session, response_body):
        ''' Handles update pings.
        '''
        # # Shit, I have a race condition somewhere.
        # time.sleep(.01)
        subscribed_ghid = Ghid.from_bytes(response_body[0:65])
        notification_ghid = Ghid.from_bytes(response_body[65:130])
        
        # for callback in self._subscriptions[subscribed_ghid]:
        #     callback(notification_ghid)
                
        # Well this is a huge hack. But something about the event loop 
        # itself is fucking up control flow and causing the world to hang
        # here. May want to create a dedicated thread just for pushing 
        # updates? Like autoresponder, but autoupdater?
        # TODO: fix this gross mess
            
        def run_callbacks(subscribed_ghid, notification_ghid):
            for callback in self._subscriptions[subscribed_ghid]:
                callback(subscribed_ghid, notification_ghid)
        
        worker = threading.Thread(
            target = run_callbacks,
            daemon = True,
            args = (subscribed_ghid, notification_ghid),
        )
        worker.start()
        
        return b'\x01'
    
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['ping']
        )
        
        if response == b'\x01':
            return True
        else:
            return False
    
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        Note that this is going to (unfortunately) result in packing + 
        unpacking the object twice for ex. a MemoryPersister. At some 
        point, that should be fixed -- maybe through ex. publish_unsafe?
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = packed,
            request_code = self.REQUEST_CODES['publish']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown response code while publishing object.')
    
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['get']
        )
            
        return response
    
    def subscribe(self, ghid, callback):
        ''' Request that the persistence provider update the client on
        any changes to the object addressed by ghid. Must target either:
        
        1. Dynamic ghid
        2. Author identity ghid
        
        Upon successful subscription, the persistence provider will 
        publish to client either of the above:
        
        1. New frames to a dynamic binding
        2. Asymmetric requests with the indicated GHID as a recipient
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        if ghid not in self._subscriptions:
            response = self.send_threadsafe(
                session = self.any_session,
                msg = bytes(ghid),
                request_code = self.REQUEST_CODES['subscribe']
            )
            
            if response != b'\x01':
                raise RuntimeError('Unknown response code while subscribing.')
                
        self._subscriptions[ghid].add(callback)
        return True
    
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed ghid at the persistence provider. Removes only the
        passed callback.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        if ghid not in self._subscriptions:
            raise ValueError('Not currently subscribed to ghid.')
            
        self._subscriptions[ghid].discard(callback)
        
        if len(self._subscriptions[ghid]) == 0:
            del self._subscriptions[ghid]
            
            response = self.send_threadsafe(
                session = self.any_session,
                msg = bytes(ghid),
                request_code = self.REQUEST_CODES['unsubscribe']
            )
        
            if response == b'\x01':
                # There was a subscription, and it was successfully removed.
                pass
            elif response == b'\x00':
                # This means there was no subscription to remove.
                pass
            else:
                raise RuntimeError(
                    'Unknown response code while unsubscribing from address. ' 
                    'The persister might still send updates, but the callback '
                    'has been removed.'
                )
                
        return True
    
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected 
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        # This would probably be a good time to reconcile states between the
        # persistence provider and our local set of subs!
        response = self.send_threadsafe(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['list_subs']
        )
        
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
    
    def list_bindings(self, ghid):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['list_bindings']
        )
        
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
    
    def list_debindings(self, ghid):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['list_debindings']
        )
        
        parser = generate_ghidlist_parser()
        return parser.unpack(response)
        
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['query']
        )
        
        if response == b'\x00':
            return False
        else:
            return True
    
    def disconnect(self):
        ''' Terminates all subscriptions and requests. Not required for
        a disconnect, but highly recommended, and prevents an window of
        attack for address spoofers. Note that such an attack would only
        leak metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['disconnect']
        )
        
        if response == b'\x01':
            self._subscriptions.clear()
            return True
        else:
            raise RuntimeError('Unknown status code during disconnection.')
            
            
class PersisterCore:
    ''' Core functions for persistence. Not to be confused with the 
    persister commands, which wrap the core.
    
    Other persisters should pass through the "ingestive tract". Local
    objects can be published directly through calling the ingest_<type> 
    methods.
    '''
    def __init__(self, doorman, enforcer, lawyer, bookie, librarian, 
                    undertaker, postman):
        self._opslock = threading.Lock()
        
        self._librarian = librarian
        self._bookie = bookie
        self._enforcer = enforcer
        self._lawyer = lawyer
        self._doorman = doorman
        self._postman = postman
        self._undertaker = undertaker
        self._enlitener = _Enlitener
        
    def ingest(self, packed):
        ''' Called on an untrusted and unknown object. May be bypassed
        by locally-created, trusted objects (by calling the individual 
        ingest methods directly). Parses, validates, and stores the 
        object, and returns True; or, raises an error.
        '''
        for loader, ingester in (
        (self._doorman.load_gidc, self.ingest_gidc),
        (self._doorman.load_geoc, self.ingest_geoc),
        (self._doorman.load_gobs, self.ingest_gobs),
        (self._doorman.load_gobd, self.ingest_gobd),
        (self._doorman.load_gdxx, self.ingest_gdxx),
        (self._doorman.load_garq, self.ingest_garq)):
            # Attempt this loader
            try:
                golix_obj = loader(packed)
            # This loader failed. Continue to the next.
            except MalformedGolixPrimitive:
                continue
            # This loader succeeded. Ingest it and then break out of the loop.
            else:
                obj = ingester(golix_obj)
                break
        # Running into the else means we could not find a loader.
        else:
            raise MalformedGolixPrimitive(
                '0x0001: Packed bytes do not appear to be a Golix primitive.'
            )
                    
        # Note that we don't need to call postman from the individual ingest
        # methods, because they will only be called directly for locally-built
        # objects, which will be distributed by the dispatcher.
        self._postman.schedule(obj)
                    
        return obj
        
    def ingest_gidc(self, obj):
        raw = obj.packed
        obj = self._enlitener._convert_gidc(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # First need to enforce target selection
            self._enforcer.validate_gidc(obj)
            # Now make sure authorship requirements are satisfied
            self._lawyer.validate_gidc(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self._undertaker:
                # Finally make sure persistence rules are followed
                self._bookie.validate_gidc(obj)
        
            # Force GC pass after every mutation
            with self._undertaker:
                # And now prep the undertaker for any necessary GC
                self._undertaker.prep_gidc(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self._bookie.place_gidc(obj)
                # And finally add it to the librarian
                self._librarian.store(obj, raw)
        
        return obj
        
    def ingest_geoc(self, obj):
        raw = obj.packed
        obj = self._enlitener._convert_geoc(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # First need to enforce target selection
            self._enforcer.validate_geoc(obj)
            # Now make sure authorship requirements are satisfied
            self._lawyer.validate_geoc(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self._undertaker:
                # Finally make sure persistence rules are followed
                self._bookie.validate_geoc(obj)
        
            # Force GC pass after every mutation
            with self._undertaker:
                # And now prep the undertaker for any necessary GC
                self._undertaker.prep_geoc(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self._bookie.place_geoc(obj)
                # And finally add it to the librarian
                self._librarian.store(obj, raw)
        
        return obj
        
    def ingest_gobs(self, obj):
        raw = obj.packed
        obj = self._enlitener._convert_gobs(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # First need to enforce target selection
            self._enforcer.validate_gobs(obj)
            # Now make sure authorship requirements are satisfied
            self._lawyer.validate_gobs(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self._undertaker:
                # Finally make sure persistence rules are followed
                self._bookie.validate_gobs(obj)
        
            # Force GC pass after every mutation
            with self._undertaker:
                # And now prep the undertaker for any necessary GC
                self._undertaker.prep_gobs(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self._bookie.place_gobs(obj)
                # And finally add it to the librarian
                self._librarian.store(obj, raw)
        
        return obj
        
    def ingest_gobd(self, obj):
        raw = obj.packed
        obj = self._enlitener._convert_gobd(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # First need to enforce target selection
            self._enforcer.validate_gobd(obj)
            # Now make sure authorship requirements are satisfied
            self._lawyer.validate_gobd(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self._undertaker:
                # Finally make sure persistence rules are followed
                self._bookie.validate_gobd(obj)
        
            # Force GC pass after every mutation
            with self._undertaker:
                # And now prep the undertaker for any necessary GC
                self._undertaker.prep_gobd(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self._bookie.place_gobd(obj)
                # And finally add it to the librarian
                self._librarian.store(obj, raw)
        
        return obj
        
    def ingest_gdxx(self, obj):
        raw = obj.packed
        obj = self._enlitener._convert_gdxx(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # First need to enforce target selection
            self._enforcer.validate_gdxx(obj)
            # Now make sure authorship requirements are satisfied
            self._lawyer.validate_gdxx(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self._undertaker:
                # Finally make sure persistence rules are followed
                self._bookie.validate_gdxx(obj)
        
            # Force GC pass after every mutation
            with self._undertaker:
                # And now prep the undertaker for any necessary GC
                self._undertaker.prep_gdxx(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self._bookie.place_gdxx(obj)
                # And finally add it to the librarian
                self._librarian.store(obj, raw)
        
        return obj
        
    def ingest_garq(self, obj):
        raw = obj.packed
        obj = self._enlitener._convert_garq(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # First need to enforce target selection
            self._enforcer.validate_garq(obj)
            # Now make sure authorship requirements are satisfied
            self._lawyer.validate_garq(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self._undertaker:
                # Finally make sure persistence rules are followed
                self._bookie.validate_garq(obj)
        
            # Force GC pass after every mutation
            with self._undertaker:
                # And now prep the undertaker for any necessary GC
                self._undertaker.prep_garq(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self._bookie.place_garq(obj)
                # And finally add it to the librarian
                self._librarian.store(obj, raw)
        
        return obj
        
        
class _Doorman:
    ''' Parses files and enforces crypto. Can be bypassed for trusted 
    (aka locally-created) objects. Only called from within the typeless
    PersisterCore.ingest() method.
    '''
    def __init__(self, librarian):
        self._librarian = librarian
        self._golix = ThirdParty()
        
    def load_gidc(self, packed):
        try:
            obj = GIDC.unpack(packed)
        except Exception as exc:
            # logger.error('Malformed gidc: ' + str(packed))
            # logger.error(repr(exc) + '\n').join(traceback.format_tb(exc.__traceback__))
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GIDC object.'
            ) from exc
            
        # No further verification required.
            
        return obj
        
    def load_geoc(self, packed):
        try:
            obj = GEOC.unpack(packed)
        except Exception as exc:
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GEOC object.'
            ) from exc
            
        # Okay, now we need to verify the object
        try:
            author = self._librarian.whois(obj.author)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient.'
            ) from exc
            
        try:
            self._golix.verify_object(
                second_party = author.identity,
                obj = obj,
            )
        except SecurityError as exc:
            raise VerificationFailure(
                '0x0002: Failed to verify object.'
            ) from exc
            
        return obj
        
    def load_gobs(self, packed):
        try:
            obj = GOBS.unpack(packed)
        except Exception as exc:
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GOBS object.'
            ) from exc
            
        # Okay, now we need to verify the object
        try:
            author = self._librarian.whois(obj.binder)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient.'
            ) from exc
            
        try:
            self._golix.verify_object(
                second_party = author.identity,
                obj = obj,
            )
        except SecurityError as exc:
            raise VerificationFailure(
                '0x0002: Failed to verify object.'
            ) from exc
            
        return obj
        
    def load_gobd(self, packed):
        try:
            obj = GOBD.unpack(packed)
        except Exception as exc:
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GOBD object.'
            ) from exc
            
        # Okay, now we need to verify the object
        try:
            author = self._librarian.whois(obj.binder)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient.'
            ) from exc
            
        try:
            self._golix.verify_object(
                second_party = author.identity,
                obj = obj,
            )
        except SecurityError as exc:
            raise VerificationFailure(
                '0x0002: Failed to verify object.'
            ) from exc
            
        return obj
        
    def load_gdxx(self, packed):
        try:
            obj = GDXX.unpack(packed)
        except Exception as exc:
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GDXX object.'
            ) from exc
            
        # Okay, now we need to verify the object
        try:
            author = self._librarian.whois(obj.debinder)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient.'
            ) from exc
            
        try:
            self._golix.verify_object(
                second_party = author.identity,
                obj = obj,
            )
        except SecurityError as exc:
            raise VerificationFailure(
                '0x0002: Failed to verify object.'
            ) from exc
            
        return obj
        
    def load_garq(self, packed):
        try:
            obj = GARQ.unpack(packed)
        except Exception as exc:
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GARQ object.'
            ) from exc
            
        # Persisters cannot further verify the object.
            
        return obj
            

_MrPostcard = collections.namedtuple(
    typename = '_MrPostcard',
    field_names = ('subscription', 'notification'),
)

            
class _MrPostman:
    ''' Tracks, delivers notifications about objects using **only weak
    references** to them. Threadsafe.
    
    ♫ Please Mister Postman...
    
    Question: should the distributed state management of GARQ recipients
    be managed here, or in the bookie (where it currently is)?
    '''
    def __init__(self, librarian, bookie):
        self._bookie = bookie
        self._librarian = librarian
        
        self._out_for_delivery = threading.Event()
        
        # Lookup <ghid>: set(<callback>)
        self._opslock_listen = threading.Lock()
        self._listeners = {}
        
        # The scheduling queue
        self._scheduled = queue.Queue()
        # Ignoring lookup: <subscribed ghid>: set(<callbacks>)
        # TODO: ctrl+F this yields 3 results. Bug?
        self._opslock_ignore = threading.Lock()
        self._ignored = {}
        # The delayed lookup. <awaiting ghid>: set(<subscribed ghids>)
        self._opslock_defer = threading.Lock()
        self._deferred = {}
        
    def schedule(self, obj, removed=False):
        ''' Schedules update delivery for the passed object.
        '''
        for deferred in self._has_deferred(obj):
            # These have already been put into _MrPostcard form.
            self._scheduled.put(deferred)
            
        for primitive, scheduler in (
        (_GidcLite, self._schedule_gidc),
        (_GeocLite, self._schedule_geoc),
        (_GobsLite, self._schedule_gobs),
        (_GobdLite, self._schedule_gobd),
        (_GdxxLite, self._schedule_gdxx),
        (_GarqLite, self._schedule_garq)):
            if isinstance(obj, primitive):
                scheduler(obj, removed)
                break
        else:
            raise TypeError('Could not schedule: wrong obj type.')
            
        return True
        
    def _schedule_gidc(self, obj, removed):
        # GIDC will never trigger a subscription.
        pass
        
    def _schedule_geoc(self, obj, removed):
        # GEOC will never trigger a subscription directly, though they might
        # have deferred updates (which are handled by self.schedule)
        pass
        
    def _schedule_gobs(self, obj, removed):
        # GOBS will never trigger a subscription.
        pass
        
    def _schedule_gobd(self, obj, removed):
        # GOBD might trigger a subscription! But, we also might to need to 
        # defer it. Or, we might be removing it.
        if removed:
            debinding_ghids = self._bookie.debind_status(obj.ghid)
            if not debinding_ghids:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            for debinding_ghid in debinding_ghids:
                self._scheduled.put(
                    _MrPostcard(obj.ghid, debinding_ghid)
                )
        else:
            notifier = _MrPostcard(obj.ghid, obj.frame_ghid)
            if obj.target not in self._librarian:
                self._defer_update(
                    awaiting_ghid = obj.target,
                    notifier = notifier,
                )
            else:
                self._scheduled.put(notifier)
        
    def _schedule_gdxx(self, obj, removed):
        # GDXX will never directly trigger a subscription. If they are removing
        # a subscribed object, the actual removal (in the undertaker GC) will 
        # trigger a subscription without us.
        pass
        
    def _schedule_garq(self, obj, removed):
        # GARQ might trigger a subscription! Or we might be removing it.
        if removed:
            debinding_ghids = self._bookie.debind_status(obj.ghid)
            if not debinding_ghids:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            for debinding_ghid in debinding_ghids:
                self._scheduled.put(
                    _MrPostcard(obj.recipient, debinding_ghid)
                )
        else:
            self._scheduled.put(
                _MrPostcard(obj.recipient, obj.ghid)
            )
            
    def _defer_update(self, awaiting_ghid, notifier):
        ''' Defer a subscription notification until the awaiting_ghid is
        received as well.
        '''
        # Note that deferred updates will always be dynamic bindings, so the
        # subscribed ghid will be identical to the notification ghid.
        with self._opslock_defer:
            try:
                self._deferred[awaiting_ghid].add(notifier)
            except KeyError:
                self._deferred[awaiting_ghid] = { notifier }
            
    def _has_deferred(self, obj):
        ''' Checks to see if a subscription is waiting on the obj, and 
        if so, returns the originally subscribed ghid.
        '''
        with self._opslock_defer:
            try:
                subscribed_ghids = self._deferred[obj.ghid]
            except KeyError:
                return set()
            else:
                del self._deferred[obj.ghid]
                return subscribed_ghids
        
    def ignore_next_update(self, ghid, callback):
        ''' Tells the postman to ignore the next update received for the
        passed ghid at the callback.
        '''
        with self._opslock_ignore:
            try:
                self._ignored[ghid].add(callback)
            except KeyError:
                self._ignored[ghid] = { callback }
        
    def subscribe(self, ghid, callback):
        ''' Tells the postman that the watching_session would like to be
        updated about ghid.
        '''
        # First add the subscription listeners
        with self._opslock_listen:
            try:
                self._listeners[ghid].add(callback)
            except KeyError:
                self._listeners[ghid] = { callback }
            
        # Now manually reinstate any desired notifications for garq requests
        # that have yet to be handled
        for existing_mail in self._bookie.recipient_status(ghid):
            obj = self._librarian.whois(existing_mail)
            self.schedule(obj)
            
    def unsubscribe(self, ghid, callback):
        ''' Remove the callback for ghid. Indempotent; will never raise
        a keyerror.
        '''
        try:
            self._listeners[ghid].discard(callback)
        except KeyError:
            logger.debug('KeyError while unsubscribing.')
            
    def do_mail_run(self):
        ''' Executes the actual mail run, clearing out the _scheduled
        queue.
        '''
        # Mail runs will continue until all pending are consumed, so threads 
        # can add to the queue until everythin is done. But, multiple calls to
        # do_mail_run will cause us to hang, and if it's from the same thread,
        # we'll deadlock. So, at least for now, prevent reentrant do_mail_run.
        # NOTE: there is a small race condition between finishing the delivery
        # loop and releasing the out_for_delivery event.
        # TODO: find a more elegant solution.
        if not self._out_for_delivery.is_set():
            self._out_for_delivery.set()
            try:
                self._delivery_loop()
            finally:
                self._out_for_delivery.clear()
                
    def _delivery_loop(self):
        while not self._scheduled.empty():
            # Ideally this will be the only consumer, but we might be running
            # in multiple threads or something, so try/catch just in case.
            try:
                subscription, notification = self._scheduled.get(block=False)
                
            except queue.Empty:
                break
                
            else:
                # We can't spin this out into a thread because some of our 
                # delivery mechanisms want this to have an event loop.
                self._deliver(subscription, notification)
            
    def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        # We need to freeze the listeners before we operate on them, but we 
        # don't need to lock them while we go through all of the callbacks.
        # Instead, just sacrifice any subs being added concurrently to the 
        # current delivery run.
        try:
            callbacks = frozenset(self._listeners[subscription])
        # No listeners for it? No worries.
        except KeyError:
            callbacks = frozenset()
                
        for callback in callbacks:
            callback(subscription, notification)
        
        
class _Undertaker:
    ''' Note: what about post-facto removal of bindings that have 
    illegal targets? For example, if someone uploads a binding for a 
    target that isn't currently known, and then it turns out that the
    target, once uploaded, actually doesn't support that binding, what
    should we do?
    
    In theory it shouldn't affect other operations. Should we just bill
    for it and call it a day? We'd need to make some kind of call to the
    bookie to handle that.
    '''
    def __init__(self, librarian, bookie, postman):
        self._librarian = librarian
        self._bookie = bookie
        self._postman = postman
        self._staging = None
        
    def __enter__(self):
        # Create a new staging object.
        self._staging = set()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        # TODO: exception handling
        # This is pretty clever; we need to be able to modify the set while 
        # iterating it, so just wait until it's empty.
        while self._staging:
            ghid = self._staging.pop()
            try:
                obj = self._librarian.whois(ghid)
                
            except KeyError:
                logger.warning(
                    'Attempt to GC an object not found in librarian.'
                )
            
            else:
                for primitive, gcollector in ((_GidcLite, self._gc_gidc),
                                            (_GeocLite, self._gc_geoc),
                                            (_GobsLite, self._gc_gobs),
                                            (_GobdLite, self._gc_gobd),
                                            (_GdxxLite, self._gc_gdxx),
                                            (_GarqLite, self._gc_garq)):
                    if isinstance(obj, primitive):
                        gcollector(obj)
                        break
                else:
                    # No appropriate GCer found (should we typerror?); so 
                    # continue with WHILE loop
                    continue
                    logger.error('No appropriate GC routine found!')
            
        self._staging = None
        
    def triage(self, ghid):
        ''' Schedule GC check for object.
        
        Note: should triaging be order-dependent?
        '''
        logger.debug('Performing triage.')
        if self._staging is None:
            raise RuntimeError(
                'Cannot triage outside of the undertaker\'s context manager.'
            )
        else:
            self._staging.add(ghid)
            
    def _gc_gidc(self, obj):
        ''' Check whether we should remove a GIDC, and then remove it
        if appropriate. Currently we don't do that, so just leave it 
        alone.
        '''
        return
            
    def _gc_geoc(self, obj):
        ''' Check whether we should remove a GEOC, and then remove it if
        appropriate. Pretty simple: is it bound?
        '''
        if not self._bookie.is_bound(obj):
            self._gc_execute(obj)
            
    def _gc_gobs(self, obj):
        logger.debug('Entering gobs GC.')
        if self._bookie.is_debound(obj):
            logger.debug('Gobs is debound. Staging target and executing GC.')
            # Add our target to the list of GC checks
            self._staging.add(obj.target)
            self._gc_execute(obj)
            
    def _gc_gobd(self, obj):
        # Child bindings can prevent GCing GOBDs
        if self._bookie.is_debound(obj) and not self._bookie.is_bound(obj):
            # Still need to add target
            self._staging.add(obj.target)
            self._gc_execute(obj)
            
    def _gc_gdxx(self, obj):
        # Note that removing a debinding cannot result in a downstream target
        # being GCd, because it wouldn't exist.
        if self._bookie.is_debound(obj) or self._bookie.is_illegal(obj):
            self._gc_execute(obj)
            
    def _gc_garq(self, obj):
        if self._bookie.is_debound(obj):
            self._gc_execute(obj)
        
    def _gc_execute(self, obj):
        # Call GC at bookie first so that librarian is still in the know.
        self._bookie.force_gc(obj)
        # Next, goodbye object.
        self._librarian.force_gc(obj)
        # Now notify the postman, and tell her it's a removal.
        self._postman.schedule(obj, removed=True)
        
    def prep_gidc(self, obj):
        ''' GIDC do not affect GC.
        '''
        return True
        
    def prep_geoc(self, obj):
        ''' GEOC do not affect GC.
        '''
        return True
        
    def prep_gobs(self, obj):
        ''' GOBS do not affect GC.
        '''
        return True
        
    def prep_gobd(self, obj):
        ''' GOBD require triage for previous targets.
        '''
        try:
            existing = self._librarian.whois(obj.ghid)
        except KeyError:
            # This will always happen if it's the first frame, so let's be sure
            # to ignore that for logging.
            if obj.history:
                logger.error('Could not find gobd to check existing target.')
        else:
            self.triage(existing.target)
            
        return True
        
    def prep_gdxx(self, obj):
        ''' GDXX require triage for new targets.
        '''
        self.triage(obj.target)
        return True
        
    def prep_garq(self, obj):
        ''' GARQ do not affect GC.
        '''
        return True
        
        
class _Lawyer:
    ''' Enforces authorship requirements, including both having a known
    entity as author/recipient and consistency for eg. bindings and 
    debindings.
    
    Threadsafe.
    '''
    def __init__(self, librarian):
        # Lookup for all known identity ghids
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        self._librarian = librarian
        
    def _validate_author(self, obj):
        try:
            author = self._librarian.whois(obj.author)
        except KeyError as exc:
            logger.info('0x0003: Unknown author / recipient.')
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient.'
            ) from exc
        else:
            if not isinstance(author, _GidcLite):
                logger.info('0x0003: Invalid author / recipient.')
                raise InvalidIdentity(
                    '0x0003: Invalid author / recipient.'
                )
                
        return True
        
    def validate_gidc(self, obj):
        ''' GIDC need no validation.
        '''
        return True
        
    def validate_geoc(self, obj):
        ''' Ensure author is known and valid.
        '''
        return self._validate_author(obj)
        
    def validate_gobs(self, obj):
        ''' Ensure author is known and valid.
        '''
        return self._validate_author(obj)
        
    def validate_gobd(self, obj):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding (if it already exists).
        '''
        self._validate_author(obj)
        try:
            existing = self._librarian.whois(obj.ghid)
        except KeyError:
            pass
        else:
            if existing.author != obj.author:
                logger.info('0x0007: Inconsistent binding author.')
                raise InconsistentAuthor(
                    '0x0007: Inconsistent binding author.'
                )
        return True
        
    def validate_gdxx(self, obj, target_obj=None):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding.
        
        If other is not None, specifically checks it against that object
        instead of obtaining it from librarian.
        '''
        self._validate_author(obj)
        try:
            if target_obj is None:
                existing = self._librarian.whois(obj.target)
            else:
                existing = target_obj
                
        except KeyError:
            pass
            
        else:
            if isinstance(existing, _GarqLite):
                if existing.recipient != obj.author:
                    logger.info('0x0007: Inconsistent debinding author.')
                    raise InconsistentAuthor(
                        '0x0007: Inconsistent debinding author.'
                    )
                
            else:
                if existing.author != obj.author:
                    logger.info('0x0007: Inconsistent debinding author.')
                    raise InconsistentAuthor(
                        '0x0007: Inconsistent debinding author.'
                    )
        return True
        
    def validate_garq(self, obj):
        ''' Validate recipient.
        '''
        try:
            recipient = self._librarian.whois(obj.recipient)
        except KeyError as exc:
            logger.info('0x0003: Unknown author / recipient.')
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient.'
            ) from exc
        else:
            if not isinstance(recipient, _GidcLite):
                logger.info('0x0003: Invalid author / recipient.')
                raise InvalidIdentity(
                    '0x0003: Invalid author / recipient.'
                )
                
        return True
        
        
class _Enforcer:
    ''' Enforces valid target selections.
    '''
    def __init__(self, librarian):
        self._librarian = librarian
        
    def validate_gidc(self, obj):
        ''' GIDC need no target verification.
        '''
        return True
        
    def validate_geoc(self, obj):
        ''' GEOC need no target validation.
        '''
        return True
        
    def validate_gobs(self, obj):
        ''' Check if target is known, and if it is, validate it.
        '''
        try:
            target = self._librarian.whois(obj.target)
        except KeyError:
            pass
        else:
            for forbidden in (_GidcLite, _GobsLite, _GdxxLite, _GarqLite):
                if isinstance(target, forbidden):
                    logger.info('0x0006: Invalid static binding target.')
                    raise InvalidTarget(
                        '0x0006: Invalid static binding target.'
                    )
        return True
        
    def validate_gobd(self, obj):
        ''' Check if target is known, and if it is, validate it.
        
        Also do a state check on the dynamic binding.
        '''
        try:
            target = self._librarian.whois(obj.target)
        except KeyError:
            pass
        else:
            for forbidden in (_GidcLite, _GobsLite, _GdxxLite, _GarqLite):
                if isinstance(target, forbidden):
                    logger.info('0x0006: Invalid dynamic binding target.')
                    raise InvalidTarget(
                        '0x0006: Invalid dynamic binding target.'
                    )
                    
        self._validate_dynamic_history(obj)
                    
        return True
        
    def validate_gdxx(self, obj, target_obj=None):
        ''' Check if target is known, and if it is, validate it.
        '''
        try:
            if target_obj is None:
                target = self._librarian.whois(obj.target)
            else:
                target = target_obj
        except KeyError:
            logger.warning(
                'GDXX was validated by Enforcer, but its target was unknown '
                'to the librarian. May indicated targeted attack.\n'
                '    GDXX ghid:   ' + str(bytes(obj.ghid)) + 
                '    Target ghid: ' + str(bytes(obj.target))
            )
            # raise InvalidTarget(
            #     '0x0006: Unknown debinding target. Cannot debind an unknown '
            #     'resource, to prevent a malicious party from preemptively '
            #     'uploading a debinding for a resource s/he did not bind.'
            # )
        else:
            # NOTE: if this changes, will need to modify place_gdxx in _Bookie
            for forbidden in (_GidcLite, _GeocLite):
                if isinstance(target, forbidden):
                    logger.info('0x0006: Invalid debinding target.')
                    raise InvalidTarget(
                        '0x0006: Invalid debinding target.'
                    )
        return True
        
    def validate_garq(self, obj):
        ''' No additional validation needed.
        '''
        return True
        
    def _validate_dynamic_history(self, obj):
        ''' Enforces state flow / progression for dynamic objects. In 
        other words, prevents zeroth bindings with history, makes sure
        future bindings contain previous ones in history, etc.
        '''
        # Try getting an existing binding.
        try:
            existing = self._librarian.whois(obj.ghid)
            
        except KeyError:
            if obj.history:
                raise IllegalDynamicFrame(
                    '0x0009: Illegal frame. Cannot upload a frame with '
                    'history as the first frame in a persistence provider.'
                )
                
        else:
            if existing.frame_ghid not in obj.history:
                raise IllegalDynamicFrame(
                    '0x0009: Illegal frame. Frame history did not contain the '
                    'most recent frame.'
                )
            
            
class _Bookie:
    ''' Tracks state relationships between objects using **only weak
    references** to them. ONLY CONCERNED WITH LIFETIMES! Does not check
    (for example) consistent authorship.
    
    (Not currently) threadsafe.
    '''
    def __init__(self, librarian, lawyer):
        self._opslock = threading.Lock()
        self._undertaker = None
        self._librarian = librarian
        self._lawyer = lawyer
        
        # Lookup for debindings flagged as illegal. So long as the local state
        # is successfully propagated upstream, this can be a local-only object.
        self._illegal_debindings = set()
        
        # Lookup <bound ghid>: set(<binding obj>)
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        self._bound_by_ghid = SetMap()
        
        # Lookup <debound ghid>: <debinding ghid>
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        # Note that any particular object can have exactly zero or one VALID 
        # debinds, but that a malicious actor could find a race condition and 
        # debind something FOR SOMEONE ELSE before the bookie knows about the
        # original object authorship.
        self._debound_by_ghid = SetMap()
        self._debound_by_ghid_staged = SetMap()
        
        # Lookup <recipient>: set(<request ghid>)
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        self._requests_for_recipient = SetMap()
        
    def link_undertaker(self, undertaker):
        # We need to be able to initiate GC on illegal debindings detected 
        # after the fact.
        self._undertaker = weakref.proxy(undertaker)
        
    def recipient_status(self, ghid):
        ''' Return a frozenset of ghids assigned to the passed ghid as
        a recipient.
        '''
        return self._requests_for_recipient.get_any(ghid)
        
    def bind_status(self, ghid):
        ''' Return a frozenset of ghids binding the passed ghid.
        '''
        return self._bound_by_ghid.get_any(ghid)
        
    def debind_status(self, ghid):
        ''' Return either a ghid, or None.
        '''
        total = set()
        total.update(self._debound_by_ghid.get_any(ghid))
        total.update(self._debound_by_ghid.get_any(ghid))
        return total
        
    def is_illegal(self, obj):
        ''' Check to see if this is an illegal debinding.
        '''
        return obj.ghid in self._illegal_debindings
        
    def is_bound(self, obj):
        ''' Check to see if the object has been bound.
        '''
        return obj.ghid in self._bound_by_ghid
            
    def is_debound(self, obj):
        # Well we have an object and a debinding, so now let's validate them.
        # NOTE: this needs to be converted to also check for debinding validity
        for debinding_ghid in self._debound_by_ghid_staged.get_any(obj.ghid):
            # Get the existing debinding
            debinding = self._librarian.whois(debinding_ghid)
            
            # Validate existing binding against newly-known target
            try:
                self._lawyer.validate_gdxx(debinding, target_obj=obj)
                
            # Validation failed. Remove illegal debinding.
            except:
                logger.warning(
                    'Removed invalid existing binding. \n'
                    '    Illegal debinding author: ' + 
                    str(bytes(debinding.author)) +
                    '    Valid object author:      ' + 
                    str(bytes(obj.author))
                )
                self._illegal_debindings.add(debinding.ghid)
                self._undertaker.triage(debinding.ghid)
            
            # It's valid, so move it out of staging.
            else:
                self._debound_by_ghid_staged.discard(obj.ghid, debinding.ghid)
                self._debound_by_ghid.add(obj.ghid, debinding.ghid)
            
        # Now we can just easily check to see if it's debound_by_ghid.
        return obj.ghid in self._debound_by_ghid
        
    def _add_binding(self, being_bound, doing_binding):
        # Exactly what it sounds like. Should remove this stub to reduce the
        # number of function calls.
        self._bound_by_ghid.add(being_bound, doing_binding)
            
    def _remove_binding(self, obj):
        being_unbound = obj.target
        
        try:
            self._bound_by_ghid.remove(being_unbound, obj.ghid)
        except KeyError:
            logger.warning(
                'Attempting to remove a binding, but the bookie has no record '
                'of its existence.'
            )
            
    def _remove_request(self, obj):
        recipient = obj.recipient
        self._requests_for_recipient.discard(recipient, obj.ghid)
            
    def _remove_debinding(self, obj):
        target = obj.target
        self._illegal_debindings.discard(obj.ghid)
        self._debound_by_ghid.discard(target, obj.ghid)
        self._debound_by_ghid_staged.discard(target, obj.ghid)
        
    def validate_gidc(self, obj):
        ''' GIDC need no state verification.
        '''
        return True
        
    def place_gidc(self, obj):
        ''' GIDC needs no special treatment here.
        '''
        pass
        
    def validate_geoc(self, obj):
        ''' GEOC must verify that they are bound.
        '''
        if self.is_bound(obj):
            return True
        else:
            raise UnboundContainer(
                '0x0004: Attempt to upload unbound GEOC; object immediately '
                'garbage collected.'
            )
        
    def place_geoc(self, obj):
        ''' No special treatment here.
        '''
        pass
        
    def validate_gobs(self, obj):
        if self.is_debound(obj):
            raise AlreadyDebound(
                '0x0005: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
        else:
            return True
        
    def place_gobs(self, obj):
        self._add_binding(
            being_bound = obj.target,
            doing_binding = obj.ghid,
        )
        
    def validate_gobd(self, obj):
        # A deliberate binding can override a debinding for GOBD.
        if self.is_debound(obj) and not self.is_bound(obj):
            raise AlreadyDebound(
                '0x0005: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
        else:
            return True
        
    def place_gobd(self, obj):
        # First we need to make sure we're not missing an existing frame for
        # this binding, and then to schedule a GC check for its target.
        try:
            existing = self._librarian.whois(obj.ghid)
        except KeyError:
            if obj.history:
                logger.error(
                    'Placing a dynamic frame with history, but it\'s missing '
                    'at the librarian.'
                )
        else:
            self._remove_binding(existing)
            
        # Now we have a clean slate and need to update things accordingly.
        self._add_binding(
            being_bound = obj.target,
            doing_binding = obj.ghid,
        )
        
    def validate_gdxx(self, obj):
        if self.is_debound(obj):
            raise AlreadyDebound(
                '0x0005: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
        else:
            return True
        
    def place_gdxx(self, obj):
        ''' Just record the fact that there is a debinding. Let GCing 
        worry about removing other records.
        '''
        # Note that the undertaker will worry about removing stuff from local
        # state. 
        if obj.target in self._librarian:
            self._debound_by_ghid.add(obj.target, obj.ghid)
        else:
            self._debound_by_ghid_staged.add(obj.target, obj.ghid)
        
    def validate_garq(self, obj):
        if self.is_debound(obj):
            raise AlreadyDebound(
                '0x0005: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
        else:
            return True
        
    def place_garq(self, obj):
        ''' Add the garq to the books.
        '''
        self._requests_for_recipient.add(obj.recipient, obj.ghid)
        
    def force_gc(self, obj):
        ''' Forces erasure of an object.
        '''
        is_binding = (isinstance(obj, _GobsLite) or
            isinstance(obj, _GobdLite))
        is_debinding = isinstance(obj, _GdxxLite)
        is_request = isinstance(obj, _GarqLite)
            
        if is_binding:
            self._remove_binding(obj)
        elif is_debinding:
            self._remove_debinding(obj)
        elif is_request:
            self._remove_request(obj)
                
    def __check_illegal_binding(self, ghid):
        ''' Deprecated-ish and unused. Former method to retroactively
        clear bindings that were initially (and illegally) accepted 
        because their (illegal) target was unknown at the time.
        
        Checks for an existing binding for ghid. If it exists,
        removes the binding, and forces its garbage collection. Used to
        overcome race condition inherent to binding.
        
        Should this warn?
        '''
        # Make sure not to check implicit bindings, or dynamic bindings will
        # show up as illegal if/when we statically bind them
        if ghid in self._bindings_static or ghid in self._bindings_dynamic:
            illegal_binding = self._bindings[ghid]
            del self._bindings[ghid]
            self._gc_execute(illegal_binding)
            
            
class _LibrarianCore(metaclass=abc.ABCMeta):
    ''' Base class for caching systems common to non-volatile librarians
    such as DiskLibrarian, S3Librarian, etc.
    
    TODO: make ghid vs frame ghid usage more consistent across things.
    '''
    def __init__(self):
        # Link to core, which will be assigned after __init__
        self._core = None
        
        # Operations and restoration lock
        self._restoring = TruthyLock()
        
        # Lookup for dynamic ghid -> frame ghid
        # Must be consistent across all concurrently connected librarians
        self._dyn_resolver = {}
        
        # Lookup for ghid -> hypergolix description
        # This may be GC'd by the python process.
        self._catalog = {}
        self._opslock = threading.Lock()
        
    def link_core(self, core):
        # Creates a weakref proxy to core.
        self._core = weakref.proxy(core)
        
    def force_gc(self, obj):
        ''' Forces erasure of an object. Does not notify the undertaker.
        Indempotent. Should never raise KeyError.
        '''
        with self._restoring.mutex:
            try:
                ghid = self._ghid_resolver(obj.ghid)
                self.remove_from_cache(ghid)
            except:
                logger.warning(
                    'Exception while removing from cache during object GC. '
                    'Probably a bug.\n' + ''.join(traceback.format_exc())
                )
            
            try:
                del self._catalog[ghid]
            except KeyError:
                pass
                
            if isinstance(obj, _GobdLite):
                del self._dyn_resolver[obj.ghid]
        
    def store(self, obj, data):
        ''' Starts tracking an object.
        obj is a hypergolix representation object.
        raw is bytes-like.
        '''  
        with self._restoring.mutex:
            # We need to do some resolver work if it's a dynamic object.
            if isinstance(obj, _GobdLite):
                reference = obj.frame_ghid
            else:
                reference = obj.ghid
            
            # Only add to cache if we are not restoring from it.
            if not self._restoring:
                self.add_to_cache(reference, data)
                
            self._catalog[reference] = obj
            
            # Finally, only if successful should we update
            if isinstance(obj, _GobdLite):
                # Remove any existing frame.
                if obj.ghid in self._dyn_resolver:
                    old_ghid = self._ghid_resolver(obj.ghid)
                    self.remove_from_cache(old_ghid)
                    # Remove any existing _catalog entry
                    self._catalog.pop(old_ghid, None)
                # Update new frame.
                self._dyn_resolver[obj.ghid] = obj.frame_ghid
                
    def _ghid_resolver(self, ghid):
        ''' Convert a dynamic ghid into a frame ghid, or return the ghid
        immediately if not dynamic.
        '''
        if ghid in self._dyn_resolver:
            return self._dyn_resolver[ghid]
        else:
            return ghid
        
    def dereference(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        with self._restoring.mutex:
            ghid = self._ghid_resolver(ghid)
            return self.get_from_cache(ghid)
        
    def whois(self, ghid):
        ''' Returns a lightweight Hypergolix description of the object.
        '''
        ghid = self._ghid_resolver(ghid)
        
        # No need to block if it exists, especially if we're restoring.
        try:
            return self._catalog[ghid]
            
        except KeyError as exc:
            # Put this inside the except block so that we don't have to do any
            # weird fiddling to make restoration work.
            with self._restoring.mutex:
                # Bypass lazy-load if restoring and re-raise
                if self._restoring:
                    raise
                else:
                    # Lazy-load a new one if possible.
                    self._lazy_load(ghid, exc)
                    return self._catalog[ghid]
                
    def _lazy_load(self, ghid, exc):
        ''' Does a lazy load restore of the ghid.
        '''
        if self._core is None:
            raise RuntimeError(
                'Core must be linked to lazy-load from cache.'
            ) from exc
            
        # This will raise if missing. Connect the new_exc to the old one tho.
        try:
            data = self.get_from_cache(ghid)
            
            # I guess we might as well validate on every lazy load.
            with self._restoring:
                self._core.ingest(data)
                
        except Exception as new_exc:
            raise new_exc from exc
        
    def __contains__(self, ghid):
        ghid = self._ghid_resolver(ghid)
        # Catalog may only be accurate locally. Shelf is accurate globally.
        return self.check_in_cache(ghid)
    
    def restore(self):
        ''' Loads any existing files from the cache.  All existing 
        files there will be attempted to be loaded, so it's best not to 
        have extraneous stuff in the directory. Will be passed through
        to the core for processing.
        '''
        # Upgrade this to warning just so that the default logging level is
        # still informed that we're restoring.
        logger.warning('# BEGINNING LIBRARIAN RESTORATION ###################')
        
        if self._core is None:
            raise RuntimeError(
                'Cannot restore a librarian\'s cache without first linking to '
                'its corresponding core.'
            )
        
        # This prevents us from wasting time rewriting existing entries in the
        # cache.
        with self._restoring:
            gidcs = []
            geocs = []
            gobss = []
            gobds = []
            gdxxs = []
            garqs = []
            
            # This will mutate the lists in-place.
            for candidate in self.walk_cache():
                self._attempt_load_inplace(
                    candidate, gidcs, geocs, gobss, gobds, gdxxs, garqs
                )
                
            # Okay yes, unfortunately this will result in unpacking all of the
            # files twice. However, we need to verify the crypto.
            
            # First load all identities, so that we have authors for everything
            for gidc in gidcs:
                self._core.ingest(gidc.packed)
                # self._core.ingest_gidc(gidc)
                
            # Now all debindings, so that we can check state while we're at it
            for gdxx in gdxxs:
                self._core.ingest(gdxx.packed)
                # self._core.ingest_gdxx(gdxx)
                
            # Now all bindings, so that objects aren't gc'd. Note: can't 
            # combine into single list, because of different ingest methods
            for gobs in gobss:
                self._core.ingest(gobs.packed)
                # self._core.ingest_gobs(gobs)
            for gobd in gobds:
                self._core.ingest(gobd.packed)
                # self._core.ingest_gobd(gobd)
                
            # Next the objects themselves, so that any requests will have their 
            # targets available (not that it would matter yet, buuuuut)...
            for geoc in geocs:
                self._core.ingest(geoc.packed)
                # self._core.ingest_geoc(geoc)
                
            # Last but not least
            for garq in garqs:
                self._core.ingest(garq.packed)
                # self._core.ingest_garq(garq)
                
        # Upgrade this to warning just so that the default logging level is
        # still informed that we're restoring.
        logger.warning('# COMPLETED LIBRARIAN RESTORATION ###################')
                
    def _attempt_load_inplace(self, candidate, gidcs, geocs, gobss, gobds, 
                            gdxxs, garqs):
        ''' Attempts to do an inplace addition to the passed lists based
        on the loading.
        '''
        for loader, target in ((GIDC.unpack, gidcs),
                                (GEOC.unpack, geocs),
                                (GOBS.unpack, gobss),
                                (GOBD.unpack, gobds),
                                (GDXX.unpack, gdxxs),
                                (GARQ.unpack, garqs)):
            # Attempt this loader
            try:
                golix_obj = loader(candidate)
            # This loader failed. Continue to the next.
            except ParseError:
                continue
            # This loader succeeded. Ingest it and then break out of the loop.
            else:
                obj = target.append(golix_obj)
                break
                
        # HOWEVER, unlike usual, don't raise if this isn't a correct object,
        # just don't bother adding it either.
        
            
    @abc.abstractmethod
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        pass
        
    @abc.abstractmethod
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the 
        cache.
        '''
        pass
        
    @abc.abstractmethod
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        pass
        
    @abc.abstractmethod
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        pass
        
    @abc.abstractmethod
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        pass
    
    
class DiskLibrarian(_LibrarianCore):
    ''' Librarian that caches stuff to disk.
    '''
    def __init__(self, cache_dir):
        ''' cache_dir should be relative to current.
        '''
        cache_dir = pathlib.Path(cache_dir)
        if not cache_dir.exists():
            raise ValueError('Path does not exist.')
        if not cache_dir.is_dir():
            raise ValueError('Path is not an available directory.')
        
        self._cachedir = cache_dir
        super().__init__()
        
    def _make_path(self, ghid):
        ''' Converts the ghid to a file path.
        '''
        fname = base64.urlsafe_b64encode(bytes(ghid)).decode() + '.ghid'
        fpath = self._cachedir / fname
        return fpath
        
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        for child in self._cachedir.iterdir():
            if child.is_file():
                yield child.read_bytes()
            
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        fpath = self._make_path(ghid)
        fpath.write_bytes(data)
        
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the 
        cache.
        '''
        fpath = self._make_path(ghid)
        try:
            fpath.unlink()
        except FileNotFoundError as exc:
            raise KeyError('Ghid does not exist at persister.') from exc
        
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        fpath = self._make_path(ghid)
        try:
            return fpath.read_bytes()
        except FileNotFoundError as exc:
            raise KeyError('Ghid does not exist at persister.') from exc
        
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        fpath = self._make_path(ghid)
        return fpath.exists()
        
        
class _Librarian(_LibrarianCore):
    def __init__(self):
        self._shelf = {}
        super().__init__()
        
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        pass
            
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        self._shelf[ghid] = data
        
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the 
        cache.
        '''
        del self._shelf[ghid]
        
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        return self._shelf[ghid]
        
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        return ghid in self._shelf
            
            
class __Librarian:
    ''' Keeps objects. Should contain the only strong references to the
    objects it keeps. Threadsafe.
    '''
    def __init__(self, shelf=None, catalog=None):
        ''' Sets up internal tracking.
        '''
        if shelf is None:
            shelf = {}
        if catalog is None:
            catalog = {}
            
        # Lookup for ghid -> raw bytes
        # This must be valid across all instances at the persister.
        self._shelf = shelf
        # Lookup for ghid -> hypergolix description
        # This may be GC'd by the python process.
        self._catalog = catalog
        # Operations lock
        self._opslock = threading.Lock()
        
    def force_gc(self, obj):
        ''' Forces erasure of an object. Does not notify the undertaker.
        Indempotent. Should never raise KeyError.
        '''
        with self._opslock:
            try:
                del self._shelf[obj.ghid]
            except KeyError:
                logger.warning(
                    'Attempted to GC a non-existent object. Probably a bug.'
                )
            
            try:
                del self._catalog[obj.ghid]
            except KeyError:
                pass
        
    def store(self, obj, raw):
        ''' Starts tracking an object.
        obj is a hypergolix representation object.
        raw is bytes-like.
        '''  
        with self._opslock:
            self._shelf[obj.ghid] = raw
            self._catalog[obj.ghid] = obj
        
    def dereference(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        return self._shelf[ghid]
        
    def whois(self, ghid):
        ''' Returns a lightweight Hypergolix description of the object.
        
        TODO: incorporate lazy-loading in case of catalog GCing.
        '''
        return self._catalog[ghid]
        
    def __contains__(self, ghid):
        # Catalog may only be accurate locally. Shelf is accurate globally.
        return ghid in self._shelf
        
    def link_core(self, *args, **kwargs):
        ''' Not used for this kind of librarian.
        '''
        pass
        
        
class _Enlitener:
    ''' Handles conversion from heavyweight Golix objects to lightweight
    Hypergolix representations.
    ''' 
    @staticmethod
    def _convert_gidc(gidc):
        ''' Converts a Golix object into a Hypergolix description.
        '''
        identity = SecondParty.from_identity(gidc)
        return _GidcLite(
            ghid = gidc.ghid,
            identity = identity,
        )
        
    @staticmethod
    def _convert_geoc(geoc):
        ''' Converts a Golix object into a Hypergolix description.
        '''
        return _GeocLite(
            ghid = geoc.ghid,
            author = geoc.author,
        )
        
    @staticmethod
    def _convert_gobs(gobs):
        ''' Converts a Golix object into a Hypergolix description.
        '''
        return _GobsLite(
            ghid = gobs.ghid,
            author = gobs.binder,
            target = gobs.target,
        )
        
    @staticmethod
    def _convert_gobd(gobd):
        ''' Converts a Golix object into a Hypergolix description.
        '''
        return _GobdLite(
            ghid = gobd.ghid_dynamic,
            author = gobd.binder,
            target = gobd.target,
            frame_ghid = gobd.ghid,
            history = gobd.history,
        )
        
    @staticmethod
    def _convert_gdxx(gdxx):
        ''' Converts a Golix object into a Hypergolix description.
        '''
        return _GdxxLite(
            ghid = gdxx.ghid,
            author = gdxx.debinder, 
            target = gdxx.target,
        )
        
    @staticmethod
    def _convert_garq(garq):
        ''' Converts a Golix object into a Hypergolix description.
        '''
        return _GarqLite(
            ghid = garq.ghid,
            recipient = garq.recipient,
        )
        
        
class _BaseLite:
    __slots__ = [
        'ghid',
        '__weakref__',
    ]
    
    def __hash__(self):
        return hash(self.ghid)
        
    def __eq__(self, other):
        try:
            return self.ghid == other.ghid
        except AttributeError as exc:
            raise TypeError('Incomparable types.') from exc
        
        
class _GidcLite(_BaseLite):
    ''' Lightweight description of a GIDC.
    '''
    __slots__ = [
        'identity'
    ]
    
    def __init__(self, ghid, identity):
        self.ghid = ghid
        self.identity = identity
        
        
class _GeocLite(_BaseLite):
    ''' Lightweight description of a GEOC.
    '''
    __slots__ = [
        'author',
    ]
    
    def __init__(self, ghid, author):
        self.ghid = ghid
        self.author = author
        
    def __eq__(self, other):
        try:
            return (
                super().__eq__(other) and 
                self.author == other.author
            )
        # This will not catch a super() TyperError, so we want to be able to
        # compare anything with a ghid. In reality, any situation where the
        # authors don't match but the ghids do is almost certainly a bug; but,
        # compare it anyways just in case.
        except AttributeError as exc:
            return False
        
        
class _GobsLite(_BaseLite):
    ''' Lightweight description of a GOBS.
    '''
    __slots__ = [
        'author',
        'target',
    ]
    
    def __init__(self, ghid, author, target):
        self.ghid = ghid
        self.author = author
        self.target = target
        
    def __eq__(self, other):
        try:
            return (
                super().__eq__(other) and 
                self.author == other.author and
                self.target == other.target
            )
            
        # This will not catch a super() TyperError, so we want to be able to
        # compare anything with a ghid. In reality, any situation where the
        # authors don't match but the ghids do is almost certainly a bug; but,
        # compare it anyways just in case.
        except AttributeError as exc:
            return False
    
        
class _GobdLite(_BaseLite):
    ''' Lightweight description of a GOBD.
    '''
    __slots__ = [
        'author',
        'target',
        'frame_ghid',
        'history',
    ]
    
    def __init__(self, ghid, author, target, frame_ghid, history):
        self.ghid = ghid
        self.author = author
        self.target = target
        self.frame_ghid = frame_ghid
        self.history = history
        
    def __eq__(self, other):
        try:
            return (
                super().__eq__(other) and 
                self.author == other.author and
                self.target == other.target and
                self.frame_ghid == other.frame_ghid
                # Skip history, because it could potentially vary
                # self.history == other.history
            )
            
        # This will not catch a super() TyperError, so we want to be able to
        # compare anything with a ghid. In reality, any situation where the
        # authors don't match but the ghids do is almost certainly a bug; but,
        # compare it anyways just in case.
        except AttributeError as exc:
            return False
    
        
class _GdxxLite(_BaseLite):
    ''' Lightweight description of a GDXX.
    '''
    __slots__ = [
        'author',
        'target',
        '_debinding',
    ]
    
    def __init__(self, ghid, author, target):
        self.ghid = ghid
        self.author = author
        self.target = target
        self._debinding = True
        
    def __eq__(self, other):
        try:
            return (
                super().__eq__(other) and 
                self.author == other.author and
                self._debinding == other._debinding
            )
            
        # This will not catch a super() TyperError, so we want to be able to
        # compare anything with a ghid. In reality, any situation where the
        # authors don't match but the ghids do is almost certainly a bug; but,
        # compare it anyways just in case.
        except AttributeError as exc:
            return False
        
        
class _GarqLite(_BaseLite):
    ''' Lightweight description of a GARQ.
    '''
    __slots__ = [
        'recipient',
    ]
    
    def __init__(self, ghid, recipient):
        self.ghid = ghid
        self.recipient = recipient
        
    def __eq__(self, other):
        try:
            return (
                super().__eq__(other) and 
                self.recipient == other.recipient
            )
            
        # This will not catch a super() TyperError, so we want to be able to
        # compare anything with a ghid. In reality, any situation where the
        # authors don't match but the ghids do is almost certainly a bug; but,
        # compare it anyways just in case.
        except AttributeError as exc:
            return False
            
            
def circus_factory(core_class=PersisterCore, core_kwargs=None, 
                    doorman_class=_Doorman, doorman_kwargs=None, 
                    enforcer_class=_Enforcer, enforcer_kwargs=None,
                    lawyer_class=_Lawyer, lawyer_kwargs=None,
                    bookie_class=_Bookie, bookie_kwargs=None,
                    librarian_class=_Librarian, librarian_kwargs=None,
                    undertaker_class=_Undertaker, undertaker_kwargs=None,
                    postman_class=_MrPostman, postman_kwargs=None):
    ''' Generate a PersisterCore, and its associated circus, correctly
    linking all of the objects in the process. Returns their instances
    in the same order they were passed.
    '''
    core_kwargs = core_kwargs or {}
    doorman_kwargs = doorman_kwargs or {}
    enforcer_kwargs = enforcer_kwargs or {}
    lawyer_kwargs = lawyer_kwargs or {}
    bookie_kwargs = bookie_kwargs or {}
    librarian_kwargs = librarian_kwargs or {}
    undertaker_kwargs = undertaker_kwargs or {}
    postman_kwargs = postman_kwargs or {}
    
    librarian = librarian_class(**librarian_kwargs)
    doorman = doorman_class(librarian=librarian, **doorman_kwargs)
    enforcer = enforcer_class(librarian=librarian, **enforcer_kwargs)
    lawyer = lawyer_class(librarian=librarian, **lawyer_kwargs)
    bookie = bookie_class(librarian=librarian, lawyer=lawyer, **bookie_kwargs)
    postman = postman_class(
        librarian = librarian,
        bookie = bookie,
        **postman_kwargs
    )
    undertaker = undertaker_class(
        librarian = librarian,
        bookie = bookie,
        postman = postman,
        **undertaker_kwargs
    )
    bookie.link_undertaker(undertaker)
    core = core_class(
        doorman = doorman,
        enforcer = enforcer,
        lawyer = lawyer,
        bookie = bookie,
        librarian = librarian,
        undertaker = undertaker,
        postman = postman,
        **core_kwargs
    )
    librarian.link_core(core)
    
    return (core, doorman, enforcer, lawyer, bookie, librarian, undertaker, 
            postman)