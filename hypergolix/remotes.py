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

RemoteNak status code conventions:
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

# Global dependencies
import abc
import collections
import weakref
import concurrent.futures
import threading
import traceback

import asyncio

from golix import Ghid

from golix.utils import generate_ghidlist_parser

# Local dependencies
from .persistence import PersistenceCore
from .persistence import Doorman
from .persistence import PostOffice
from .persistence import Undertaker
from .persistence import Lawyer
from .persistence import Enforcer
from .persistence import Bookie
from .persistence import DiskLibrarian
from .persistence import MemoryLibrarian
from .persistence import Enlitener
from .persistence import SalmonatorNoop

from .exceptions import RemoteNak
from .exceptions import MalformedGolixPrimitive
from .exceptions import VerificationFailure
from .exceptions import UnboundContainer
from .exceptions import InvalidIdentity
from .exceptions import DoesNotExist
from .exceptions import AlreadyDebound
from .exceptions import InvalidTarget
# from .exceptions import StillBoundWarning
# from .exceptions import RequestError
from .exceptions import InconsistentAuthor
from .exceptions import IllegalDynamicFrame

from .utils import _JitSetDict
from .utils import call_coroutine_threadsafe
from .utils import _generate_threadnames

# from .comms import WSAutoServer
# from .comms import WSAutoClient
from .comms import WSConnection as _AutoresponderSession
from .comms import RequestResponseProtocol as Autoresponder


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Rolodex',
]


# ###############################################
# Library
# ###############################################


ERROR_CODES = {
    b'\xFF\xFF': RemoteNak,
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


class RemotePersistenceServer:
    ''' Simple persistence server.
    Expected defaults:
    host:       'localhost'
    port:       7770
    logfile:    None
    verbosity:  'warning'
    debug:      False
    traceur:    False
    '''
    
    def __init__(self, cache_dir=None):
        self.bridge = None
        
        self.percore = PersistenceCore()
        self.doorman = Doorman()
        self.enforcer = Enforcer()
        self.lawyer = Lawyer()
        self.bookie = Bookie()
        
        if cache_dir is None:
            self.librarian = MemoryLibrarian()
        else:
            self.librarian = DiskLibrarian(cache_dir)
            
        self.postman = PostOffice()
        self.undertaker = Undertaker()
        # I mean, this won't be used unless we set up peering, but it saves us
        # needing to do a modal switch for remote persistence servers
        self.salmonator = SalmonatorNoop()
        
    def assemble(self, bridge):
        # Now we need to link everything together.
        self.percore.assemble(self.doorman, self.enforcer,
                              self.lawyer, self.bookie,
                              self.librarian, self.postman,
                              self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.lawyer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore)
        self.postman.assemble(self.librarian, self.bookie)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        # Note that this will break if we ever try to use it, because
        # golix_core isn't actually a golix_core.
        self.salmonator.assemble(self, self.percore, self.doorman,
                                 self.postman, self.librarian)
        
        # Okay, now set up the bridge, and we should be ready.
        self.bridge = bridge
        self.bridge.assemble(self.percore, self.bookie,
                             self.librarian, self.postman)


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
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise RemoteNak
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
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to
        the passed ghid at the persistence provider. Removes only the
        passed callback.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def list_bindings(self, ghid):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def list_debindings(self, ghid):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise RemoteNak
        '''
        pass
        
    @abc.abstractmethod
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise RemoteNak
        '''
        pass
    
    @abc.abstractmethod
    def disconnect(self):
        ''' Terminates all subscriptions and requests. Not required for
        a disconnect, but highly recommended, and prevents an window of
        attack for address spoofers. Note that such an attack would only
        leak metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        pass


class MemoryPersister(PersistenceCore):
    ''' Basic in-memory persister.
    
    This is a deprecated legacy thing we're keeping around so that we
    don't need to completely re-write our already inadequate test suite.
    '''
    
    def __init__(self):
        super().__init__()
        self.assemble(Doorman(), Enforcer(), Lawyer(), Bookie(),
                      MemoryLibrarian(), PostOffice(), Undertaker(),
                      SalmonatorNoop())
        
        self.subscribe = self.postman.subscribe
        self.unsubscribe = self.postman.unsubscribe
        # self.silence_notification = self.postman.silence_notification
        # self.publish = self.core.ingest
        self.list_bindings = self.bookie.bind_status
        self.list_debindings = self.bookie.debind_status
        
    def assemble(self, doorman, enforcer, lawyer, bookie, librarian, postman,
                 undertaker, salmonator):
        self.doorman = doorman
        self.enlitener = Enlitener
        self.enforcer = enforcer
        self.lawyer = lawyer
        self.bookie = bookie
        self.postman = postman
        self.undertaker = undertaker
        self.librarian = librarian
        self.salmonator = salmonator
        
        self.doorman.assemble(librarian)
        self.postman.assemble(librarian, bookie)
        self.undertaker.assemble(librarian, bookie, postman)
        self.lawyer.assemble(librarian)
        self.enforcer.assemble(librarian)
        self.bookie.assemble(librarian, lawyer, undertaker)
        self.librarian.assemble(self, self.salmonator)
        self.salmonator.assemble(self, self, self.doorman, self.postman,
                                 self.librarian)
        
    def publish(self, *args, **kwargs):
        # This is a temporary fix to force memorypersisters to notify during
        # publishing. Ideally, this would happen immediately after returning.
        self.ingest(*args, **kwargs)
        self.postman.do_mail_run()
        
    def ping(self):
        ''' Queries the persistence provider for availability.
        '''
        return True
        
    def get(self, ghid):
        ''' Returns a packed Golix object.
        '''
        try:
            return self.librarian.retrieve(ghid)
        except KeyError as exc:
            raise DoesNotExist(
                '0x0008: Not found at persister: ' + str(ghid)
            ) from exc
        
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
        NAK/failure is represented by raise RemoteNak
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
        NAK/failure is represented by raise RemoteNak
        '''
        # TODO: figure out something different to do here.
        self.postman._listeners = {}
        return True
        
        
class DiskCachePersister(MemoryPersister):
    ''' Persister that caches to disk.
    Replicate MemoryPersister, just replace Librarian.
    
    Same note re: deprecated.
    '''
    
    def __init__(self, cache_dir):
        super().__init__()
        self.assemble(Doorman(), Enforcer(), Lawyer(), Bookie(),
                      DiskLibrarian(cache_dir=cache_dir), PostOffice(),
                      Undertaker(), SalmonatorNoop())
        
        self.subscribe = self.postman.subscribe
        self.unsubscribe = self.postman.unsubscribe
        # self.silence_notification = self.postman.silence_notification
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
        
        # This is in charge of muting notifications for things we just sent.
        # Maxlen may need some tuning, and this is a bit of a hack.
        self._silenced = collections.deque(maxlen=5)
    
        # NOTE: these must be done as a closure upon functions, instead of
        # normal bound methods, because otherwise the weakrefs used in the
        # post office subscription weakset will be DOA
                
        async def send_subs_update_ax(subscribed_ghid, notification_ghid):
            ''' Deliver any subscription updates.
            
            Also, temporary workaround for not re-delivering updates for
            objects we just sent up.
            '''
            if notification_ghid not in self._silenced:
                try:
                    await self._transport.send(
                        session = self,
                        msg = (bytes(subscribed_ghid) +
                               bytes(notification_ghid)),
                        request_code = self._transport.REQUEST_CODES[
                            'send_subs_update'
                        ],
                        # Note: for now, just don't worry about failures.
                        # await_reply = False
                    )
                    
                # KeyErrors have been happening when connections/sessions
                # disconnect without calling close (or something similar to
                # that, I never tracked down the original source of the
                # problem). This is a quick, hacky fix to remove the bad
                # subscription when we first get an update for an object.
                # This is far, far from ideal, but hey, what can you do
                # without any time to do it in?
                except KeyError as exc:
                    # Well, this is an awkward way to handle an unsub, but it
                    # gets the job done until we fix the way all of this plays
                    # together.
                    await self._transport.unsubscribe_wrapper(
                        session = self,
                        request_body = bytes(subscribed_ghid)
                    )
                    logger.warning(
                        'Application client subscription persisted longer ' +
                        'than the connection itself w/ traceback:\n' +
                        ''.join(traceback.format_exc())
                    )
                
                except Exception:
                    logger.error(
                        'Application client failed to receive sub update at ' +
                        str(subscribed_ghid) + ' for notification ' +
                        str(notification_ghid) + ' w/ traceback: \n' +
                        ''.join(traceback.format_exc())
                    )
        
        def send_subs_update(subscribed_ghid, notification_ghid):
            ''' Send the connection its subscription update.
            Note that this is going to be called from within an event loop,
            but not asynchronously (no await).
            
            TODO: make persisters async.
            '''
            return call_coroutine_threadsafe(
                coro = send_subs_update_ax(subscribed_ghid, notification_ghid),
                loop = self._transport._loop
            )
            
            # asyncio.run_coroutine_threadsafe(
            #     coro = self.send_subs_update_ax(
            #         subscribed_ghid,
            #         notification_ghid
            #     )
            #     loop = self._transport._loop
            # )
            
        self.send_subs_update = send_subs_update
        self.send_subs_update_ax = send_subs_update_ax
        
        super().__init__(*args, **kwargs)
        
    def silence(self, ghid):
        # Silence any notifications for the passed ghid.
        self._silenced.appendleft(ghid)


class PersisterBridgeServer(Autoresponder):
    ''' Serialization mixins for persister bridges.
    '''
    REQUEST_CODES = {
        # Receive an update for an existing object.
        'send_subs_update': b'!!',
    }
    
    def __init__(self, *args, **kwargs):
        self._percore = None
        self._bookie = None
        self._librarian = None
        self._postman = None
        
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
        
        # Create an executor for mail runs.
        self._mailrunner = concurrent.futures.ThreadPoolExecutor()
        # Create an executor for ingesting.
        self._ingester = concurrent.futures.ThreadPoolExecutor()
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            error_lookup = ERROR_CODES,
            *args, **kwargs
        )
        
    def assemble(self, persistence_core, bookie, librarian, postman):
        # Link to the remote core.
        self._percore = weakref.proxy(persistence_core)
        self._bookie = weakref.proxy(bookie)
        self._postman = weakref.proxy(postman)
        self._librarian = weakref.proxy(librarian)
            
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
        # Yep, we're available.
        return b'\x01'
            
    async def publish_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        obj = await self._loop.run_in_executor(
            self._ingester,         # executor
            self._percore.ingest,   # func
            request_body,           # packed
            False                   # remotable
        )
        
        # If we already have an exact copy of the object, do not go on a mail
        # run.
        if obj is not None:
            # Silence any notifications for the object ghid.
            # If it has a frame_ghid, silence that
            try:
                session.silence(obj.frame_ghid)
            # Otherwise, silence the object ghid
            except AttributeError:
                session.silence(obj.ghid)
            # Execute a parallel call to postman.do_mail_run()
            asyncio.ensure_future(self._handle_mail_run())
            
        # We don't need to wait for the mail run to have a successful return
        return b'\x01'
        
    async def _handle_mail_run(self):
        ''' Wraps running a mail run with error handling. This needs to
        be totally autonomous, or asyncio will get angry.
        '''
        try:
            await self._loop.run_in_executor(
                executor = self._mailrunner,
                func = self._postman.do_mail_run
            )
        except Exception:
            logger.error(
                'Error during mail run: \n' + ''.join(traceback.format_exc())
            )
            
    async def get_wrapper(self, session, request_body):
        ''' Deserializes a get request; forwards it to the persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        return self._librarian.retrieve(ghid)
            
    async def subscribe_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        
        def updater(subscribed_ghid, notification_ghid,
                    call=session.send_subs_update):
            call(subscribed_ghid, notification_ghid)
        
        self._postman.subscribe(ghid, updater)
        session._subscriptions[ghid] = updater
        return b'\x01'
            
    async def unsubscribe_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        callback = session._subscriptions[ghid]
        unsubbed = self._postman.unsubscribe(ghid, callback)
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
        ghidlist = self._bookie.bind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def list_debindings_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        ghidlist = self._bookie.debind_status(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    async def query_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        if ghid in self._librarian:
            return b'\x01'
        else:
            return b'\x00'
            
    async def disconnect_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the
        persister.
        '''
        for sub_ghid, sub_callback in session._subscriptions.items():
            self._postman.unsubscribe(sub_ghid, sub_callback)
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
            name = _generate_threadnames('remoupd')[0],
        )
        worker.start()
        
        return b'\x01'
    
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
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
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
        response = self.send_threadsafe(
            session = self.any_session,
            msg = packed,
            request_code = self.REQUEST_CODES['publish']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError(
                'Unknown response code while publishing object:\n' +
                str(response)
            )
    
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
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
        NAK/failure is represented by raise RemoteNak
        '''
        if ghid not in self._subscriptions:
            self.await_session_threadsafe()
            response = self.send_threadsafe(
                session = self.any_session,
                msg = bytes(ghid),
                request_code = self.REQUEST_CODES['subscribe']
            )
            
            if response != b'\x01':
                raise RuntimeError(
                    'Unknown response code while subscribing to ' + str(ghid)
                )
                
        self._subscriptions[ghid].add(callback)
        return True
    
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to
        the passed ghid at the persistence provider. Removes only the
        passed callback.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise RemoteNak
        '''
        if ghid not in self._subscriptions:
            raise ValueError('Not currently subscribed to ' + str(ghid))
            
        self._subscriptions[ghid].discard(callback)
        
        if len(self._subscriptions[ghid]) == 0:
            del self._subscriptions[ghid]
            
            self.await_session_threadsafe()
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
                    'Unknown response code while unsubscribing from ' +
                    str(ghid) + '\nThe persister might still send '
                    'updates, but the callback has been removed.'
                )
                
        return True
    
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise RemoteNak
        '''
        # This would probably be a good time to reconcile states between the
        # persistence provider and our local set of subs!
        self.await_session_threadsafe()
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
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
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
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
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
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
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
        NAK/failure is represented by raise RemoteNak
        '''
        self.await_session_threadsafe()
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
