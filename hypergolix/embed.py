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


# Global dependencies
import logging
import collections
import weakref
import queue
import threading
import traceback
import asyncio
import loopa
import concurrent.futures

from golix import Ghid
from loopa.utils import await_coroutine_threadsafe
from loopa.utils import await_coroutine_loopsafe
from loopa.utils import Triplicate
from loopa.utils import triplicated

# Local dependencies
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api

from .utils import SetMap
from .utils import WeakSetMap
from .utils import ApiID
from .utils import _reap_wrapped_task

from .exceptions import HGXLinkError

from .comms import ConnectionManager
from .comms import WSConnection
from .ipc import IPCClientProtocol

# from .objproxy import ObjBase
# from .objproxy import ObjCore


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports.
__all__ = [
    # 'PersistenceCore',
]


# ###############################################
# Utils
# ###############################################
    
    
class TriplicateAPI(Triplicate, API):
    ''' Combine loopa's triplicate metaclass with hypothetical.API.
    '''


# ###############################################
# Lib
# ###############################################
            

class HGXLink(loopa.TaskCommander, metaclass=TriplicateAPI):
    ''' Amalgamate all of the necessary app functions into a single
    namespace. Also, and threadsafe and loopsafe bindings for stuff.
    '''
    
    @public_api
    def __init__(self, ipc_port=7772, autostart=True, debug=False, aengel=None,
                 *args, threaded=True, ipc_fixture=None, **kwargs):
        ''' Args:
        ipc_port    is self-explanatory
        autostart   True -> immediately start the link
                    False -> app must explicitly start() the link
        debug       Sets debug mode for eg. asyncio
        aengel      Set up a watcher for main thread exits
        '''
        super().__init__(
            reusable_loop = False,
            threaded = threaded,
            debug = debug,
            name = 'hgxlink',
            *args,
            **kwargs
        )
        
        # Handle fixturing (for testing)
        if ipc_fixture is None:
            ipc_protocol = IPCClientProtocol()
            ipc_manager = ConnectionManager(
                connection_cls = WSConnection,
                msg_handler = ipc_protocol
            )
            self.register_task(
                ipc_manager,
                host = 'localhost',
                port = ipc_port,
                tls = False
            )
            self._ipc_manager = ipc_manager
            self._ipc_protocol = ipc_protocol
        else:
            self._ipc_manager = ipc_fixture
            self.register_task(ipc_fixture)
        
        # All of the various object handlers
        # Lookup api_id: async awaitable share handler
        self._share_handlers = {}
        # Lookup api_id: object class
        self._share_typecast = {}
        
        # Lookup for ghid -> object
        self._objs_by_ghid = weakref.WeakValueDictionary()
        
        # Currently unused
        self._nonlocal_handlers = {}
        
        # These are intentionally None.
        self._token = None
        self._whoami = None
        # The startup object ghid gets stored here
        self._startup_obj = None
        
        # Create an executor for awaiting threadsafe callbacks and handlers
        self._executor = concurrent.futures.ThreadPoolExecutor()
        
        if autostart:
            self.start()
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' Fixture all the things!
        '''
        self.state_lookup = {}
        self.share_lookup = {}
        self.api_lookup = {}
        self.deleted = set()
        
    @property
    def whoami(self):
        ''' Read-only access to self._whoami with a raising wrapper if
        it is undefined.
        '''
        if self._whoami is None:
            raise HGXLinkError(
                'Whoami has not been defined. Most likely, no IPC client is ' +
                'currently available.'
            )
        else:
            return self._whoami
            
    @whoami.setter
    def whoami(self, ghid):
        ''' Set whoami, if (and only if) it has yet to be set.
        '''
        if self._whoami is None:
            self._whoami = ghid
        else:
            raise HGXLinkError(
                'Whoami has already been defined. It must be cleared before ' +
                'being re-set.'
            )
    
    @property
    def app_token(self):
        ''' Read-only access to the current app token.
        '''
        if self._token is None:
            raise HGXLinkError('No token available.')
        else:
            return self._token
            
    @app_token.setter
    def app_token(self, value):
        ''' Set app_token, if (and only if) it has yet to be set.
        '''
        if self._token is None:
            self._token = value
        else:
            raise HGXLinkError(
                'Token already set. It must be cleared before being re-set.'
            )
    
    @triplicated
    async def register_nonlocal_handler(self, api_id, handler):
        ''' Call this to register a handler for any private objects
        created by the same hypergolix identity and the same hypergolix
        application, but at a separate, concurrent session.
        
        This HANDLER will be called from within the IPC embed's internal
        event loop.
        
        This METHOD must be called from within the IPC embed's internal
        event loop.
        '''
        raise NotImplementedError()
        api_id = self._normalize_api_id(api_id)
        
        # self._nonlocal_handlers = {}
    
    @triplicated
    async def register_share_handler(self, api_id, handler):
        ''' Call this to register a handler for an object shared by a
        different hypergolix identity, or the same hypergolix identity
        but a different application. Any api_id can have at most one
        share handler, across ALL forms of callback (internal,
        threadsafe, loopsafe).
        
        The handler will be called as:
            await handler(ghid, origin, api_id)
        
        This HANDLER will be called from within the IPC embed's internal
        event loop.
        
        This METHOD must be called from within the IPC embed's internal
        event loop.
        '''
        if not isinstance(api_id, ApiID):
            raise TypeError('api_id must be ApiID.')
        
        await self._ipc_manager.register_api(api_id)
        
        # Any handlers passed to us this way can already be called natively
        # from withinour own event loop, so they just need to be wrapped such
        # that they never raise.
        async def wrap_handler(*args, handler=handler, **kwargs):
            try:
                await handler(*args, **kwargs)
                
            except asyncio.CancelledError:
                raise
                
            except Exception:
                logger.error(
                    'Error while running share handler. Traceback:\n' +
                    ''.join(traceback.format_exc())
                )
        
        # Hey, look at this! Because we're running a single-threaded event loop
        # and not ceding flow control to the loop, we don't need to worry about
        # synchro primitives here!
        self._share_handlers[api_id] = wrap_handler
        
    @triplicated
    async def deregister_share_handler(self, api_id):
        ''' Removes a share handler.
        '''
        await self._ipc_manager.deregister_api(api_id)
        try:
            del self._share_handlers[api_id]
        except KeyError:
            logger.warning('No existing share handler for ' + str(api_id))
    
    def wrap_threadsafe(self, callback):
        ''' Call this to register a handler for an object shared by a
        different hypergolix identity, or the same hypergolix identity
        but a different application. Any api_id can have at most one
        share handler, across ALL forms of callback (internal,
        threadsafe, loopsafe).
        
        typecast determines what kind of ObjProxy class the object will
        be cast into before being passed to the handler.
        
        This HANDLER will be called from within a single-use, dedicated
        thread.
        
        This METHOD must be called from a different thread than the IPC
        embed's internal event loop.
        '''
        # For simplicity, wrap the handler, so that any shares can be called
        # normally from our own event loop.
        async def wrapped_handler(*args, self_weakref=weakref.ref(self),
                                  func=callback):
            ''' Wrap the handler in run_in_executor.
            '''
            self = self_weakref()
            if self is not None:
                await self._loop.run_in_executor(
                    self._executor,
                    func,
                    *args
                )
        
        return wrapped_handler
    
    def wrap_loopsafe(self, callback, target_loop):
        ''' Call this to register a handler for an object shared by a
        different hypergolix identity, or the same hypergolix identity
        but a different application. Any api_id can have at most one
        share handler, across ALL forms of callback (internal,
        threadsafe, loopsafe).
        
        typecast determines what kind of ObjProxy class the object will
        be cast into before being passed to the handler.
        
        This HANDLER will be called within the specified event loop,
        also implying the specified event loop context (ie thread).
        
        This METHOD must be called from a different event loop than the
        IPC embed's internal event loop. It is internally loopsafe, and
        need not be wrapped by run_coroutine_loopsafe.
        '''
        # For simplicity, wrap the handler, so that any shares can be called
        # normally from our own event loop.
        async def wrapped_handler(*args, target_loop=target_loop,
                                  coro=callback):
            ''' Wrap the handler in await_coroutine_loopsafe.
            '''
            await await_coroutine_loopsafe(
                coro = coro(*args),
                target_loop = target_loop
            )
        
        return wrapped_handler
        
    @triplicated
    async def register_token(self, token=None):
        ''' Registers the application as using a particular token, OR
        gets a new token. Returns the ghid for the startup object (if
        one has already been defined), or None. Tokens are be available
        in app_token.
        '''
        # The result of this will be the actual token.
        self.app_token = await self._ipc_manager.set_token(token)
        
        if token is not None:
            self._startup_obj = await self._ipc_manager.get_startup_obj()
        
        # Let applications manually request the startup object, so that they
        # can deal with casting it appropriately.
        return self._startup_obj
        
    @triplicated
    async def register_startup_obj(self, obj):
        ''' Registers the object as the startup object.
        '''
        await self._ipc_manager.register_startup_obj(obj._hgx_ghid)
        
    @triplicated
    async def deregister_startup(self, obj):
        ''' Inverse of the above.
        '''
        await self._ipc_manager.deregister_startup_obj()
        
    @triplicated
    async def get(self, cls, ghid):
        ''' Pass to connection manager. Also, turn the object into the
        specified class.
        '''
        (address,
         author,
         state,
         is_link,
         api_id,
         private,
         dynamic,
         _legroom) = await self._ipc_manager.get_ghid(ghid)
            
        if is_link:
            # First discard the object, since we can't support it.
            await self._ipc_manager.discard_ghid(ghid)
            
            # Now raise.
            raise NotImplementedError(
                'Hypergolix does not yet support nested links to other '
                'dynamic objects.'
            )
            # link = Ghid.from_bytes(state)
            # state = await self._get(link)
        
        state = await cls.hgx_unpack(state)
        obj = cls(
            hgxlink = self,
            ipc_manager = self._ipc_manager,
            state = state,
            api_id = api_id,
            dynamic = dynamic,
            private = private,
            ghid = address,
            binder = author,
            _legroom = _legroom,
        )
            
        # Don't forget to add it to local lookup so we can apply updates.
        self._objs_by_ghid[obj._hgx_ghid] = obj
        
        return obj
        
    @triplicated
    async def new(self, cls, state, api_id=None, dynamic=True, private=False,
                  _legroom=None, *args, **kwargs):
        ''' Create a new object w/ class cls.
        '''
        if _legroom is None:
            _legroom = self._legroom
            
        obj = cls(
            hgxlink = self,
            ipc_manager = self._ipc_manager,
            _legroom = _legroom,
            state = state,
            api_id = api_id,
            dynamic = dynamic,
            private = private,
            binder = self.whoami,
            *args, **kwargs
        )
        
        packed_state = await obj.hgx_pack(state)
        
        address = await self._ipc_manager.new_ghid(
            packed_state,
            api_id,
            dynamic,
            private,
            _legroom
        )
        
        obj._hgx_ghid = address
        # Don't forget to add it to local lookup so we can apply updates.
        self._objs_by_ghid[obj._hgx_ghid] = obj
        return obj
        
    @public_api
    async def _pull_state(self, ghid, state):
        ''' Applies an incoming state update.
        '''
        if isinstance(state, Ghid):
            raise NotImplementedError('Linked objects not yet supported.')
        
        try:
            obj = self._objs_by_ghid[ghid]
            
        except KeyError:
            # Just discard the object, since we don't actually have a copy of
            # it locally.
            logger.warning(
                'Received an object update, but the object was no longer '
                'contained in memory. Discarding its subscription: ' +
                str(ghid) + '.'
            )
            await self._ipc_manager.discard_ghid(ghid)
            
        else:
            logger.debug(
                'Received update for ' + str(ghid) + '; forcing pull.'
            )
            await obj._hgx_force_pull(state)
            
    @_pull_state.fixture
    async def _pull_state(self, ghid, state):
        ''' Fixture for applying incoming state update.
        '''
        self.state_lookup[ghid] = state
            
    @public_api
    async def handle_share(self, ghid, origin, api_id):
        ''' Handles an incoming shared object.
        '''
        # This is async, which is single-threaded, so there's no race condition
        try:
            handler = self._share_handlers[api_id]
            
        except KeyError:
            logger.warning(
                'Received a share for an API_ID that was lacking a handler or '
                'typecast. Deregistering the API_ID.'
            )
            await self._ipc_manager.deregister_api(api_id)
            
        else:
            # Run the share handler concurrently, so that we can release the
            # req/res session
            share_task = asyncio.ensure_future(handler(ghid, origin, api_id))
            share_task.add_done_callback(_reap_wrapped_task)
            
    @handle_share.fixture
    async def handle_share(self, ghid, origin, api_id):
        ''' Fixture handling an incoming share object.
        '''
        self.share_lookup[ghid] = origin
        self.api_lookup[ghid] = api_id
            
    @public_api
    async def handle_delete(self, ghid):
        ''' Applies an incoming delete.
        '''
        try:
            obj = self._objs_by_ghid[ghid]
        
        except KeyError:
            logger.debug(str(ghid) + ' not known to IPCEmbed.')
        
        else:
            await obj._hgx_force_delete()
            del self._objs_by_ghid[ghid]
            
    @handle_delete.fixture
    async def handle_delete(self, ghid):
        ''' Fixtures handling an incoming delete.
        '''
        self.deleted.add(ghid)
