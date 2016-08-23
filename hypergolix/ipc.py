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


Some thoughts:

Misc extras: 
    + More likely than not, all persistence remotes should also use a 
        single autoresponder, through the salmonator. Salmonator should
        then be moved into hypergolix.remotes instead of .persistence.
    + At least for now, applications must ephemerally declare themselves
        capable of supporting a given API. Note, once again, that these
        api_id registrations ONLY APPLY TO UNSOLICITED OBJECT SHARES!
    
It'd be nice to remove the msgpack dependency in utils.IPCPackerMixIn.
    + Could use very simple serialization instead.
    + Very heavyweight for such a silly thing.
    + It would take very little time to remove.
    + This should wait until we have a different serialization for all
        of the core bootstrapping _GAOs. This, in turn, should wait 
        until after SmartyParse is converted to be async.
        
IPC Apps should not have access to objects that are not _Dispatchable.
    + Yes, this introduces some overhead. Currently, it isn't the most
        efficient abstraction.
    + Non-dispatchable objects are inherently un-sharable. That's the
        most fundamental issue here.
    + Note that private objects are also un-sharable, so they should be
        able to bypass some overhead in the future (see below)
    + Future effort will focus on making the "dispatchable" wrapper as
        efficient an abstraction as possible.
    + This basically makes a judgement call that everything should be
        sharable.
'''

# External dependencies
import abc
import os
import warnings
import weakref
import threading
import collections

from golix import Ghid

import concurrent
import asyncio
import websockets
from websockets.exceptions import ConnectionClosed

import time
# import string
import traceback

# Intrapackage dependencies
from .exceptions import HandshakeError
from .exceptions import HandshakeWarning
from .exceptions import IPCError

from .utils import IPCPackerMixIn
from .utils import call_coroutine_threadsafe
from .utils import await_sync_future
from .utils import WeakSetMap
from .utils import SetMap
from .utils import _generate_threadnames

from .comms import _AutoresponderSession
from .comms import Autoresponder
from .comms import AutoresponseConnector

from .dispatch import _Dispatchable
from .dispatch import _DispatchableState
from .dispatch import _AppDef

from .objproxy import ObjProxyBase


# ###############################################
# Boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Inquisitor', 
]


# ###############################################
# Library
# ###############################################
            
            
# Identity here can be either a sender or recipient dependent upon context
_ShareLog = collections.namedtuple(
    typename = '_ShareLog',
    field_names = ('ghid', 'identity'),
)


class IPCCore(Autoresponder, IPCPackerMixIn):
    ''' The core IPC system, including the server autoresponder. Add the
    individual IPC servers to the IPC Core.
    
    NOTE: this class, with the exception of initialization, is wholly
    asynchronous. Outside entities should call into it using 
    utils.call_coroutine_threadsafe. Any thread-wrapping that needs to
    happen to break in-loop chains should also be executed in the 
    outside entity.
    '''
    REQUEST_CODES = {
        # Receive a declared startup obj.
        'send_startup': b':O',
        # Receive a new object from a remotely concurrent instance of self.
        'send_object': b'+O',
        # Receive an update for an existing object.
        'send_update': b'!O',
        # Receive an update that an object has been deleted.
        'send_delete': b'XO',
        # Receive an object that was just shared with us.
        'send_share': b'^O',
        # Receive an async notification of a sharing failure.
        'notify_share_failure': b'^F',
        # Receive an async notification of a sharing success.
        'notify_share_success': b'^S',
    }
    
    def __init__(self, *args, **kwargs):
        ''' Initialize the autoresponder and get it ready to go.
        '''
        self._dispatch = None
        self._oracle = None
        self._golcore = None
        self._rolodex = None
        self._salmonator = None
        
        # Some distributed objects to be bootstrapped
        # Set of incoming shared ghids that had no endpoint
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = None
        # Setmap-like lookup for share acks that had no endpoint
        # <app token>: set(<ghids>)
        self._orphan_share_acks = None
        # Setmap-like lookup for share naks that had no endpoint
        # <app token>: set(<ghids>)
        self._orphan_share_naks = None
        
        # Lookup <server_name>: <server>
        self._ipc_servers = {}
        
        # Lookup <app token>: <connection/session/endpoint>
        self._endpoint_from_token = weakref.WeakValueDictionary()
        # Reverse lookup <connection/session/endpoint>: <app token>
        self._token_from_endpoint = weakref.WeakKeyDictionary()
        
        # Lookup <api ID>: set(<connection/session/endpoint>)
        self._endpoints_from_api = WeakSetMap()
        
        # This lookup directly tracks who has a copy of the object
        # Lookup <object ghid>: set(<connection/session/endpoint>)
        self._update_listeners = WeakSetMap()
        
        req_handlers = {
            # Get new app token
            b'+T': self.new_token_wrapper,
            # Register existing app token
            b'$T': self.set_token_wrapper,
            # Register an API
            b'$A': self.add_api_wrapper,
            # Deegister an API
            b'XA': self.remove_api_wrapper,
            # Register a startup object
            b'$O': self.register_startup_wrapper,
            # Whoami?
            b'?I': self.whoami_wrapper,
            # Get object
            b'>O': self.get_object_wrapper,
            # New object
            b'+O': self.new_object_wrapper,
            # Sync object
            b'~O': self.sync_object_wrapper,
            # Update object
            b'!O': self.update_object_wrapper,
            # Share object
            b'@O': self.share_object_wrapper,
            # Freeze object
            b'*O': self.freeze_object_wrapper,
            # Hold object
            b'#O': self.hold_object_wrapper,
            # Discard object
            b'-O': self.discard_object_wrapper,
            # Delete object
            b'XO': self.delete_object_wrapper,
        }
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            *args, **kwargs
        )
        
    def assemble(self, golix_core, oracle, dispatch, rolodex, salmonator):
        # Chicken, egg, etc.
        self._golcore = weakref.proxy(golix_core)
        self._oracle = weakref.proxy(oracle)
        self._dispatch = weakref.proxy(dispatch)
        self._rolodex = weakref.proxy(rolodex)
        self._salmonator = weakref.proxy(salmonator)
        
    def bootstrap(self, incoming_shares, orphan_acks, orphan_naks):
        ''' Initializes distributed state.
        '''
        # Set of incoming shared ghids that had no endpoint
        # set(<ghid, sender tuples>)
        self._orphan_incoming_shares = incoming_shares
        # Setmap-like lookup for share acks that had no endpoint
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_acks = orphan_acks
        # Setmap-like lookup for share naks that had no endpoint
        # <app token>: set(<ghid, recipient tuples>)
        self._orphan_share_naks = orphan_naks
        
    def add_ipc_server(self, server_name, server_class, *args, **kwargs):
        ''' Automatically sets up an IPC server connected to the IPCCore
        system. Just give it the server_class, eg WSBasicServer, and
        all of the *args and **kwargs will be passed to the server's
        __init__.
        '''
        if server_name in self._ipc_servers:
            raise ValueError(
                'Cannot overwrite an existing IPC server. Pop it first.'
            )
        
        # We could maybe do this elsewhere, but adding an IPC server isn't 
        # really performance-critical, especially not now.
        class LinkedServer(AutoresponseConnector, server_class):
            pass
            
        self._ipc_servers[server_name] = \
            LinkedServer(autoresponder=self, *args, **kwargs)

    def pop_ipc_server(self, server_name):
        ''' Removes and returns the IPC server. It may then be cleanly 
        shut down (manually).
        '''
        self._ipc_servers.pop(server_name)
        
    async def notify_update(self, ghid, deleted=False):
        ''' Updates all ipc endpoints with copies of the object.
        '''
        callsheet = self._update_listeners.get_any(ghid)
        
        # Go ahead and distribute it to the appropriate endpoints.
        if deleted:
            await self.distribute_to_endpoints(
                callsheet,
                self.send_delete,
                ghid
            )
        else:
            await self.distribute_to_endpoints(
                callsheet,
                self.send_update,
                ghid
            )
            
    async def process_share(self, target, sender):
        ''' Manage everything about processing incoming shares.
        '''
        # Build a callsheet for the target.
        callsheet = await self._make_callsheet(target)
        # Go ahead and distribute it to the appropriate endpoints.
        await self.distribute_to_endpoints(
            callsheet,
            self.send_share,
            target,
            sender
        )
        
    async def process_share_success(self, target, recipient, tokens):
        ''' Wrapper to notify all requestors of share success.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._endpoint_from_token[token])
            except KeyError:
                pass
        
        # Distribute the share success to all apps that requested its delivery
        await self._robodialer(
            self.notify_share_success,
            callsheet,
            target,
            recipient
        )
    
    async def process_share_failure(self, target, recipient, tokens):
        ''' Wrapper to notify all requestors of share failure.
        '''
        callsheet = set()
        for token in tokens:
            # Escape any keys that have gone missing during the rat race
            try:
                callsheet.add(self._endpoint_from_token[token])
            except KeyError:
                pass
        
        # Distribute the share success to all apps that requested its delivery
        await self._robodialer(
            self.notify_share_failure,
            callsheet,
            target,
            recipient
        )
    
    async def _make_callsheet(self, ghid, skip_endpoint=None):
        ''' Generates a callsheet (set of tokens) for the dispatchable
        obj.
        
        The callsheet is generated from app_tokens, so that the actual 
        distributor can kick missing tokens back for safekeeping.
        
        TODO: make this a "private" method -- aka, remove this from the
        rolodex share handling.
        TODO: make this exclusively apply to object sharing, NOT to obj
        updates, which uses _update_listeners directly and exclusively.
        '''
        try:
            obj = self._oracle.get_object(
                gaoclass = _Dispatchable, 
                ghid = ghid,
                dispatch = self._dispatch,
                ipc_core = self
            )
        
        except:
            # At some point we'll need some kind of proper handling for this.
            logger.error(
                'Failed to retrieve object at ' + str(ghid) + '\n' + 
                ''.join(traceback.format_exc())
            )
            return set()
        
        # Create a temporary set for relevant endpoints
        callsheet = set()
        
        private_owner = self._dispatch.get_parent_token(ghid)
        if private_owner:
            try:
                private_endpoint = self._endpoint_from_token[private_owner]
            except KeyError:
                logger.warning(
                    'Could not retrieve the object\'s private owner, with '
                    'traceback: \n' + ''.join(traceback.format_exc())
                )
            else:
                callsheet.add(private_endpoint)
            
        else:
            # Add any endpoints based on their tracking of the api.
            logger.debug(
                'Object has no private owner; generating list of approx. ' + 
                str(len(self._endpoints_from_api.get_any(obj.api_id))) + 
                ' interested API endpoints.'
            )
            callsheet.update(self._endpoints_from_api.get_any(obj.api_id))
            
            # Add any endpoints based on their existing listening status.
            logger.debug(
                'Adding an additional approx ' + 
                str(len(self._update_listeners.get_any(obj.ghid))) + 
                ' explicit object listeners.'
            )
            callsheet.update(self._update_listeners.get_any(obj.ghid))
            
        # And discard any skip_endpoint, if it's there.
        callsheet.discard(skip_endpoint)
        
        logger.debug('Callsheet generated: ' + repr(callsheet))
        
        return callsheet
        
    async def distribute_to_endpoints(self, callsheet, distributor, *args):
        ''' For each app token in the callsheet, awaits the distributor,
        passing it the endpoint and *args.
        '''
        if len(callsheet) == 0:
            logger.info('No applications are available to handle the request.')
            await self._handle_orphan_distr(distributor, *args)
            
        else:
            await self._robodialer(
                self._distr_single, 
                callsheet, 
                distributor, 
                *args
            )
            
    async def _robodialer(self, caller, callsheet, *args):
        tasks = []
        for endpoint in callsheet:
            # For each endpoint...
            tasks.append(
                # ...in parallel, schedule a single execution
                asyncio.ensure_future(
                    # Of a _distribute_single call to the distributor.
                    caller(endpoint, *args)
                )
            )
        await asyncio.gather(*tasks)
                    
    async def _distr_single(self, endpoint, distributor, *args):
        ''' Distributes a single request to a single token.
        '''
        try:
            await distributor(endpoint, *args)
            
        except:
            logger.error(
                'Error while contacting endpoint: \n' + 
                ''.join(traceback.format_exc())
            )
            
    async def _handle_orphan_distr(self, distributor, *args):
        ''' This is what happens when our callsheet has zero length.
        Also, this is how we get ants.
        '''
        # Save incoming object shares.
        if distributor is self.send_share:
            sharelog = _ShareLog(*args)
            self._orphan_incoming_shares.add(sharelog)
    
        # But ignore everything else.
    
    async def _obj_sender(self, endpoint, ghid, request_code):
        ''' Generic flow control for sending an object.
        '''
        try:
            obj = self._oracle.get_object(
                gaoclass = _Dispatchable, 
                ghid = ghid,
                dispatch = self._dispatch,
                ipc_core = self
            )
            
        except:
            # At some point we'll need some kind of proper handling for this.
            logger.error(
                'Failed to retrieve object at ' + str(ghid) + '\n' + 
                ''.join(traceback.format_exc())
            )
            
        else:
            try:
                response = await self.send(
                    session = endpoint,
                    msg = self._pack_object_def(
                        obj.ghid,
                        obj.author,
                        obj.state,
                        False, # is_link is currently unsupported
                        obj.api_id,
                        None,
                        obj.dynamic,
                        None
                    ),
                    request_code = self.REQUEST_CODES[request_code],
                )
                
            except:
                logger.error(
                    'Application client failed to receive object at ' + 
                    str(ghid) + ' w/ the following traceback: \n' + 
                    ''.join(traceback.format_exc())
                )
                
            else:
                # Don't forget to track who has the object
                self._update_listeners.add(ghid, endpoint)
        
    async def set_token_wrapper(self, endpoint, request_body):
        ''' With the current paradigm of independent app starting, this
        is the "official" start of the application. We set our lookups 
        for endpoint <--> token, and then send all startup objects.
        '''
        app_token = request_body[0:4]
        
        if app_token in self._endpoint_from_token:
            raise RuntimeError(
                'Attempt to reregister a new endpoint for the same token. '
                'Each app token must have exactly one endpoint.'
            )
        
        appdef = _AppDef(app_token)
        # Check our app token
        self._dispatch.start_application(appdef)
        
        # TODO: should these be enclosed within an operations lock?
        self._endpoint_from_token[app_token] = endpoint
        self._token_from_endpoint[endpoint] = app_token
        
        startup_ghid = self._dispatch.get_startup_obj(app_token)
        if startup_ghid is not None:
            await self.send_startup(endpoint, startup_ghid)
        
        return b'\x01'
    
    async def new_token_wrapper(self, endpoint, request_body):
        ''' Ignore body, get new token from dispatch, and proceed.
        
        Obviously doesn't require an existing app token.
        '''
        appdef = self._dispatch.register_application()
        app_token = appdef.app_token
        
        # TODO: should these be enclosed within an operations lock?
        self._endpoint_from_token[app_token] = endpoint
        self._token_from_endpoint[endpoint] = app_token
        
        return app_token
    
    async def send_startup(self, endpoint, ghid):
        ''' Sends the endpoint a startup object.
        '''
        await self._obj_sender(endpoint, ghid, 'send_startup')
    
    async def send_share(self, endpoint, ghid, sender):
        ''' Notifies the endpoint of a shared object, for which it is 
        interested. This will never be called when the object was 
        created concurrently by another remote instance of the agent
        themselves, just when someone else shares the object with the
        agent.
        '''
        # Note: currently we're actually sending the whole object update, not
        # just a notification of update address.
        # Note also that we're not currently doing anything about who send the
        # share itself.
        await self._obj_sender(endpoint, ghid, 'send_share')
        
    async def send_object(self, endpoint, ghid):
        ''' Sends a new object to the emedded client. This is called
        when another (concurrent and remote) instance of the logged-in 
        agent has created an object that local applications might be
        interested in.
        
        NOTE: This is not currently invoked anywhere, because we don't
        currently have a mechanism to push these things between multiple
        concurrent Hypergolix instances. Put simply, we're lacking a 
        notification mechanism. See note in Dispatcher.
        '''
        # Note: currently we're actually sending the whole object update, not
        # just a notification of update address.
        await self._obj_sender(endpoint, ghid, 'send_object')
    
    async def send_update(self, endpoint, ghid):
        ''' Sends an updated object to the emedded client.
        '''
        # Note: currently we're actually sending the whole object update, not
        # just a notification of update address.
        await self._obj_sender(endpoint, ghid, 'send_update')
        
    async def send_delete(self, endpoint, ghid):
        ''' Notifies the endpoint that the object has been deleted 
        upstream.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('ghid must be type Ghid or similar.')
        
        try:
            response = await self.send(
                session = self,
                msg = bytes(ghid),
                request_code = self.REQUEST_CODES['send_delete'],
                # Note: for now, just don't worry about failures.
                # await_reply = False
            )
                
        except:
            logger.error(
                'Application client failed to receive delete at ' + 
                str(ghid) + ' w/ the following traceback: \n' + 
                ''.join(traceback.format_exc())
            )
        
    async def notify_share_success(self, endpoint, ghid, recipient):
        ''' Notifies the embedded client of a successful share.
        '''
        try:
            response = await self.send(
                session = endpoint,
                msg = bytes(ghid) + bytes(recipient),
                request_code = self.REQUEST_CODES['notify_share_success'],
                # Note: for now, just don't worry about failures.
                # await_reply = False
            )
            
        except:
            logger.error(
                'Application client failed to receive share success at ' + 
                str(ghid) + ' w/ the following traceback: \n' + 
                ''.join(traceback.format_exc())
            )
        
    async def notify_share_failure(self, endpoint, ghid, recipient):
        ''' Notifies the embedded client of an unsuccessful share.
        '''
        try:
            response = await self.send(
                session = endpoint,
                msg = bytes(ghid) + bytes(recipient),
                request_code = self.REQUEST_CODES['notify_share_failure'],
                # Note: for now, just don't worry about failures.
                # await_reply = False
            )
        except:
            logger.error(
                'Application client failed to receive share failure at ' + 
                str(ghid) + ' w/ the following traceback: \n' + 
                ''.join(traceback.format_exc())
            )
        
    async def add_api_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        
        Requires existing app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token prior to adding APIs.')
            
        if len(request_body) != 65:
            raise ValueError('Invalid API ID format.')
            
        self._endpoints_from_api.add(request_body, endpoint)
        
        return b'\x01'
        
    async def remove_api_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        
        Requires existing app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token prior to removing APIs.')
            
        if len(request_body) != 65:
            raise ValueError('Invalid API ID format.')
            
        self._endpoints_from_api.discard(request_body, endpoint)
        
        return b'\x01'
        
    async def whoami_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.whoami into a bytes return.
        
        Does not require an existing app token.
        '''
        ghid = self._golcore.whoami
        return bytes(ghid)
        
    async def register_startup_wrapper(self, endpoint, request_body):
        ''' Wraps object sharing. Requires existing app token. Note that
        it will return successfully immediately, regardless of whether
        or not the share was eventually accepted by the recipient.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError(
                'Must register app token before registering startup objects.'
            )
            
        ghid = Ghid.from_bytes(request_body)
        requesting_token = self._token_from_endpoint[endpoint]
        self._dispatch.register_startup(requesting_token, ghid)
        return b'\x01'
        
    async def get_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.get_object into a bytes return.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        self._update_listeners.add(ghid, endpoint)
            
        if isinstance(obj.state, Ghid):
            is_link = True
            state = bytes(obj.state)
        else:
            is_link = False
            state = obj.state
            
        # For now, anyways.
        # Note: need to add some kind of handling for legroom.
        _legroom = None
        
        return self._pack_object_def(
            obj.ghid,
            obj.author,
            state,
            is_link,
            obj.api_id,
            obj.private,
            obj.dynamic,
            _legroom
        )
        
    async def new_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_object into a bytes return.
        
        Does not require an existing app token.
        '''
        (
            address, # Unused and set to None.
            author, # Unused and set to None.
            state, 
            is_link, 
            api_id, 
            private, 
            dynamic, 
            _legroom
        ) = self._unpack_object_def(request_body)
        
        app_token = self._token_from_endpoint[endpoint]
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
        
        obj = self._oracle.new_object(
            gaoclass = _Dispatchable,
            dispatch = self._dispatch,
            ipc_core = self,
            state = _DispatchableState(api_id, state),
            dynamic = dynamic,
            _legroom = _legroom,
            api_id = api_id,
        )
            
        # Add the endpoint as a listener.
        self._update_listeners.add(obj.ghid, endpoint)
        
        # If the object is private, register it as such.
        if private:
            logger.debug(
                'Creating private object for ' + str(endpoint) + 
                '; bypassing distribution.'
            )
            self._dispatch.register_private(app_token, obj.ghid)
            
        # Otherwise, make sure to notify any other interested parties.
        else:
            # TODO: change send_object to just send the ghid, not the object
            # itself, so that the app doesn't have to be constantly discarding
            # stuff it didn't create?
            callsheet = await self._make_callsheet(
                obj.ghid, 
                skip_endpoint = endpoint
            )
             
            # Note that self._obj_sender handles adding update listeners
            await self.distribute_to_endpoints(
                callsheet,
                self.send_share,
                obj.ghid,
                self._golcore.whoami
            )
        
        return bytes(obj.ghid)
        
    async def update_object_wrapper(self, endpoint, request_body):
        ''' Called to handle downstream application update requests.
        '''
        logger.debug('Handling update request from ' + str(endpoint))
        (
            address,
            author, # Unused and set to None.
            state, 
            is_link, 
            api_id, # Unused and set to None.
            private, # Unused and set to None.
            dynamic, # Unused and set to None.
            _legroom # Unused and set to None.
        ) = self._unpack_object_def(request_body)
        
        if is_link:
            raise NotImplementedError('Linked objects are not yet supported.')
            state = Ghid.from_bytes(state)
            
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = address,
            dispatch = self._dispatch,
            ipc_core = self
        )
        obj.update(state)
        
        if not obj.private:
            logger.debug('Object is NOT private; distributing.')
            callsheet = await self._make_callsheet(
                obj.ghid, 
                skip_endpoint = endpoint
            )
                
            await self.distribute_to_endpoints(
                callsheet,
                self.send_update,
                obj.ghid
            )
        else:
            logger.debug('Object IS private; skipping distribution.')
        
        return b'\x01'
        
    async def sync_object_wrapper(self, endpoint, request_body):
        ''' Requires existing app token. Will not return the update; if
        a new copy of the object was available, it will be sent 
        independently.
        '''
        ghid = Ghid.from_bytes(request_body)
        self._salmonator.pull(ghid)
        return b'\x01'
        
    async def share_object_wrapper(self, endpoint, request_body):
        ''' Wraps object sharing. Requires existing app token. Note that
        it will return successfully immediately, regardless of whether
        or not the share was eventually accepted by the recipient.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token before sharing objects.')
            
        ghid = Ghid.from_bytes(request_body[0:65])
        recipient = Ghid.from_bytes(request_body[65:130])
        requesting_token = self._token_from_endpoint[endpoint]
        self._rolodex.share_object(ghid, recipient, requesting_token)
        return b'\x01'
        
    async def freeze_object_wrapper(self, endpoint, request_body):
        ''' Wraps object freezing.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        frozen_address = obj.freeze()
        
        return bytes(frozen_address)
        
    async def hold_object_wrapper(self, endpoint, request_body):
        ''' Wraps object holding.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        obj.hold()
        return b'\x01'
        
    async def discard_object_wrapper(self, endpoint, request_body):
        ''' Wraps object discarding. 
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        self._update_listeners.discard(ghid, endpoint)
        return b'\x01'
        
    async def delete_object_wrapper(self, endpoint, request_body):
        ''' Wraps object deletion with a packable format.
        
        Does not require an existing app token.
        '''
        ghid = Ghid.from_bytes(request_body)
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable, 
            ghid = ghid,
            dispatch = self._dispatch,
            ipc_core = self
        )
        self._update_listeners.discard(ghid, endpoint)
        obj.delete()
        return b'\x01'
        
        
class IPCEmbed(Autoresponder, IPCPackerMixIn):
    ''' The thing you actually put in your app. 
    '''
    REQUEST_CODES = {
        # # Get new app token
        'new_token': b'+T',
        # # Register existing app token
        'set_token': b'$T',
        # Register an API
        'register_api': b'$A',
        # Register an API
        'deregister_api': b'XA',
        # Register a startup object
        'register_startup': b'$O',
        # Whoami?
        'whoami': b'?I',
        # Get object
        'get_object': b'>O',
        # New object
        'new_object': b'+O',
        # Sync object
        'sync_object': b'~O',
        # Update object
        'update_object': b'!O',
        # Share object
        'share_object': b'@O',
        # Freeze object
        'freeze_object': b'*O',
        # Hold object
        'hold_object': b'#O',
        # Discard an object
        'discard_object': b'-O',
        # Delete object
        'delete_object': b'XO',
    }
    
    def __init__(self, *args, **kwargs):
        ''' Initializes self.
        '''  
        self._token = None
        self._whoami = None
        self._ipc = None
        self._startup_obj = None
        self._legroom = 7
        
        # Lookup for ghid -> object
        self._objs_by_ghid = weakref.WeakValueDictionary()
        
        # All of the various object handlers
        # Lookup api_id: async awaitable share handler
        self._share_handlers = {}
        # Lookup api_id: object class
        self._share_typecast = {}
        
        # Currently unused
        self._nonlocal_handlers = {}
        
        # Create an executor for awaiting threadsafe callbacks and handlers
        self._executor = concurrent.futures.ThreadPoolExecutor()
        
        # Note that these are only for unsolicited contact from the server.
        req_handlers = {
            # Receive a startup object.
            b':O': self.deliver_startup_wrapper,
            # Receive a new object from a remotely concurrent instance of self.
            b'+O': self.deliver_object_wrapper,
            # Receive a new object from a share.
            b'^O': self.deliver_share_wrapper,
            # Receive an update for an existing object.
            b'!O': self.update_object_wrapper,
            # Receive a delete command.
            b'XO': self.delete_object_wrapper,
            # Receive an async notification of a sharing failure.
            b'^F': self.notify_share_failure_wrapper,
            # Receive an async notification of a sharing success.
            b'^S': self.notify_share_success_wrapper,
        }
        
        super().__init__(
            req_handlers = req_handlers,
            success_code = b'AK',
            failure_code = b'NK',
            # Note: can also add error_lookup = {b'er': RuntimeError}
            *args, **kwargs
        )
        
    @property
    def whoami(self):
        ''' Read-only access to self._whoami with a raising wrapper if
        it is undefined.
        '''
        if self._whoami is not None:
            return self._whoami
        else:
            raise RuntimeError(
                'Whoami has not been defined. Most likely, no IPC client is '
                'currently available.'
            )
        
    async def _get_whoami(self):
        ''' Pulls identity fingerprint from hypergolix IPC.
        '''
        raw_ghid = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['whoami']
        )
        return Ghid.from_bytes(raw_ghid)
        
    async def _add_ipc(self, client_class, *args, **kwargs):
        ''' Automatically sets up an IPC client connected to hypergolix.
        Just give it the client_class, eg WSBasicClient, and all of the 
        *args and **kwargs will be passed to the client's __init__.
        '''
        if self._ipc is not None:
            raise RuntimeError(
                'Must clear existing ipc before establishing a new one.'
            )
        
        # We could maybe do this elsewhere, but adding an IPC client isn't 
        # really performance-critical, especially not now.
        class LinkedClient(AutoresponseConnector, client_class):
            async def loop_stop(client, *args, **kwargs):
                ''' Clear both the app token and whoami in the embedded
                link when the loop stops. Ideally, this would also be
                called when a connection drops. TODO: that. Or, perhaps
                something similar, but after we've assimilated multiple
                loopertroopers into a single event loop.
                '''
                # This is a closure around parent self.
                self._startup_obj = None
                self._whoami = None
                await super().loop_stop(*args, **kwargs)
            
        self._ipc = LinkedClient(autoresponder=self, *args, **kwargs)
        await self.await_session_async()
        self._whoami = await self._get_whoami()
        
    async def add_ipc_loopsafe(self, *args, **kwargs):
        await run_coroutine_loopsafe(
            coro = self._add_ipc(*args, **kwargs),
            target_loop = self._loop
        )
        
    def add_ipc_threadsafe(self, *args, **kwargs):
        call_coroutine_threadsafe(
            coro = self._add_ipc(*args, **kwargs),
            loop = self._loop
        )
            
    async def _clear_ipc(self):
        ''' Disconnects and removes the current IPC.
        '''
        # NOTE THAT THIS WILL NEED TO CHANGE if the _ipc client is ever brought
        # into the same event loop as the IPCEmbed autoresponder.
        if self._ipc is None:
            raise RuntimeError('No existing IPC to clear.')
            
        self._ipc.stop_threadsafe_nowait()
        self._ipc = None
        self._whoami = None
        
    async def clear_ipc_loopsafe(self, *args, **kwargs):
        await run_coroutine_loopsafe(
            coro = self._clear_ipc(*args, **kwargs),
            target_loop = self._loop
        )
        
    def clear_ipc_threadsafe(self, *args, **kwargs):
        call_coroutine_threadsafe(
            coro = self._clear_ipc(*args, **kwargs),
            loop = self._loop
        )
    
    @property
    def app_token(self):
        ''' Read-only access to the current app token.
        '''
        if self._token is None:
            return RuntimeError(
                'You must get a new token (or set an existing one) first!'
            )
        else:
            return self._token
        
    async def _get_new_token(self):
        ''' Registers a new token with Hypergolix. Call this once per
        application, and then reuse each time the application restarts.
        
        Returns the token, and also caches it with self.app_token.
        '''
        app_token = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['new_token']
        )
        self._token = app_token
        return app_token
    
    def get_new_token_threadsafe(self):
        ''' Threadsafe wrapper for new_token.
        '''
        return call_coroutine_threadsafe(
            coro = self._get_new_token(),
            loop = self._loop,
        )
    
    async def get_new_token_loopsafe(self):
        ''' Loopsafe wrapper for new_token.
        '''
        return (await run_coroutine_loopsafe(
            coro = self._get_new_token(),
            target_loop = self._loop,
        ))
        
    async def _set_existing_token(self, app_token):
        ''' Sets the app token for an existing application. Should be
        called every time the application restarts.
        '''
        response = await self.send(
            session = self.any_session,
            msg = app_token,
            request_code = self.REQUEST_CODES['set_token']
        )
        
        # If we haven't errored out...
        self._token = app_token
        
        # Note that, due to:
        #   1. the way the request/response system works
        #   2. the ipc host sending any startup obj during token registration
        #   3. the ipc host awaiting OUR ack from the startup-object-sending
        #       before acking the original token setting
        #   4. us awaiting that last ack
        # we are guaranteed to already have any declared startup object.
        if self._startup_obj is not None:
            return self._startup_obj
        else:
            return None
    
    def set_existing_token_threadsafe(self, *args, **kwargs):
        ''' Threadsafe wrapper for self._set_existing_token.
        '''
        return call_coroutine_threadsafe(
            self._set_existing_token(*args, **kwargs),
            loop = self._loop,
        )
        
    async def set_existing_token_loopsafe(self, *args, **kwargs):
        ''' Loopsafe wrapper for self._set_existing_token.
        '''
        return (await run_coroutine_loopsafe(
            coro = self._set_existing_token(*args, **kwargs),
            target_loop = self._loop,
        ))
            
    def _normalize_api_id(self, api_id):
        ''' Wraps the api_id appropriately, making sure the first byte
        is '\x00' and that it is an appropriate length.
        '''
        if len(api_id) == 65:
            if api_id[0:1] != b'\x00':
                raise ValueError(
                    'Improper api_id. First byte of full 65-byte field must '
                    'be x00.'
                )
        elif len(api_id) == 64:
            api_id = b'\x00' + api_id
            
        else:
            raise ValueError('Improper length of api_id.')
            
        return api_id
    
    async def _register_api(self, api_id):
        ''' Registers the api_id with the hypergolix service, allowing
        this application to receive shares from it.
        '''
        # Don't need to call this twice...
        # api_id = self._normalize_api_id(api_id)
            
        response = await self.send(
            session = self.any_session,
            msg = api_id,
            request_code = self.REQUEST_CODES['register_api']
        )
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while registering API.')
            
    async def _deregister_api(self, api_id):
        ''' Stops updates for the api_id from the hypergolix service.
        '''
        response = await self.send(
            session = self.any_session,
            msg = api_id,
            request_code = self.REQUEST_CODES['deregister_api']
        )
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while deregistering API.')
    
    async def _register_share_handler(self, api_id, cls, handler):
        ''' Call this to register a handler for an object shared by a
        different hypergolix identity, or the same hypergolix identity
        but a different application. Any api_id can have at most one 
        share handler, across ALL forms of callback (internal, 
        threadsafe, loopsafe).
        
        typecast determines what kind of ObjProxy class the object will
        be cast into before being passed to the handler.
        
        This HANDLER will be called from within the IPC embed's internal
        event loop.
        
        This METHOD must be called from within the IPC embed's internal
        event loop.
        '''
        api_id = self._normalize_api_id(api_id)
        await self._register_api(api_id)
        
        # Any handlers passed to us this way can already be called natively 
        # from withinour own event loop, so they just need to be wrapped such 
        # that they never raise.
        async def wrap_handler(*args, handler=handler, **kwargs):
            try:
                await handler(*args, **kwargs)
                
            except:
                logger.error(
                    'Error while running share handler. Traceback: \n' +
                    ''.join(traceback.format_exc())
                )
        
        # Hey, look at this! Because we're running a single-threaded event loop
        # and not ceding flow control to the loop, we don't need to worry about
        # synchro primitives here!
        self._share_handlers[api_id] = wrap_handler
        self._share_typecast[api_id] = cls
    
    def register_share_handler_threadsafe(self, api_id, cls, handler):
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
        async def wrapped_handler(*args, func=handler):
            ''' Wrap the handler in run_in_executor.
            '''
            await self._loop.run_in_executor(
                self._executor,
                func,
                *args
            )
            
        call_coroutine_threadsafe(
            coro = self._register_share_handler(
                api_id, 
                cls, 
                wrapped_handler
            ),
            loop = self._loop
        )
    
    async def register_share_handler_loopsafe(self, api_id, cls, handler, 
            target_loop):
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
        async def wrapped_handler(*args, loop=target_loop, coro=handler):
            ''' Wrap the handler in run_in_executor.
            '''
            await run_coroutine_loopsafe(
                coro = coro(*args),
                target_loop = loop
            )
            
        await run_coroutine_loopsafe(
            coro = self._register_share_handler(
                api_id, 
                cls, 
                wrapped_handler
            ),
            loop = self._loop
        )
    
    async def _register_nonlocal_handler(self, api_id, handler):
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
    
    def register_nonlocal_handler_threadsafe(self, api_id, handler):
        ''' Call this to register a handler for any private objects 
        created by the same hypergolix identity and the same hypergolix 
        application, but at a separate, concurrent session.
        
        This HANDLER will be called from within a single-use, dedicated
        thread.
        
        This METHOD must be called from a different thread than the IPC 
        embed's internal event loop.
        '''
        raise NotImplementedError()
        api_id = self._normalize_api_id(api_id)
        
        # self._nonlocal_handlers = {}
    
    async def register_nonlocal_handler_loopsafe(self, api_id, handler, loop):
        ''' Call this to register a handler for any private objects 
        created by the same hypergolix identity and the same hypergolix 
        application, but at a separate, concurrent session.
        
        This HANDLER will be called within the specified event loop, 
        also implying the specified event loop context (ie thread).
        
        This METHOD must be called from a different event loop than the 
        IPC embed's internal event loop. It is internally loopsafe, and
        need not be wrapped by run_coroutine_loopsafe.
        '''
        raise NotImplementedError()
        api_id = self._normalize_api_id(api_id)
        
        # self._nonlocal_handlers = {}
        
    async def deliver_startup_wrapper(self, session, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        '''
        (
            address,
            author,
            state, 
            is_link, 
            api_id,
            private, # Will be unused and set to None 
            dynamic,
            _legroom # Will be unused and set to None
        ) = self._unpack_object_def(request_body)
        
        # Resolve any links
        if is_link:
            link = Ghid.from_bytes(state)
            # Note: this may cause things to freeze, because async
            state = self.get_object(link)
            
        # Okay, now let's create an object for it
        obj = ObjProxyBase(
            hgxlink = self, 
            state = state, 
            api_id = api_id, 
            dynamic = dynamic,
            private = False,
            ghid = address, 
            binder = author, 
            # _legroom = None,
        )
            
        # Don't forget to add it to local lookup, since we're not rerouting
        # the update through get_object.
        self._objs_by_ghid[address] = obj
        
        # Set the startup obj internally so that _set_existing_token has access
        # to it.
        self._startup_obj = obj
        
        # Successful delivery. Return true
        return b'\x01'
        
    async def _get(self, cls, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        
        TODO: support implicit typecast based on api_id.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['get_object']
        )
        
        (
            address,
            author,
            state, 
            is_link, 
            api_id, 
            private, 
            dynamic, 
            _legroom
        ) = self._unpack_object_def(response)
            
        if is_link:
            # First discard the object, since we can't support it.
            response = await self.send(
                session = self.any_session,
                msg = bytes(address),
                request_code = self.REQUEST_CODES['discard_object']
            )
            
            # Now raise.
            raise NotImplementedError(
                'Hypergolix does not yet support nested links to other '
                'dynamic objects.'
            )
            # link = Ghid.from_bytes(state)
            # state = await self._get(link)
        
        state = await cls._hgx_unpack(state)
        obj = cls(
            hgxlink = self,
            state = state, 
            api_id = api_id, 
            dynamic = dynamic,
            private = private, 
            ghid = address, 
            binder = author, 
            # _legroom = _legroom,
        )
            
        # Don't forget to add it to local lookup so we can apply updates.
        self._objs_by_ghid[address] = obj
        
        return obj
        
    def get_threadsafe(self, cls, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        return call_coroutine_threadsafe(
            coro = self._get(cls, ghid),
            loop = self._loop,
        )
        
    async def get_loopsafe(self, cls, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        return (await run_coroutine_loopsafe(
            coro = self._get(cls, ghid),
            target_loop = self._loop,
        ))
    
    async def _new(self, cls, state, api_id=None, dynamic=True, private=False,
                    *args, **kwargs):
        ''' Create the object, yo.
        '''
        if api_id is None:
            api_id = cls._hgx_DEFAULT_API_ID
        
        obj = cls(
            hgxlink = self, 
            state = state, 
            api_id = api_id,
            dynamic = dynamic,
            private = private,
            *args, **kwargs
        )
        await obj._hgx_push()
        self._objs_by_ghid[obj.hgx_ghid] = obj
        return obj
        
    def new_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            coro = self._new(*args, **kwargs),
            loop = self._loop,
        )
        
    async def new_loopsafe(self, *args, **kwargs):
        return (await run_coroutine_loopsafe(
            coro = self._new(*args, **kwargs),
            target_loop = self._loop,
        ))
        
    async def _make_new(self, obj):
        ''' Submits a request for making a new object, and returns the
        resulting (address, binder).
        '''
        state = await obj._hgx_pack(
            obj._proxy_3141592
        )
        
        payload = self._pack_object_def(
            None,
            None,
            state,
            False, # is_link
            self._normalize_api_id(obj._api_id_3141592), # api_id
            obj.hgx_private,
            obj.hgx_dynamic,
            self._legroom
        )
        # Do this before making the request in case we disconnect immediately
        # after making it.
        binder = self.whoami
        # Now actually make the object.
        response = await self.send(
            session = self.any_session,
            msg = payload,
            request_code = self.REQUEST_CODES['new_object']
        )
        
        address = Ghid.from_bytes(response)
        return address, binder
        
    async def _make_update(self, obj):
        ''' Submits a request for updating an object. Does no LBYL 
        checking if dynamic, etc; just goes for it.
        '''
        state = await obj._hgx_pack(
            obj._proxy_3141592
        )
        msg = self._pack_object_def(
            obj.hgx_ghid,
            None, # Author
            state,
            False, # is_link
            None, # api_id
            None, # private
            None, # dynamic
            None # legroom
        )
        
        response = await self.send(
            session = self.any_session,
            msg = msg,
            request_code = self.REQUEST_CODES['update_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
            
        # Let the object worry about callbacks.
            
        return True
        
    async def _make_sync(self, obj):
        ''' Initiates a forceful upstream sync.
        ''' 
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['sync_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
    async def _make_share(self, obj, recipient):
        ''' Handles only the sharing of an object via the hypergolix
        service. Does not manage anything to do with the proxy itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid) + bytes(recipient),
            request_code = self.REQUEST_CODES['share_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while updating object.')
    
    async def _make_freeze(self, obj):
        ''' Handles only the freezing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['freeze_object']
        )
        
        frozen = await self._get(
            cls = type(obj), 
            ghid = Ghid.from_bytes(response)
        )
        
        return frozen
        
    async def _make_hold(self, obj):
        ''' Handles only the holding of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['hold_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while holding object.')
            
    async def _make_discard(self, obj):
        ''' Handles only the discarding of an object via the hypergolix
        service.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['discard_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        # It's a weakvaluedict. Doing this doesn't make the object any freer,
        # but it prevents us from fixing any future problems with it.
        
        # try:
        #     del self._objs_by_ghid[obj.hgx_ghid]
        # except KeyError:
        #     pass
            
        return True
        
    async def _make_delete(self, obj):
        ''' Handles only the deleting of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.hgx_ghid),
            request_code = self.REQUEST_CODES['delete_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        # It's a weakvaluedict. Doing this doesn't make the object any freer,
        # but it prevents us from fixing any future problems with it.
        
        # try:
        #     del self._objs_by_ghid[obj.address]
        # except KeyError:
        #     pass
        
        return True
        
    async def deliver_share_wrapper(self, session, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        '''
        (
            address,
            author,
            state, 
            is_link, 
            api_id,
            private, # Will be unused and set to None 
            dynamic,
            _legroom # Will be unused and set to None
        ) = self._unpack_object_def(request_body)
        
        # Resolve any links
        if is_link:
            raise NotImplementedError()
            
        # This is async, which is single-threaded, so there's no race condition
        try:
            handler = self._share_handlers[api_id]
            cls = self._share_typecast[api_id]
            
        except KeyError:
            logger.warning(
                'Received a share for an API_ID that was lacking a handler or '
                'typecast. Deregistering the API_ID.'
            )
            await self._deregister_api(api_id)
            
        else:
            state = await cls._hgx_unpack(state)
            obj = cls(
                hgxlink = self,
                state = state,
                api_id = api_id,
                dynamic = dynamic,
                private = False,
                ghid = address,
                binder = author
            )
            
            # Don't forget to add it to local lookup, since we're not rerouting
            # the update through get_object.
            self._objs_by_ghid[address] = obj
            
            # Run this concurrently, so that we can release the req/res session
            asyncio.ensure_future(handler(obj))
        
        # Successful delivery. Return true
        return b'\x01'
        
    async def deliver_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object delivery, dispatches it to
        the application, and serializes a response to the IPC host.
        
        Note that (despite the terrible name) this is only called when a
        concurrent instance of the same application with the same 
        hypergolix agent creates a (private) object.
        '''
        raise NotImplementedError()

    async def update_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object update, updates the AppObj
        instance(s) accordingly, and serializes a response to the IPC 
        host.
        '''
        (
            address,
            author, # Will be unused and set to None
            state, 
            is_link, 
            api_id, # Will be unused and set to None 
            private, # Will be unused and set to None 
            dynamic, # Will be unused and set to None 
            _legroom # Will be unused and set to None
        ) = self._unpack_object_def(request_body)
        
        try:
            obj = self._objs_by_ghid[address]
            
        except KeyError:
            # Just discard the object, since we don't actually have a copy of
            # it locally.
            logger.warning(
                'Received an object update, but the object was no longer '
                'contained in memory. Discarding its subscription: ' + 
                str(address) + '.'
            )
            response = await self.send(
                session = self.any_session,
                msg = bytes(address),
                request_code = self.REQUEST_CODES['discard_object']
            )
            
        else:
            if is_link:
                # Uhhhhhh... Raise? It's not really appropriate to discard...
                raise NotImplementedError(
                    'Cannot yet support objects with nested dynamic links.'
                )
                
            else:
                logger.debug(
                    'Received update for ' + str(address) + '; forcing pull.'
                )
                await obj._force_pull_3141592(state)
            
        return b'\x01'
        
    async def delete_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object deletion, and applies it to
        the object.
        '''
        ghid = Ghid.from_bytes(request_body)
        
        try:
            obj = self._objs_by_ghid[ghid]
        except KeyError:
            pass
        else:
            await obj._force_delete_3141592()
            
        return b'\x01'

    async def notify_share_failure_wrapper(self, session, request_body):
        ''' Deserializes an incoming async share failure notification, 
        dispatches that to the app, and serializes a response to the IPC 
        host.
        '''
        return b''

    async def notify_share_success_wrapper(self, session, request_body):
        ''' Deserializes an incoming async share failure notification, 
        dispatches that to the app, and serializes a response to the IPC 
        host.
        '''
        return b''
        

class AppObj:
    ''' A class for objects to be used by apps. Can be updated (if the 
    object was created by the connected Agent and is mutable) and have
    a state.
    
    AppObj instances will wrap their state in a dispatch structure 
    before updating golix containers.
    
    Can be initiated directly using a reference to an embed. May also be
    constructed from _EmbedBase.new_object.
    
    Everything here is wrapped from the messagepack dispatch format, so
    state may be more than just bytes.
    
    Unlike RawObj instances, AppObj instances are meant to be used with
    a specific API definition, and/or a specific token. Tokens can be 
    used to provide a consistent PRIVATE, freeform application state,
    whilst any objects that are being communicated to a different agent
    must use api ids.
    
    Basically, token-based AppObj instances without an api_id represent
    data that will not be shared. It can, however, be any unstructured
    data.
    
    _private isinstance bool
        if True, dispatch by token.
        if False, dispatch by api id.
        
    Note: AppObj should be set up such that there is exactly one AppObj
    per application. As such, they should probably be aware of their 
    endpoints.
    
    Note: Todo: Add some kind of automatic garbage collection mechanism
    such that, when gc'd because, for example, it is only defined within
    the scope of an AppObj state (as when linking between dynamic 
    objects), we don't continue to get updates for an object that we no
    longer retain.
    '''
    # This should define *only* ADDITIONAL slots.
    __slots__ = [
        '__weakref__',
        '_embed',
        '_is_dynamic',
        '_callbacks_threadsafe',
        '_callbacks_async',
        '_inoperable',
        '_author',
        '_address',
        '_state',
        '_api_id',
        '_private',
        '_legroom',
    ]
    
    # Restore the original behavior of hash
    __hash__ = type.__hash__
        
    @classmethod
    def from_threadsafe(cls, embed, *args, **kwargs):
        ''' Synchronous constructor.
        '''
        return call_coroutine_threadsafe(
            cls.from_async(embed, *args, **kwargs),
            loop = embed._loop,
        )
        
    @classmethod
    async def from_async(cls, embed, state, api_id=None, private=False, 
    dynamic=True, threadsafe_callbacks=None, async_callbacks=None, 
    _legroom=None):
        ''' Asyncronous constructor. Use ONLY for direct new object 
        creation.
        '''
        dynamic = bool(dynamic)
        private = bool(private)
        
        if threadsafe_callbacks is None:
            threadsafe_callbacks = []
        
        if async_callbacks is None:
            async_callbacks = []
        
        # This creates an actual Golix object via hypergolix service.
        address, author = await embed._new_object(
            state, api_id, private, dynamic, _legroom
        )
        
        try:
            obj = cls(embed, address, state, author, api_id, private, dynamic, 
                threadsafe_callbacks, async_callbacks, _legroom)
        except:
            # Cleanup any failure and reraise
            await embed.await_session_async()
            await embed.send(
                session = embed.any_session,
                msg = bytes(address),
                request_code = embed.REQUEST_CODES['delete_object']
            )
            raise
            
        return obj
    
    def __init__(self, embed, address, state, author, api_id, private, dynamic, 
    threadsafe_callbacks, async_callbacks, _legroom=None):
        ''' Create a new AppObj with:
        
        state isinstance bytes(like)
        dynamic isinstance bool(like) (optional)
        callbacks isinstance iterable of callables (optional)
        '''
        # Copying like this seems dangerous, but I think it should be okay.
        if not isinstance(embed, weakref.ProxyTypes):
            embed = weakref.proxy(embed)
        self._embed = embed
            
        self._inoperable = False
        self._is_dynamic = dynamic
        self._api_id = api_id
        self._private = private
        self._callbacks_threadsafe = collections.deque(threadsafe_callbacks)
        self._callbacks_async = collections.deque(async_callbacks)
        self._author = author
        self._address = address
            
        # Legroom is None. Infer it from the dispatch.
        if _legroom is None:
            self._legroom = self._embed._legroom
        else:
            self._legroom = _legroom
        
        # Only proceed to define legroom if this is dynamic.
        if self.is_dynamic:
            self._state = collections.deque(
                maxlen = self._legroom
            )
            self._state.appendleft(state)
            
        else:
            self._state = state
        
        # # _preexisting was set, so we're loading an existing object.
        # # "Trust" anything using _preexisting to have passed a correct value
        # # for state and dynamic.
        # if _preexisting is not None:
        #     address = _preexisting[0]
        #     author = _preexisting[1]
        #     state, api_id, private, dynamic, _legroom = state
        
        # Todo: add (python) gc logic such that this is removed when the object
        # is removed as well. Note that we currently DO do this in delete, but
        # not on actual python gc.
        self._embed._objs_by_ghid[address] = self
        
    @property
    def author(self):
        ''' The ghid address of the agent that created the object.
        '''
        return self._author
        
    @property
    def address(self):
        ''' The ghid address of the object itself.
        '''
        return self._address
            
    @property
    def private(self):
        ''' Return the (immutable) property describing whether this is
        a private application object, or a sharable api-id-dispatched
        object.
        '''
        return self._private
        
    @property
    def api_id(self):
        ''' The api_id (if one exists) of the object. Private objects
        may or may not omit this.
        '''
        return self._api_id
        
    @property
    def threadsafe_callbacks(self):
        if self.is_dynamic:
            return self._callbacks_threadsafe
        else:
            raise TypeError('Static objects cannot have callbacks.')
        
    @property
    def async_callbacks(self):
        if self.is_dynamic:
            return self._callbacks_async
        else:
            raise TypeError('Static objects cannot have callbacks.')
            
    @property
    def is_dynamic(self):
        ''' Indicates whether this object is dynamic.
        returns True/False.
        '''
        return self._is_dynamic
        
    @property
    def is_owned(self):
        ''' Indicates whether this object is owned by the associated 
        Agent.
        
        returns True/False.
        
        TODO: fix this. Maybe with hook in get_object?
        '''
        return self._embed.whoami_threadsafe() == self.author
            
    @property
    def mutable(self):
        ''' Returns true if and only if self is a dynamic object and is
        owned by the current agent.
        
        TODO: fix this. Maybe with hook in get_object?
        '''
        return self.is_dynamic and self.is_owned
        
    @property
    def is_link(self):
        if self.is_dynamic:
            if isinstance(self._state[0], AppObj):
                return True
            else:
                return False
        else:
            return None
            
    @property
    def link_address(self):
        ''' Only available when is_link is True. Otherwise, will return
        None.
        '''
        if self.is_dynamic and self.is_link:
            return self._state[0].address
        else:
            return None
        
    @property
    def state(self):
        if self._inoperable:
            raise ValueError('Object has already been deleted.')
        elif self.is_dynamic:
            if self.is_link:
                # Recursively resolve any nested/linked objects
                return self._state[0].state
            else:
                return self._state[0]
        else:
            return self._state
        
    def append_threadsafe_callback(self, callback):
        ''' Registers a callback to be called when the object receives
        an update.
        
        callback must be hashable and callable. Function definitions and
        lambdas are natively hashable; callable classes may not be.
        
        On update, callbacks are passed the object.
        '''
        if not callable(callback):
            raise TypeError('Callback must be callable.')
        self._callbacks_threadsafe.append(callback)
        
    def prepend_threadsafe_callback(self, callback):
        ''' Registers a callback for updates.
        '''
        if not callable(callback):
            raise TypeError('Callback must be callable.')
        self._callbacks_threadsafe.appendleft(callback)
        
    def remove_threadsafe_callback(self, callback):
        ''' Removes the first instance of a threadsafe callback.
        
        Raises ValueError if the callback has not been registered.
        '''
        self._callbacks_threadsafe.remove(callback)
        
    def clear_threadsafe_callbacks(self):
        ''' Resets all threadsafe callbacks.
        '''
        self._callbacks_threadsafe.clear()
        
    def append_async_callback(self, callback):
        ''' Registers a callback to be called when the object receives
        an update.
        
        callback must be hashable and callable. Function definitions and
        lambdas are natively hashable; callable classes may not be.
        
        On update, callbacks are passed the object.
        '''
        # TODO: complain about lack of built-in awaitable() function
        if not callable(callback):
            raise TypeError('Callback must be callable.')
        self._callbacks_async.append(callback)
        
    def prepend_async_callback(self, callback):
        ''' Registers a callback for updates.
        '''
        # TODO: complain about lack of built-in awaitable() function
        if not callable(callback):
            raise TypeError('Callback must be callable.')
        self._callbacks_async.appendleft(callback)
        
    def remove_threadsafe_callback(self, callback):
        ''' Removes the first instance of an async callback.
        
        Raises ValueError if the callback has not been registered.
        '''
        self._callbacks_async.remove(callback)
        
    def clear_async_callbacks(self):
        ''' Resets all threadsafe callbacks.
        '''
        self._callbacks_threadsafe.clear()
        
    async def _notify_callbacks(self):
        ''' INFORM THE OTHERS
        '''
        def callerbackyall():
            # At least the closure makes this much easier
            for callback in self.threadsafe_callbacks:
                try:
                    callback(self)
                except Exception as exc:
                    logger.error(
                        'Callback exception swallowed: ' + repr(exc) + '\n' + 
                        ''.join(traceback.format_tb(exc.__traceback__))
                    )
        
        thread_fut_sync = self._embed._executor.submit(callerbackyall)
        thread_fut_async = asyncio.ensure_future(
            await_sync_future(thread_fut_sync)
        )
        assy_fut = asyncio.ensure_future(
            self._notify_async_callbacks()
        )
        await asyncio.wait([thread_fut_async, assy_fut])
        
    async def _notify_async_callbacks(self):
        # TODO: convert this to parallel execution using ensure_future?
        for callback in self.async_callbacks:
            try:
                await callback(self)
            except Exception as exc:
                logger.error(
                    'Callback exception swallowed: ' + repr(exc) + '\n' + 
                    ''.join(traceback.format_tb(exc.__traceback__))
                )
        
    def update_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.update_async(*args, **kwargs),
            loop = self._embed._loop,
        )
            
    async def update_async(self, state):
        ''' Updates a mutable object to a new state.
        
        May only be called on a dynamic object that was created by the
        attached Agent.
        
        If _preexisting is True, this is an update coming down from a
        persister, and we will NOT push it upstream.
        '''
        # TODO: fix this, or update a way around it, or something. This will
        # hang otherwise.
        # if not self.is_owned:
        #     raise TypeError(
        #         'Cannot update an object that was not created by the '
        #         'attached Agent.'
        #     )
            
        # First operate on the object, since it's easier to undo local changes
        await self._update(state)
        await self._embed._update_object(self, state)
        
        return True
            
    async def _update(self, state):
        ''' Handles the actual updating **for the object only.** Does 
        not update or involve the embed.
        '''
        if self._inoperable:
            raise ValueError('Object has already been deleted.')
            
        if not self.is_dynamic:
            raise TypeError('Cannot update a static object.')
            
        # Update local state.
        self._state.appendleft(state)
        
    def sync_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.sync_async(*args, **kwargs),
            loop = self._embed._loop,
        )
            
    async def sync_async(self, *args):
        ''' Checks the current state matches the state at the connected
        Agent. If this is a dynamic and an update is available, do that.
        If it's a static and the state mismatches, raise error.
        '''
        await self._embed._sync_object(self)
        await self._sync()
        
        return True
            
    async def _sync(self, *args):
        ''' Handles the actual syncing **for the object only.** Does not
        update or involve the embed.
        '''
        pass
        
    def share_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.share_async(*args, **kwargs),
            loop = self._embed._loop,
        )
            
    async def share_async(self, recipient):
        ''' Public accessor for sharing via object.
        '''
        # Todo: add try-catch to undo local changes after failed upstream share
        # First operate on the object, since it's easier to undo local changes
        await self._share(recipient)
        await self._embed._share_object(self, recipient)
        
        return True
            
    async def _share(self, recipient):
        ''' Handles the actual sharing **for the object only.** Does not
        update or involve the embed.
        
        This prevents the sharing of private objects.
        
        Overriding this without calling super() may result in security
        risks for applications.
        '''
        # Note: should this be moved into the Embed? That might be a little bit
        # safer. Or, should the hypergolix service check to make sure nothing
        # with an app_token is shared?
        if self.private:
            raise TypeError('Private application objects cannot be shared.')
        else:
            return True
        
    def freeze_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.freeze_async(*args, **kwargs),
            loop = self._embed._loop,
        )
        
    async def freeze_async(self):
        ''' Creates a static snapshot of the dynamic object. Returns a 
        new static RawObj instance. Does NOT modify the existing object.
        May only be called on dynamic objects. 
        
        Note: should really be reimplemented as a recursive resolution
        of the current container object, and then a hold on that plus a
        return of a static RawObj version of that. This is pretty buggy.
        
        Note: does not currently traverse nested dynamic bindings, and
        will probably error out if you attempt to freeze one.
        '''
        await self._freeze()
        frozen = await self._embed._freeze_object(self)
        
        return frozen
            
    async def _freeze(self):
        ''' Handles the actual freezing **for the object only.** Does 
        not update or involve the embed.
        '''
        if not self.is_dynamic:
            raise TypeError(
                'Static objects cannot be frozen. If attempting to save them, '
                'call hold instead.'
            )
        
    def hold_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.hold_async(*args, **kwargs),
            loop = self._embed._loop,
        )
        
    async def hold_async(self):
        ''' Binds to the object, preventing its deletion.
        '''
        await self._hold()
        await self._embed.hold_object(self)
        
        return True
            
    async def _hold(self):
        ''' Handles the actual holding **for the object only.** Does not
        update or involve the embed.
        '''
        pass
        
    def discard_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.discard_async(*args, **kwargs),
            loop = self._embed._loop,
        )
        
    async def discard_async(self):
        ''' Tells the hypergolix service that the application is done 
        with the object, but does not directly delete it. No more 
        updates will be received.
        '''
        await self._discard()
        await self._embed._discard_object(self)
        
        return True
        
    async def _discard(self):
        ''' Performs AppObj actions necessary to discard.
        '''
        self.clear_threadsafe_callbacks()
        self.clear_async_callbacks()
        super().__setattr__('_inoperable', True)
        super().__setattr__('_is_dynamic', None)
        super().__setattr__('_author', None)
        
    def delete_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.delete_async(*args, **kwargs),
            loop = self._embed._loop,
        )
            
    async def delete_async(self):
        ''' Tells any persisters to delete. Clears local state. Future
        attempts to access will raise ValueError, but does not (and 
        cannot) remove the object from memory.
        '''
        await self._delete()
        await self._embed._delete_object(self)
        
        return True
            
    async def _delete(self):
        ''' Handles the actual deleting **for the object only.** Does 
        not update or involve the embed.
        '''
        await self._discard()
        # This creates big problems down the road.
        # super().__setattr__('_embed', None)
    
    # This might be a little excessive, but I guess it's nice to have a
    # little extra protection against updates?
    def __setattr__(self, name, value):
        ''' Prevent rewriting declared attributes in slots. Does not
        prevent assignment using @property.
        
        Note: if this gets removed, or re-assingment is otherwise
        implemented, you will need to add a check for overwriting an 
        existing state in static objects.
        '''
        if name in self.__slots__:
            try:
                __ = getattr(self, name)
            except AttributeError:
                pass
            else:
                raise AttributeError(
                    'RawObj internals cannot be changed once they have been '
                    'declared. They must be mutated instead.'
                )
                
        super().__setattr__(name, value)
            
    def __delattr__(self, name):
        ''' Prevent deleting declared attributes.
        '''
        raise AttributeError(
            'RawObj internals cannot be changed once they have been '
            'declared. They must be mutated instead.'
        )
        
    def __eq__(self, other):
        if not isinstance(other, AppObj):
            raise TypeError(
                'Cannot compare RawObj instances to incompatible types.'
            )
            
        # Short-circuit if dynamic mismatches
        if self.is_dynamic != other.is_dynamic:
            return False
            
        meta_comparison = (
            # Don't compare is_owned, because we want shared objects to be ==
            # Don't compare app_token, because we want shared objects to be ==
            self.api_id == other.api_id and
            self.address == other.address and
            self.author == other.author
        )
        
        # If dynamic, state comparison looks at as many state shots as we share
        if self.is_dynamic:
            state_comparison = True
            comp = zip(self._state, other._state)
            for a, b in comp:
                state_comparison &= (a == b)
                
        # If static, state comparison simply looks at both states directly
        else:
            state_comparison = (self.state == other.state)
            
        # Return the result of the whole comparison
        return meta_comparison and state_comparison