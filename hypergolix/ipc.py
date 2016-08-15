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
        callsheet = set()
        for listener in self._update_listeners.get_any(ghid):
            callsheet.add(self._token_from_endpoint[listener])
        
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
    
    async def make_callsheet(self, ghid, skip_endpoint=None, skip_tokens=None):
        ''' Generates a callsheet (set of tokens) for the dispatchable
        obj.
        
        The callsheet is generated from app_tokens, so that the actual 
        distributor can kick missing tokens back for safekeeping.
        
        TODO: make this a "private" method -- aka, remove this from the
        rolodex share handling.
        TODO: make this exclusively apply to object sharing, NOT to obj
        updates, which uses _update_listeners directly and exclusively.
        '''
        logger.debug('Generating callsheet.')
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
                'Failed to retrieve object at ' + str(bytes(ghid)) + '\n' + 
                ''.join(traceback.format_exc())
            )
            return set()
        
        # Create a temporary set for relevant endpoints
        callsheet = set()
        
        private_owner = self._dispatch.get_parent_token(ghid)
        if private_owner:
            logger.debug('Ghid has a private owner.')
            callsheet.add(private_owner)
            
        else:
            logger.debug('Ghid has NO private owner.')
            endpoints = set()
            
            # Add any endpoints based on their tracking of the api.
            logger.debug(
                'Interested API endpoints: ' + 
                str(len(self._endpoints_from_api.get_any(obj.api_id)))
            )
            endpoints.update(self._endpoints_from_api.get_any(obj.api_id))
            
            # Add any endpoints based on their existing listening status.
            logger.debug(
                'Object listeners: ' + 
                str(self._update_listeners.get_any(obj.ghid))
            )
            endpoints.update(self._update_listeners.get_any(obj.ghid))
            
            # Convert endpoints to tokens.
            for endpoint in endpoints:
                logger.debug(
                    'Adding endpoint to callsheet: ' + repr(endpoint)
                )
                callsheet.add(self._token_from_endpoint[endpoint])
                
        # Normally the keyerror will catch both default of None as well as any 
        # missing objects, but because it's a weakly referenced lookup, we have
        # to explicitly skip an undefined skip_endpoint
        if skip_endpoint is not None:
            try:
                skip_token = self._token_from_endpoint[skip_endpoint]
            except KeyError:
                logger.warning(
                    'Skip endpoint was included in making callsheet, but it '
                    'appears to have been removed during generation.'
                )
                skip_token = None
            else:
                logger.debug('Skipping token for ' + repr(skip_endpoint))
        else:
            skip_token = None
            
        callsheet.discard(skip_token)
        # What would use this?
        # callsheet.difference_update(skip_tokens)
        
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
        for token in callsheet:
            # For each token...
            tasks.append(
                # ...in parallel, schedule a single execution
                asyncio.ensure_future(
                    # Of a _distribute_single call to the distributor.
                    caller(token, *args)
                )
            )
        await asyncio.gather(*tasks)
                    
    async def _distr_single(self, token, distributor, *args):
        ''' Distributes a single request to a single token.
        '''
        try:
            await distributor(self._endpoint_from_token[token], *args)
            
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
                'Failed to retrieve object at ' + str(bytes(ghid)) + '\n' + 
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
        
        for ghid in self._dispatch.get_startup_objs(app_token):
            await self.send_startup(endpoint, ghid)
        
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
        
    async def notify_share_success(self, token, ghid, recipient):
        ''' Notifies the embedded client of a successful share.
        '''
        try:
            endpoint = self._endpoint_from_token[token]
            
        except KeyError:
            logger.info('Requesting app currently unavailable for share ack.')
            sharelog = _ShareLog(ghid, recipient)
            self._orphan_share_acks.add(token, sharelog)
            
        else:
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
        
    async def notify_share_failure(self, token, ghid, recipient):
        ''' Notifies the embedded client of an unsuccessful share.
        '''
        try:
            endpoint = self._endpoint_from_token[token]
            
        except KeyError:
            logger.info('Requesting app currently unavailable for share nak.')
            sharelog = _ShareLog(ghid, recipient)
            self._orphan_share_acks.add(token, sharelog)
            
        else:
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
        
        Requires an existing app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token prior to getting objects.')
            
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
        
        Requires an existing app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token prior to making objects.')
        
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
            self._dispatch.register_private(app_token, obj.ghid)
            
        # Otherwise, make sure to notify any other interested parties.
        else:
            # TODO: change send_object to just send the ghid, not the object
            # itself, so that the app doesn't have to be constantly discarding
            # stuff it didn't create?
            callsheet = await self.make_callsheet(
                obj.ghid, 
                skip_endpoint = endpoint
            )
             
            # Note that self._obj_sender handles adding update listeners
            await self.distribute_to_endpoints(
                callsheet,
                self.send_object,
                obj.ghid
            )
        
        return bytes(obj.ghid)
        
    async def update_object_wrapper(self, endpoint, request_body):
        ''' Wraps self.dispatch.new_token into a bytes return.
        
        Requires existing app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token before updating objects.')
            
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
            callsheet = await self.make_callsheet(
                obj.ghid, 
                skip_endpoint = endpoint
            )
                
            await self.distribute_to_endpoints(
                callsheet,
                self.send_update,
                obj.ghid
            )
        
        return b'\x01'
        
    async def sync_object_wrapper(self, endpoint, request_body):
        ''' Requires existing app token. Will not return the update; if
        a new copy of the object was available, it will be sent 
        independently.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token before syncing objects.')
            
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
        
        Requires an app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token before freezing objects.')
            
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
        ''' Wraps object holding. Requires an app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token before holding objects.')
            
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
        ''' Wraps object discarding. Requires an app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Register an app token before discarding objects.')
            
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
        
        Requires an app token.
        '''
        if endpoint not in self._token_from_endpoint:
            raise IPCError('Must register app token before deleting objects.')
            
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
    ''' The thing you actually put in your app. Pair with a client in an
    Autocomms to establish IPC over that transport.
    
    Note that each embed has exactly one endpoint. I guess there's not
    really any reason an application couldn't use multiple embeds, but
    there really should never be a reason to do that?
    
    Todo: asyncify.
    '''
    REQUEST_CODES = {
        # # Get new app token
        'new_token': b'+T',
        # # Register existing app token
        'set_token': b'$T',
        # Register an API
        'register_api': b'$A',
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
        try:    
            self._token = None
            self._legroom = 3
        
        # Something strange using _TestEmbed is causing this, suppress it for
        # now until technical debt can be reduced
        except AttributeError:
            pass
        
        # Lookup for ghid -> object
        self._objs_by_ghid = weakref.WeakValueDictionary()
        
        # Track registered callbacks for new objects
        self._object_handlers = {}
        
        # Create an executor for awaiting threadsafe callbacks
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
    def app_token(self):
        ''' Get your app token, or if you have none, register a new one.
        '''
        if self._token is None:
            return RuntimeError(
                'You must get a new token (or set an existing one) first!'
            )
        else:
            return self._token
    
    def whoami_threadsafe(self):
        ''' Threadsafe wrapper for whoami.
        '''
        return call_coroutine_threadsafe(
            self.whoami_async(),
            loop = self._loop,
        )
        
    async def whoami_async(self):
        ''' Gets our identity GHID from the hypergolix service.
        '''
        await self.await_session_async()
        raw_ghid = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['whoami']
        )
        return Ghid.from_bytes(raw_ghid)
    
    def new_token_threadsafe(self):
        ''' Threadsafe wrapper for new_token.
        '''
        return call_coroutine_threadsafe(
            self.new_token_async(),
            loop = self._loop,
        )
        
    async def new_token_async(self):
        ''' Gets a new app_token.
        '''
        await self.await_session_async()
        app_token = await self.send(
            session = self.any_session,
            msg = b'',
            request_code = self.REQUEST_CODES['new_token']
        )
        self._token = app_token
        return app_token
    
    def set_token_threadsafe(self, *args, **kwargs):
        ''' Threadsafe wrapper for set_token.
        '''
        return call_coroutine_threadsafe(
            self.set_token_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def set_token_async(self, app_token):
        ''' Sets an existing token.
        '''
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = app_token,
            request_code = self.REQUEST_CODES['set_token']
        )
        # If we haven't errored out...
        self._token = app_token
        return response
        
    def register_api_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.register_api_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def register_api_async(self, api_id, object_handler=None):
        ''' Registers an API ID with the hypergolix service for this 
        application.
        
        Note that object handlers must be threadsafe.
        '''
        if len(api_id) != 65:
            raise ValueError('Invalid API ID.')
        
        if object_handler is None:
            object_handler = lambda *args, **kwargs: None
            
        if not callable(object_handler):
            raise TypeError('object_handler must be callable')
        
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = api_id,
            request_code = self.REQUEST_CODES['register_api']
        )
        if response == b'\x01':
            self._object_handlers[api_id] = object_handler
            return True
        else:
            raise RuntimeError('Unknown error while registering API.')
        
    def get_obj_threadsafe(self, *args, **kwargs):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        return call_coroutine_threadsafe(
            self.get_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def get_obj_async(self, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        response = await self._get_object(ghid)
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
            link = Ghid.from_bytes(state)
            state = await self.get_object_async(link)
        
        return AppObj(
            embed = self, # embed
            address = address, 
            state = state, 
            author = author, 
            api_id = api_id, 
            private = private, 
            dynamic = dynamic,
            _legroom = _legroom,
            threadsafe_callbacks = [],
            async_callbacks = [],
        )
        
    async def _get_object(self, ghid):
        ''' Loads an object into local memory from the hypergolix 
        service.
        '''
        # Simple enough. super().get_object() will handle converting this to an
        # actual object.
        await self.await_session_async()
        return (await self.send(
            session = self.any_session,
            msg = bytes(ghid),
            request_code = self.REQUEST_CODES['get_object']
        ))
        
    def new_obj_threadsafe(self, *args, **kwargs):
        ''' Alternative constructor for AppObj that does not require 
        passing the embed explicitly.
        '''
        # We don't need to do anything special here, since AppObj will call
        # _new_object for us.
        return AppObj.from_threadsafe(embed=self, *args, **kwargs)
        
    async def new_obj_async(self, *args, **kwargs):
        ''' Alternative constructor for AppObj that does not require 
        passing the embed explicitly.
        '''
        # We don't need to do anything special here, since AppObj will call
        # _new_object for us.
        return (await AppObj.from_async(embed=self, *args, **kwargs))
        
    async def _new_object(self, state, api_id, private, dynamic, _legroom):
        ''' Handles only the creation of a new object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        
        return address, author
        '''
        if private:
            app_token = self.app_token
            
        else:
            app_token = bytes(4)
            if api_id is None:
                raise TypeError(
                    'api_id must be defined for a non-private object.'
                )
            
        if isinstance(state, AppObj):
            state = state.address
            is_link = True
        else:
            is_link = False
            
        payload = self._pack_object_def(
            None,
            None,
            state,
            is_link,
            api_id,
            private,
            dynamic,
            _legroom
        )
        
        # Note that currently, we're not re-packing the api_id or app_token.
        # ^^ Not sure what that note is actually about.
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = payload,
            request_code = self.REQUEST_CODES['new_object']
        )
            
        # Note that the upstream ipc_host will automatically send us updates.
        
        address = Ghid.from_bytes(response)
        author = await self.whoami_async()
        return address, author
        
    def update_obj_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.update_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def update_obj_async(self, obj, state):
        ''' Wrapper for obj.update.
        '''
        # is_owned will call whoami which will deadlock
        # TODO: fix or reconsider strategy
        # if not obj.is_owned:
        #     raise TypeError(
        #         'Cannot update an object that was not created by the '
        #         'attached Agent.'
        #     )
            
        # First operate on the object, since it's easier to undo local changes
        await obj._update(state)
        await self._update_object(obj, state)
        # Todo: add try-catch to revert update after failed upstream push
        
        return True
        
    async def _update_object(self, obj, state):
        ''' Handles only the updating of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        if isinstance(state, AppObj):
            state = bytes(state.address)
        
        msg = self._pack_object_def(
            obj.address,
            None,
            state,
            obj.is_link,
            None,
            None,
            None,
            None
        )
        
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = msg,
            request_code = self.REQUEST_CODES['update_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
            
        # This breaks our DoC but effectively insulates us from calling any 
        # callbacks on an invalid update.
        await obj._notify_callbacks()
        return True
        
    def sync_obj_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.sync_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def sync_obj_async(self, obj):
        ''' Checks for an update to a dynamic object, and if one is 
        available, performs an update.
        '''
        await obj._sync_object()
        await self._sync_object(obj)
        
        return True

    async def _sync_object(self, obj):
        ''' Handles only the syncing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        raise NotImplementedError(
            'Manual object syncing is not yet supported. Approximate it with '
            'get_object?'
        )
        # Note: this is probably all trash.
        if obj.is_dynamic:
            pass
        else:
            other = self._get_object(obj.address)
            if other.state != obj.state:
                raise RuntimeError(
                    'Local object state appears to be corrupted.'
                )
        
    def share_obj_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.share_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def share_obj_async(self, obj, recipient):
        ''' Shares an object with someone else.
        '''
        # Todo: add try-catch to undo local changes after failed upstream share
        # First operate on the object, since it's easier to undo local changes
        await obj._share(recipient)
        await self._share_object(obj, recipient)
        
        return True

    async def _share_object(self, obj, recipient):
        ''' Handles only the sharing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.address) + bytes(recipient),
            request_code = self.REQUEST_CODES['share_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while updating object.')
        
    def freeze_obj_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.freeze_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def freeze_obj_async(self, obj):
        ''' Converts a dynamic object to a static object.
        '''
        await obj._freeze()
        frozen = await self._freeze_object(obj)
        
        return frozen

    async def _freeze_object(self, obj):
        ''' Handles only the freezing of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        # TODO: make better fix for race condition in applying state freezing. 
        # (Freeze, and while awaiting response, update state; results in 
        # later obj.state being incorrect)
        state_ish = obj.state
        
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['freeze_object']
        )
        return AppObj(
            embed = self,
            address = Ghid.from_bytes(response),
            state = state_ish,
            author = obj.author,
            api_id = obj.api_id,
            private = obj.private,
            dynamic = False,
            _legroom = 0,
            threadsafe_callbacks = [],
            async_callbacks = [],
        )
        
    def hold_obj_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.hold_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def hold_obj_async(self, obj):
        ''' Binds an object, preventing its deletion.
        '''
        await obj._hold()
        await self._hold_object(obj)
        
        return True

    async def _hold_object(self, obj):
        ''' Handles only the holding of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['hold_object']
        )
        
        if response == b'\x01':
            return True
        else:
            raise RuntimeError('Unknown error while updating object.')
        
    def discard_obj_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.discard_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def discard_obj_async(self, obj):
        ''' Attempts to delete an object. May not succeed, if another 
        Agent has bound to it.
        '''
        await obj._discard()
        await self._discard_object(obj)
        
        return True
        
    async def _discard_object(self, obj):
        ''' Handles only the discarding of an object via the hypergolix
        service.
        '''
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['discard_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        del self._objs_by_ghid[obj.address]
        return True
        
    def delete_obj_threadsafe(self, *args, **kwargs):
        return call_coroutine_threadsafe(
            self.delete_obj_async(*args, **kwargs),
            loop = self._loop,
        )
        
    async def delete_obj_async(self, obj):
        ''' Attempts to delete an object. May not succeed, if another 
        Agent has bound to it.
        '''
        await obj._delete()
        await self._delete_object(obj)
        
        return True

    async def _delete_object(self, obj):
        ''' Handles only the deleting of an object via the hypergolix
        service. Does not manage anything to do with the AppObj itself.
        '''
        await self.await_session_async()
        response = await self.send(
            session = self.any_session,
            msg = bytes(obj.address),
            request_code = self.REQUEST_CODES['delete_object']
        )
        
        if response != b'\x01':
            raise RuntimeError('Unknown error while updating object.')
        
        try:
            del self._objs_by_ghid[obj.address]
        except KeyError:
            pass
        
        return True
        
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
        obj = AppObj(
            embed = self, 
            address = address, 
            state = state, 
            author = author, 
            api_id = api_id, 
            private = False,
            dynamic = dynamic,
            _legroom = None,
            threadsafe_callbacks = [],
            async_callbacks = [],
        )
            
        # Well this is a huge hack. But something about the event loop 
        # itself is fucking up control flow and causing the world to hang
        # here.
        # TODO: fix this.
        worker = threading.Thread(
            target = self._object_handlers[api_id],
            daemon = True,
            args = (obj,),
            name = _generate_threadnames('embedsup')[0],
        )
        worker.start()
        
        # Successful delivery. Return true
        return b'\x01'
        
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
            link = Ghid.from_bytes(state)
            # Note: this may cause things to freeze, because async
            state = self.get_object(link)
            
        # Okay, now let's create an object for it
        obj = AppObj(
            embed = self, 
            address = address, 
            state = state, 
            author = author, 
            api_id = api_id, 
            private = False,
            dynamic = dynamic,
            _legroom = None,
            threadsafe_callbacks = [],
            async_callbacks = [],
        )
            
        # Well this is a huge hack. But something about the event loop 
        # itself is fucking up control flow and causing the world to hang
        # here.
        # TODO: fix this.
        worker = threading.Thread(
            target = self._object_handlers[api_id],
            daemon = True,
            args = (obj,),
            name = _generate_threadnames('embedshr')[0],
        )
        worker.start()
        
        # Successful delivery. Return true
        return b'\x01'
        
    async def deliver_object_wrapper(self, session, request_body):
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
        obj = AppObj(
            embed = self, 
            address = address, 
            state = state, 
            author = author, 
            api_id = api_id, 
            private = False,
            dynamic = dynamic,
            _legroom = None,
            threadsafe_callbacks = [],
            async_callbacks = [],
        )
            
        # Well this is a huge hack. But something about the event loop 
        # itself is fucking up control flow and causing the world to hang
        # here.
        # TODO: fix this.
        worker = threading.Thread(
            target = self._object_handlers[api_id],
            daemon = True,
            args = (obj,),
            name = _generate_threadnames('embedobj')[0],
        )
        worker.start()
        
        # Successful delivery. Return true
        return b'\x01'

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
                
        if address in self._objs_by_ghid:
            if is_link:
                link = Ghid.from_bytes(state)
                # Note: this may cause things to freeze, because async
                state = self.get_object(link)
            
            await self._objs_by_ghid[address]._update(state)
            await self._objs_by_ghid[address]._notify_callbacks()
            
            # self._ws_loop.call_soon_threadsafe(, state)
            # self._objs_by_ghid[address]._update(state)
            
            # python tests/trashtest/trashtest_ipc_hosts.py
            
        return b'\x01'
        
    async def delete_object_wrapper(self, session, request_body):
        ''' Deserializes an incoming object deletion, and applies it to
        the object.
        '''
        ghid = Ghid.from_bytes(request_body)
        await self._objs_by_ghid[ghid]._delete()
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