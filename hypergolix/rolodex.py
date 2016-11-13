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
import weakref
import traceback
import threading
import collections

from golix import SecondParty
from golix import Ghid

from golix.utils import AsymHandshake
from golix.utils import AsymAck
from golix.utils import AsymNak

# Local dependencies
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api

from .exceptions import UnknownParty

from .persistence import _GarqLite
from .persistence import _GdxxLite

from .utils import call_coroutine_threadsafe


# ###############################################
# Boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    'Rolodex',
]


# ###############################################
# Library
# ###############################################
            
            
_SharePair = collections.namedtuple(
    typename = '_SharePair',
    field_names = ('ghid', 'recipient'),
)


class Rolodex(metaclass=API):
    ''' Handles sharing, requests, etc.
    
    In the future, will maintain a contacts list to approve/reject
    incoming requests. In the further future, will maintain sharing
    pipelines, negotiated through handshakes, to perform sharing
    symmetrically between contacts.
    '''
    
    @public_api
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._opslock = threading.Lock()
        
        self._golcore = None
        self._privateer = None
        self._percore = None
        self._librarian = None
        self._ghidproxy = None
        self._ipccore = None
        self._salmonator = None
        
        # Persistent dict-like lookup for
        # request_ghid -> (request_target, request recipient)
        self._pending_requests = None
        # Lookup for <target_obj_ghid, recipient> -> set(<app_tokens>)
        self._outstanding_shares = None
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' Init the fixture.
        '''
        super(Rolodex.__fixture__, self).__init__(*args, **kwargs)
        
        self.shared = {}
        self._pending_requests = {}
        self._outstanding_shares = {}
        
    @fixture_api
    def RESET(self):
        ''' Super simple reset alias to __init__.
        '''
        self.shared.clear()
        self._pending_requests.clear()
        self._outstanding_shares.clear()
        
    def bootstrap(self, pending_requests, outstanding_shares):
        ''' Initialize distributed state.
        '''
        # Persistent dict-like lookup for
        # request_ghid -> (request_target, request recipient)
        self._pending_requests = pending_requests
        
        # These need to be distributed but aren't yet. TODO!
        # Lookup for <target_obj_ghid, recipient> -> set(<app_tokens>)
        self._outstanding_shares = outstanding_shares
        
    def assemble(self, golix_core, privateer, dispatch, persistence_core,
                 librarian, salmonator, ghidproxy, ipc_core):
        # Chicken, meet egg.
        self._golcore = weakref.proxy(golix_core)
        self._privateer = weakref.proxy(privateer)
        self._dispatch = weakref.proxy(dispatch)
        self._librarian = weakref.proxy(librarian)
        self._salmonator = weakref.proxy(salmonator)
        self._percore = weakref.proxy(persistence_core)
        self._ghidproxy = weakref.proxy(ghidproxy)
        self._ipccore = weakref.proxy(ipc_core)
        
    @public_api
    async def share_object(self, target, recipient, requesting_token):
        ''' Share a target ghid with the recipient.
        '''
        if not isinstance(target, Ghid):
            raise TypeError(
                'target must be Ghid or similar.'
            )
        if not isinstance(recipient, Ghid):
            raise TypeError(
                'recipient must be Ghid or similar.'
            )

        sharepair = _SharePair(target, recipient)
            
        # For now, this is just doing a handshake with some typechecking.
        self._hand_object(*sharepair)
        
        if requesting_token is not None:
            self._outstanding_shares.add(sharepair, requesting_token)
            
    @share_object.fixture
    async def share_object(self, target, recipient, requesting_token):
        ''' Pretty basic.
        '''
        # Ignore requesting token, because we won't generally set it when
        # testing this.
        self.shared[target] = recipient
        
    def _hand_object(self, target, recipient):
        ''' Initiates a handshake request with the recipient.
        '''
        if recipient not in self._librarian:
            try:
                self._salmonator.attempt_pull(recipient)
            except Exception as exc:
                raise UnknownParty(
                    'Recipient unknown: ' + str(recipient)
                ) from exc
            
        contact = SecondParty.from_packed(
            self._librarian.retrieve(recipient)
        )
        
        # This is guaranteed to resolve the container fully.
        container_ghid = self._ghidproxy.resolve(target)
        
        with self._opslock:
            # TODO: fix Golix library so this isn't such a shitshow re:
            # breaking abstraction barriers.
            handshake = self._golcore._identity.make_handshake(
                target = target,
                secret = self._privateer.get(container_ghid)
            )
            
            request = self._golcore._identity.make_request(
                recipient = contact,
                request = handshake
            )
        
        # Note that this must be called before publishing to the persister, or
        # there's a race condition between them.
        self._pending_requests[request.ghid] = target, recipient
        
        # Note the potential race condition here. Should catch errors with the
        # persister in case we need to resolve pending requests that didn't
        # successfully post.
        self._percore.ingest_garq(request)
        
    def notification_handler(self, subscription, notification):
        ''' Callback to handle any requests.
        '''
        if notification not in self._librarian:
            self._salmonator.attempt_pull(notification, quiet=True)
        
        # Note that the notification could also be a GDXX.
        request_or_debind = self._librarian.summarize(notification)
        
        if isinstance(request_or_debind, _GarqLite):
            self._handle_request(notification)
            
        elif isinstance(request_or_debind, _GdxxLite):
            # This case should only be relevant if we have multiple agents 
            # logged in at separate locations at the same time, processing the
            # same GARQs.
            self._handle_debinding(request_or_debind)
            
        else:
            raise RuntimeError(
                'Unexpected Golix primitive while listening for requests.'
            )
            
    def _handle_request(self, notification):
        ''' The notification is an asymmetric request. Deal with it.
        '''
        try:
            packed = self._librarian.retrieve(notification)
            unpacked = self._golcore.unpack_request(packed)
        
            # TODO: have this filter based on contacts.
            if unpacked.author not in self._librarian:
                try:
                    self._salmonator.attempt_pull(unpacked.author)
                except Exception as exc:
                    raise UnknownParty(
                        'Request author unknown: ' + str(unpacked.author)
                    ) from exc
        
            payload = self._golcore.open_request(unpacked)
            self._dispatch_payload(payload, notification)

        # Don't forget to (always) debind.
        finally:
            debinding = self._golcore.make_debinding(notification)
            self._percore.ingest_gdxx(debinding)
        
    def _handle_debinding(self, debinding):
        ''' The notification is a debinding. Deal with it.
        '''
        # For now we just need to remove any pending requests for the
        # debinding's target.
        try:
            del self._pending_requests[debinding.target]
        except KeyError:
            logger.debug(
                str(debinding.target) + ' missing in debind w/ traceback ' +
                ''.join(traceback.format_exc())
            )
        
    def _dispatch_payload(self, payload, source_ghid):
        ''' Appropriately handles a request payload.
        '''
        if isinstance(payload, AsymHandshake):
            self._handle_handshake(payload, source_ghid)
            
        elif isinstance(payload, AsymAck):
            self._handle_ack(payload)
            
        elif isinstance(payload, AsymNak):
            self._handle_nak(payload)
            
        else:
            raise RuntimeError('Encountered an unknown request type.')
            
    def _handle_handshake(self, request, source_ghid):
        ''' Handles a handshake request after reception.
        '''
        try:
            # First, we need to figure out what the actual container object's
            # address is, and then stage the secret for it.
            container_ghid = self._ghidproxy.resolve(request.target)
            self._privateer.quarantine(
                ghid = container_ghid,
                secret = request.secret
            )
            
            # Note that unless we raise a HandshakeError RIGHT NOW, we'll be
            # sending an ack to the handshake, just to indicate successful
            # receipt of the share. If the originating app wants to check for
            # availability, well, that's currently on them. In the future, add
            # handle for that in SHARE instead of HANDSHAKE?
            
        except Exception as exc:
            logger.error(
                'Exception encountered while handling a handshake. Returned a '
                'NAK.\n' + ''.join(traceback.format_exc())
            )
            # Erfolglos. Send a nak to whomever sent the handshake
            response_obj = self._golcore._identity.make_nak(
                target = source_ghid
            )
            
        else:
            # Success. Send an ack to whomever sent the handshake
            response_obj = self._golcore._identity.make_ack(
                target = source_ghid
            )
        
        response = self._golcore.make_request(request.author, response_obj)
        self._percore.ingest_garq(response)
            
        self.share_handler(request.target, request.author)
            
    def _handle_ack(self, request):
        ''' Handles a handshake ack after reception.
        '''
        try:
            target, recipient = self._pending_requests.pop(request.target)
        except KeyError:
            logger.error(
                'Received an ACK for an unknown origin: ' +
                str(request.target)
            )
        else:
            self.receipt_ack_handler(target, recipient)
            
    def _handle_nak(self, request):
        ''' Handles a handshake nak after reception.
        '''
        try:
            target, recipient = self._pending_requests.pop(request.target)
        except KeyError:
            logger.error(
                'Received a NAK for an unknown origin: ' +
                str(request.target)
            )
        else:
            self.receipt_nak_handler(target, recipient)
    
    def share_handler(self, target, sender):
        ''' Incoming share targets (well, their ghids anyways) are 
        forwarded to the _ipccore.
        
        Only separate from _handle_handshake right now because in the
        future, object sharing will be at least partly handled within 
        its own dedicated rolodex pipeline.
        '''
        call_coroutine_threadsafe(
            coro = self._ipccore.process_share(target, sender),
            loop = self._ipccore._loop
        )
    
    def receipt_ack_handler(self, target, recipient):
        ''' Receives a share ack from the rolodex and passes it on to 
        the application that requested the share.
        '''
        sharepair = _SharePair(target, recipient)
        tokens = self._outstanding_shares.pop_any(sharepair)
        
        call_coroutine_threadsafe(
            coro = self._ipccore.process_share_success(
                target, 
                recipient, 
                tokens
            ),
            loop = self._ipccore._loop
        )
    
    def receipt_nak_handler(self, target, recipient):
        ''' Receives a share nak from the rolodex and passes it on to 
        the application that requested the share.
        '''
        sharepair = _SharePair(target, recipient)
        tokens = self._outstanding_shares.pop_any(sharepair)
        
        call_coroutine_threadsafe(
            coro = self._ipccore.process_share_failure(
                target,
                recipient,
                tokens
            ),
            loop = self._ipccore._loop
        )