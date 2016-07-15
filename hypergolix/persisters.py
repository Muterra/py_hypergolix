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
    'DiskPersister'
]

# Global dependencies
import abc
import collections
import warnings
import functools
import struct
import weakref
import queue

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
    def list_debinding(self, ghid):
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
        

class UnsafeMemoryPersister(_PersisterBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Lookup for GIDC authors, {<Ghid>: <secondparty>}
        self._id_bases = {}
        # Lookup for dynamic author proxies, {<Ghid>: <secondparty>}
        self._id_proxies = {}
        # Lookup for all valid authors, {<Ghid>: <secondparty>}
        self._secondparties = _DeepDeleteChainMap(
            self._id_bases,
            self._id_proxies
        )
        # All objects. {<Ghid>: <packed object>}
        # Includes proxy reference from dynamic ghids.
        self._store = {}
        # Dynamic states. {<Dynamic Ghid>: tuple(<current history>)}
        # self._dynamic_states = {}
        
        # Forward lookup for static bindings, 
        # {
        #   <binding Ghid>: (<binder Ghid>, tuple(<bound Ghid>))
        # }
        self._targets_static = {}
        
        # Forward lookup for dynamic bindings, 
        # {
        #   <dynamic ghid>: (<binder ghid>, tuple(<current history>))
        # }
        self._targets_dynamic = {}
        
        # Forward lookup for debindings, 
        # {
        #   <debinding Ghid>: (<debinder Ghid>, tuple(<debound Ghid>))
        # }
        self._targets_debind = {}
        
        # Forward lookup for asymmetric requests
        # {
        #   <garq Ghid>: (<recipient Ghid>,)
        # }
        self._requests = {}
        
        # Forward lookup for targeted objects. Note that a ghid can only be one 
        # thing, so a chainmap is appropriate.
        self._targets = _DeepDeleteChainMap(
            self._targets_static,
            self._targets_dynamic,
            self._targets_debind
        )
        
        # Forward lookup for everything.
        self._forward_references = _DeepDeleteChainMap(
            self._targets_static,
            self._targets_dynamic,
            self._targets_debind,
            self._requests
        ) 
        
        # Reverse lookup for bindings, {<bound Ghid>: {<bound by Ghid>}}
        self._bindings_static = {}
        # Reverse lookup for dynamic bindings {<bound Ghid>: {<bound by Ghid>}}
        self._bindings_dynamic = {}
        # Reverse lookup for implicit bindings {<bound Ghid>: {<bound Ghid>}}
        self._bindings_implicit = {}
        # Reverse lookup for any valid bindings
        self._bindings = _WeldedSetDeepChainMap(
            self._bindings_static,
            self._bindings_dynamic,
            self._bindings_implicit
        )
        # Reverse lookup for debindings, {<debound Ghid>: {<debound by Ghid>}}
        self._debindings = {}
        # Reverse lookup for everything, same format as other bindings
        self._reverse_references = _WeldedSetDeepChainMap(
            self._bindings_static,
            self._bindings_dynamic,
            self._bindings_implicit,
            self._debindings
        )
        
        # Lookup for subscriptions, {<subscribed Ghid>: [callbacks]}
        self._subscriptions = {}
        # Parallel lookup for any subscription notifications that are blocking
        # on an updated target. {<pending target>: <sub ghid, notify ghid>}
        self._pending_notifications = {}
        
    def publish(self, unpacked):
        ''' Handles publishing for an unpacked object. DOES NOT perform
        ghid verification. ONLY USE THIS IF YOU CREATED THE UNPACKED 
        OBJECT YOURSELF, or have already performed your own unpacking 
        step. However, will still perform signature verification.
        '''
        # Select a dispatch function by its type, and raise TypeError if no
        # valid type found.
        for case, dispatch in (
            (GIDC, self._dispatch_gidc),
            (GEOC, self._dispatch_geoc),
            (GOBS, self._dispatch_gobs),
            (GOBD, self._dispatch_gobd),
            (GDXX, self._dispatch_gdxx),
            (GARQ, self._dispatch_garq)
        ):
                if isinstance(unpacked, case):
                    dispatch(unpacked)
                    break
        else:
            logger.debug('0x0001: Does not appear to be a Golix object.')
            raise NakError('0x0001: Does not appear to be a Golix object.')
        
        return True
        
    def _verify_obj(self, assignee, obj):
        ''' Ensures assignee (author/binder/recipient/etc) is known to
        the storage provider and verifies obj.
        '''
        if assignee not in self._secondparties:
            try:
                self.get(assignee)
            except NakError as e:
                logger.info('0x0003: Unknown author / recipient.')
                raise NakError(
                    '0x0003: Unknown author / recipient.'
                ) from e
    
        try:
            # This will raise a SecurityError if verification fails.
            self._golix_provider.verify_object(
                second_party = self._secondparties[assignee],
                obj = obj
            )
        except SecurityError as e:
            logger.warning('0x0002: Failed to verify GEOC.')
            raise NakError(
                '0x0002: Failed to verify GEOC.'
            ) from e
            
    def _dispatch_gidc(self, gidc):
        ''' Does whatever is needed to preprocess a GIDC.
        '''
        # Note that GIDC do not require verification beyond unpacking.
        author = gidc.ghid
        
        if author not in self._id_bases:
            secondparty = SecondParty.from_identity(gidc)
            self._id_bases[author] = secondparty
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gidc.ghid] = gidc
            
    def _dispatch_geoc(self, geoc):
        ''' Does whatever is needed to preprocess a GEOC.
        '''
        self._verify_obj(
            assignee = geoc.author,
            obj = geoc
        )
            
        if geoc.ghid not in self._bindings:
            logger.debug('0x0004: Unbound container')
            raise UnboundContainer(
                '0x0004: Attempt to upload unbound GEOC; object immediately '
                'garbage collected.'
            )
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[geoc.ghid] = geoc
        
        # Update any pending notifications and then clean up.
        if geoc.ghid in self._pending_notifications:
            self._check_for_subs(
                subscription_ghid = self._pending_notifications[geoc.ghid][0],
                notification_ghid = self._pending_notifications[geoc.ghid][1]
            )
            del self._pending_notifications[geoc.ghid]
            
    def _dispatch_gobs(self, gobs):
        ''' Does whatever is needed to preprocess a GOBS.
        '''
        self._verify_obj(
            assignee = gobs.binder,
            obj = gobs
        )
        
        if gobs.ghid in self._debindings:
            logger.warning('0x0005: Already debound')
            raise NakError(
                '0x0005: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        # Check for any KNOWN illegal targets, and fail loudly if so
        self._check_illegal_gobs_target(gobs)
        
        # Check to see if someone has already uploaded an illegal binding for 
        # this static binding (due to the race condition in target inspection).
        # If so, force garbage collection.
        self._check_illegal_binding(gobs.ghid)
        
        # Update the state of local bindings
        # Assuming this is an atomic change, we'll always need to do
        # both of these, or neither.
        if gobs.target in self._bindings_static:
            self._bindings_static[gobs.target].add(gobs.ghid)
        else:
            self._bindings_static[gobs.target] = { gobs.ghid }
        # These must, by definition, be identical for any repeated ghid, so 
        # it doesn't much matter if we already have them.
        self._targets_static[gobs.ghid] = gobs.binder, (gobs.target,)
        self._bindings_implicit[gobs.ghid] = { gobs.ghid }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gobs.ghid] = gobs
            
    def _dispatch_gobd(self, gobd):
        ''' Does whatever is needed to preprocess a GOBD.
        '''
        self._verify_obj(
            assignee = gobd.binder,
            obj = gobd
        )
        
        if gobd.ghid_dynamic in self._debindings:
            logger.warning('0x0005: Already debound')
            raise NakError(
                '0x0005: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        # Check for any KNOWN illegal targets, and fail loudly if so
        self._check_illegal_gobd_target(gobd)
        
        # This needs to be done before updating local bindings, since it does
        # some verification.
        if gobd.ghid_dynamic in self._targets_dynamic:
            self._dispatch_updated_dynamic(gobd)
        else:
            self._dispatch_new_dynamic(gobd)
            
        # Status check: we now know we have a legal binding, and that
        # self._targets_dynamic[ghid_dynamic] exists, and that any existing
        # **targets** have been removed and GC'd.
        
        old_history = set(self._targets_dynamic[gobd.ghid_dynamic][1])
        new_history = set(gobd.history)
        
        # Remove implicit bindings for any expired frames and call GC_check on
        # them.
        # Might want to make this suppress persistence warnings.
        for expired in old_history - new_history:
            # Remove any explicit bindings from the dynamic ghid to the frames
            self._bindings_dynamic[expired].remove(gobd.ghid_dynamic)
            self._gc_check(expired)
            
        # Update the state of target bindings
        if gobd.target in self._bindings_dynamic:
            self._bindings_dynamic[gobd.target].add(gobd.ghid_dynamic)
        else:
            self._bindings_dynamic[gobd.target] = { gobd.ghid_dynamic }
        
        # Create a new state.
        self._targets_dynamic[gobd.ghid_dynamic] = (
            gobd.binder,
            (gobd.ghid,) + tuple(gobd.history) + (gobd.target,)
        )
        self._bindings_dynamic[gobd.ghid] = { gobd.ghid_dynamic }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gobd.ghid] = gobd
        # Overwrite any existing value, or create a new one.
        self._store[gobd.ghid_dynamic] = gobd
        
        # Update any subscribers (if they exist) that an updated frame has 
        # been issued. May need to delay until target is uploaded (race cond)
        self._check_for_subs(
            subscription_ghid = gobd.ghid_dynamic,
            notification_ghid = gobd.ghid,
            target_ghid = gobd.target
        )
        
    def _dispatch_new_dynamic(self, gobd):
        ''' Performs validation for a dynamic binding that the persister
        is encountering for the first time. Mostly ensures that the
        first frame seen by the persistence provider is, in fact, the
        root frame for the binding, in order to validate the dynamic 
        address.
        
        Also preps the history declaration so that subsequent operations
        in _dispatch_gobd can be identical between new and updated 
        bindings.
        '''
        # Note: golix._getlow.GOBD will perform verification of correct dynamic
        # hash address when unpacking new bindings, so we only need to make
        # sure that it has no history.
        if gobd.history:
            logger.info('0x0009: Zeroth frame has history.')
            raise NakError(
                '0x0009: Illegal dynamic frame. Cannot upload a frame with '
                'history as the first frame in a persistence provider.'
            )
            
        # self._dynamic_states[gobd.ghid_dynamic] = collections.deque()
        self._targets_dynamic[gobd.ghid_dynamic] = (None, tuple())
        self._bindings_implicit[gobd.ghid_dynamic] = { gobd.ghid_dynamic }
        
    def _dispatch_updated_dynamic(self, gobd):
        ''' Performs validation for a dynamic binding from which the 
        persister has an existing frame. Checks author consistency, 
        historical progression, and calls garbage collection on any 
        expired frames.
        '''
        # Verify consistency of binding author
        if gobd.binder != self._targets_dynamic[gobd.ghid_dynamic][0]:
            logger.warning('0x0007: Inconsistent dynamic author.')
            raise NakError(
                '0x0007: Dynamic binding author is inconsistent with an '
                'existing dynamic binding at the same address.'
            )
            
        
        # Verify history contains existing most recent frame
        if self._targets_dynamic[gobd.ghid_dynamic][1][0] not in gobd.history:
            logger.warning('0x0009: Existing frame not in history.')
            raise NakError(
                '0x0009: Illegal dynamic frame. Attempted to upload a new '
                'dynamic frame, but its history did not contain the most '
                'recent frame.'
            )
            
        # This is unnecessary -- it's all handled through self._targets_dynamic
        # in the dispatch_gobd method.
        # # Release hold on previous target. WARNING: currently this means that
        # # updating dynamic bindings is a non-atomic change.
        # old_frame = self._targets_dynamic[gobd.ghid_dynamic][1][0]
        # old_target = self._store[old_frame].target
        # # This should always be true, unless something was forcibly GC'd
        # if old_target in self._bindings_dynamic:
        #     self._bindings_dynamic[old_target].remove(gobd.ghid_dynamic)
        #     self._gc_check(old_target)
            
    def _dispatch_gdxx(self, gdxx):
        ''' Does whatever is needed to preprocess a GDXX.
        
        Also performs a garbage collection check.
        '''
        self._verify_obj(
            assignee = gdxx.debinder,
            obj = gdxx
        )
        
        if gdxx.ghid in self._debindings:
            logger.warning('0x0005: Already debound')
            raise NakError(
                '0x0005: Attempt to upload a debinding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        if gdxx.target not in self._forward_references:
            logger.info('0x0006: Invalid target')
            raise NakError(
                '0x0006: Invalid target for debinding. Debindings must target '
                'static/dynamic bindings, debindings, or asymmetric requests. '
                'This may indicate the target does not exist in local storage.'
            )
            
        # Check debinder is consistent with other (de)bindings in the chain
        if gdxx.debinder != self._forward_references[gdxx.target][0]:
            logger.warning('0x0007: Inconsistent debinder')
            raise NakError(
                '0x0007: Debinding author is inconsistent with the resource '
                'being debound.'
            )
            
        # Note: if the entire rest of this is not performed atomically, errors
        # will result.
            
        # Must be added to the store before updating subscribers.
        self._store[gdxx.ghid] = gdxx
            
        # Update any subscribers (if they exist) that a GDXX has been issued.
        # Must be called before removing bindings, or the subscription record
        # will be removed.
        self._check_for_subs(
            subscription_ghid = gdxx.target,
            notification_ghid = gdxx.ghid
        )
            
        # Debindings can only target one thing, so it doesn't much matter if it
        # already exists (we've already checked for replays)
        self._debindings[gdxx.target] = { gdxx.ghid }
        self._targets_debind[gdxx.ghid] = gdxx.debinder, (gdxx.target,)
        self._bindings_implicit[gdxx.ghid] = { gdxx.ghid }
        
        # Targets will always have an implicit binding. Remove it.
        del self._bindings_implicit[gdxx.target]
        
        # Check for (and possibly perform) garbage collect on the target.
        # Cannot blindly call _gc_execute because of dynamic bindings.
        # Note that, to correctly handle implicit bindings, this MUST come
        # after adding the current debinding to the internal store.
        self._gc_check(gdxx.target)
            
    def _dispatch_garq(self, garq):
        ''' Does whatever is needed to preprocess a GARQ.
        
        Also notifies any subscribers to that recipient address.
        '''
        # Don't call verify, since it would error out, as GARQ are not
        # verifiable by a third party.
        if garq.recipient not in self._secondparties:
            try:
                self.get(garq.recipient)
            except NakError as e:
                logger.info('0x0003: Unknown author / recipient.')
                raise NakError(
                    '0x0003: Unknown author / recipient.'
                ) from e
            
        if garq.ghid in self._debindings:
            logger.warning('0x0005: Already debound')
            raise NakError(
                '0x0005: Attempt to upload a request for which a debinding '
                'already exists. Remove the debinding first.'
            )
        
        # Check to see if someone has already uploaded an illegal binding for 
        # this request (due to the race condition in target inspection).
        # If so, force garbage collection.
        self._check_illegal_binding(garq.ghid)
            
        # Update persister state information
        self._requests[garq.ghid] = (garq.recipient,)
        self._bindings_implicit[garq.ghid] = { garq.ghid }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[garq.ghid] = garq
        
        # Call the subscribers after adding, in case they request it.
        # Also in case they error out.
        self._check_for_subs(
            subscription_ghid = garq.recipient,
            notification_ghid = garq.ghid
        )
                
    def _check_for_subs(self, subscription_ghid, notification_ghid, target_ghid=None):
        ''' Check for subscriptions, and update them if any exist.
        '''
        if subscription_ghid in self._subscriptions:
            # If the target isn't defined, or if it exists, clear to update
            if not target_ghid or self.query(target_ghid):
                for callback in self._subscriptions[subscription_ghid]:
                    callback(notification_ghid)
            # Otherwise it's defined and missing; wait until we get the target.
            else:
                self._pending_notifications[target_ghid] = \
                    subscription_ghid, notification_ghid
                
    def _check_illegal_binding(self, ghid):
        ''' Checks for an existing binding for ghid. If it exists,
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

    def _check_illegal_gobs_target(self, gobs):
        ''' Checks for illegal targets for GOBS objects. Only guarantees
        a valid target if the target is already known.
        '''
        # Check for any KNOWN illegal targets, and fail loudly if so
        if (gobs.target in self._targets_static or
            gobs.target in self._targets_debind or
            gobs.target in self._requests or
            gobs.target in self._id_bases):
                logger.info('0x0006: Invalid static binding target.')
                raise NakError(
                    '0x0006: Attempt to bind to an invalid target.'
                )
                
        return True

    def _check_illegal_gobd_target(self, gobd):
        ''' Checks for illegal targets for GOBS objects. Only guarantees
        a valid target if the target is already known.
        '''
        # Check for any KNOWN illegal targets, and fail loudly if so
        if (gobd.target in self._targets_static or
            gobd.target in self._requests or
            gobd.target in self._id_bases):
                logger.info('0x0006: Invalid dynamic binding target.')
                raise NakError(
                    '0x0006: Attempt to bind to an invalid target.'
                )
                
        return True
        
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        return True
        
    def get(self, ghid):
        ''' Returns an unpacked Golix object. Only use this if you 100%
        trust the PersistenceProvider to perform all public verification
        of objects.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        try:
            return self._store[ghid]
        except KeyError as e:
            logger.debug('0x0008: Ghid not found in store.')
            raise DoesNotExist('0x0008: Ghid not found in store.') from e
        
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
        if not callable(callback):
            raise TypeError('callback must be callable.')
            
        if (ghid not in self._targets_dynamic and 
            ghid not in self._secondparties):
                logger.debug('0x0006: Invalid or unknown subscription target.')
                raise InvalidTarget(
                    '0x0006: Invalid or unknown target for subscription.'
                )
                
        if ghid in self._subscriptions:
            self._subscriptions[ghid].add(callback)
        else:
            self._subscriptions[ghid] = { callback }
            
        return True
        
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed ghid at the persistence provider.
        
        ACK/success is represented by: 
            return True if subscription existed
            return False is subscription did not exist
        NAK/failure is represented by raise NakError
        '''
        try:
            if callback not in self._subscriptions[ghid]:
                raise RuntimeError('Callback not registered for ghid.')
                
            self._subscriptions[ghid].remove(callback)
                
            if len(self._subscriptions[ghid]) == 0:
                del self._subscriptions[ghid]
                
        except KeyError as e:
            return False
            
        else:
            return True
        
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected 
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        return tuple(self._subscriptions)
    
    def list_bindings(self, ghid):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        NOTE: does not currently satisfy condition 3 in the spec (aka,
        it is not currently preferentially listing the author binding 
        first).
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        result = []
        # Bindings can only be static XOR dynamic, so we need not worry about
        # set intersections. But note that the lookup dicts contain sets as 
        # keys, so we need to extend, not append.
        if ghid in self._bindings_static:
            result.extend(self._bindings_static[ghid])
        if ghid in self._bindings_dynamic:
            result.extend(self._bindings_dynamic[ghid])
        return result
        
    def list_debinding(self, ghid):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. False if it does not exist
        NAK/failure is represented by raise NakError
        '''
        if ghid in self._debindings:
            return tuple(self._debindings[ghid])[0]
        else:
            return False
            
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise NakError
        '''
        if ghid in self._store:
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
        self._subscriptions = {}
        return True
    
    def _gc_orphan_bindings(self):
        ''' Removes any orphaned (target does not exist) dynamic or 
        static bindings.
        '''
        pass
        
    def _gc_check(self, ghid):
        ''' Checks for, and if needed, performs, garbage collection. 
        Only checks the passed ghid.
        '''
        # Case 1: not even in the store. Gitouttahere.
        if ghid not in self._store:
            return
            
        # Case 2: ghid is an identity (GIDC). Basically never delete those.
        elif ghid in self._id_bases:
            return
        
        # Case 3: Still bound?
        elif ghid in self._bindings:
            # Case 3a: still bound; something else is blocking. Warn.
            if len(self._bindings[ghid]) > 0:
                warnings.warn(
                    message = str(ghid) + ' has outstanding bindings.',
                    category = PersistenceWarning
                )
            # Case 3b: the binding length is zero; unbound; also remove.
            # This currently only happens when updating dynamic bindings.
            else:
                del self._bindings[ghid]
                self._gc_execute(ghid)
        # The resource is unbound. Perform GC.
        else:
            self._gc_execute(ghid)
                
    def _gc_execute(self, ghid):
        ''' Performs garbage collection on ghid.
        '''
        # This means it's a binding or debinding
        if ghid in self._targets:
            # Clean up the reverse lookup, then remove any empty sets
            for target in self._targets[ghid][1]:
                self._reverse_references[target].remove(ghid)
                self._reverse_references.remove_empty(target)
                # Perform recursive garbage collection check on the target
                self._gc_check(target)
            
            # Clean up the forward lookup
            del self._targets[ghid]
        # It cannot be both a _target and a _request
        elif ghid in self._requests:
            del self._requests[ghid]
            
        # Clean up any subscriptions.
        if ghid in self._subscriptions:
            del self._subscriptions[ghid]
            
        # Warn of state problems if still in bindings.
        if ghid in self._bindings:
            warnings.warn(
                message = str(ghid) + ' has conflicted state. It was removed '
                        'through forced garbage collection, but it still has '
                        'outstanding bindings.'
            )
            
        # And finally, clean up the store.
        del self._store[ghid]
        

class __MemoryPersister(UnsafeMemoryPersister):
    ''' A safe memory persister. Fully verifies everything in both 
    directions.
    '''
        
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        ACK is represented by a return True
        NAK is represented by raise NakError
        '''
        # This will raise if improperly formatted.
        try:
            obj = self._golix_provider.unpack_object(packed)
        except ParseError as exc:
            msg = (repr(exc) + '\n').join(traceback.format_tb(exc.__traceback__))
            logger.debug(msg)
            raise NakError('0x0001: Does not appear to be a Golix object.') from exc
        # We are now guaranteed a Golix object.
        
        return super().publish(obj)
        
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        return super().get(ghid).packed


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
        self.list_debinding = self.bookie.debind_status
        
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
        

class DiskPersister(_PersisterBase):
    pass


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
            b'LD': self.list_debinding_wrapper,
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
        
        except Exception as exc:
            msg = ('Error while receiving ping.\n' + repr(exc) + '\n' + 
                ''.join(traceback.format_tb(exc.__traceback__)))
            logger.error(msg)
            # return b'\x00'
            raise exc
            
    async def publish_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        session._processing.set()
        self._persister.publish(request_body)
        session._processing.clear()
        return b'\x01'
        obj = self._persister.ingest(request_body)
        self.schedule_ignore_update(obj.ghid, session)
        ensure_future(do_future_subs_update())
        
    def subs_callback(self, ghid):
        return True
        schedule_future_subs_update(ghid)
            
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
        
        def updater(notification_ghid, subscribed_ghid=ghid, 
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
            
    async def list_debinding_wrapper(self, session, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        result = self._persister.list_debinding(ghid)
        
        if not result:
            result = b'\x00'
        else:
            result = bytes(result)
        
        return result
            
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
        'list_debinding': b'LD',
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
                callback(notification_ghid)
        
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
    
    def list_debinding(self, ghid):
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
            request_code = self.REQUEST_CODES['list_debinding']
        )
        
        if response == b'\x00':
            return None
        else:
            return Ghid.from_bytes(response)
        
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
            debinding_ghid = self._bookie.debind_status(obj.ghid)
            if not debinding_ghid:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            self._scheduled.put(
                _MrPostcard(obj.ghid, debinding_ghid)
            )
        elif obj.target not in self._librarian:
            self._defer_update(
                awaiting_ghid = obj.target,
                subscribed_ghid = obj.ghid,
            )
        else:
            self._scheduled.put(
                _MrPostcard(obj.ghid, obj.ghid)
            )
        
    def _schedule_gdxx(self, obj, removed):
        # GDXX will never directly trigger a subscription. If they are removing
        # a subscribed object, the actual removal (in the undertaker GC) will 
        # trigger a subscription without us.
        pass
        
    def _schedule_garq(self, obj, removed):
        # GARQ might trigger a subscription! Or we might be removing it.
        if removed:
            debinding_ghid = self._bookie.debind_status(obj.ghid)
            if not debinding_ghid:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            self._scheduled.put(
                _MrPostcard(obj.recipient, debinding_ghid)
            )
        else:
            self._scheduled.put(
                _MrPostcard(obj.recipient, obj.ghid)
            )
            
    def _defer_update(self, awaiting_ghid, subscribed_ghid):
        ''' Defer a subscription notification until the awaiting_ghid is
        received as well.
        '''
        # Note that deferred updates will always be dynamic bindings, so the
        # subscribed ghid will be identical to the notification ghid.
        with self._opslock_defer:
            try:
                self._deferred[awaiting_ghid].add(
                    _MrPostcard(subscribed_ghid, subscribed_ghid)
                )
            except KeyError:
                self._deferred[awaiting_ghid] = { 
                    _MrPostcard(subscribed_ghid, subscribed_ghid) 
                }
            
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
            
            for primitive, gcollector in (
            (_GidcLite, self._gc_gidc),
            (_GeocLite, self._gc_geoc),
            (_GobsLite, self._gc_gobs),
            (_GobdLite, self._gc_gobd),
            (_GdxxLite, self._gc_gdxx),
            (_GarqLite, self._gc_garq)):
                if isinstance(obj, primitive):
                    gcollector(obj)
                    break
            else:
                # No appropriate GCer found (should we typerror?); so continue 
                # with WHILE loop
                continue
                logger.error('No appropriate GC routine found!')
            
        self._staging = None
        
    def triage(self, ghid):
        ''' Schedule GC check for object.
        
        Note: should triaging be order-dependent?
        '''
        logger.info('Performing triage.')
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
        logger.info('Entering gobs GC.')
        if self._bookie.is_debound(obj):
            logger.info('Gobs is debound. Staging target and executing GC.')
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
        if self._bookie.is_debound(obj):
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
        logger.info('-------------------------------------------')
        logger.info('Prepping for GOBD frame.')
        try:
            existing = self._librarian.whois(obj.ghid)
        except KeyError:
            # This will always happen if it's the first frame, so let's be sure
            # to ignore that for logging.
            if obj.history:
                logger.error('Could not find gobd to check existing target.')
        else:
            self.triage(existing.target)
            logger.info('Triaged existing target.')
            
        logger.info('-------------------------------------------')
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
        
    def validate_gdxx(self, obj):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding.
        '''
        self._validate_author(obj)
        try:
            existing = self._librarian.whois(obj.target)
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
        
    def validate_gdxx(self, obj):
        ''' Check if target is known, and if it is, validate it.
        '''
        try:
            target = self._librarian.whois(obj.target)
        except KeyError:
            logger.info('0x0006: Unknown debinding target.')
            raise InvalidTarget(
                '0x0006: Unknown debinding target. Cannot debind an unknown '
                'resource, to prevent a malicious party from preemptively '
                'uploading a debinding for a resource s/he did not bind.'
            )
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
    
    Threadsafe.
    '''
    def __init__(self, librarian):
        self._opslock = threading.Lock()
        self._librarian = librarian
        
        # Lookup <bound ghid>: set(<binding obj>)
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        self._bound_by_ghid = {}
        
        # Lookup <debound ghid>: <debinding ghid>
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        # Note that any particular object can have exactly zero or one debinds
        self._debound_by_ghid = {}
        # # Lookup <debinding ghid>: <debound ghid>
        # self._debound_to_ghid = weakref.WeakKeyDictionary()
        
        # Lookup <recipient>: set(<request ghid>)
        self._requests_for_recipient = {}
        
    def recipient_status(self, ghid):
        ''' Return a frozenset of ghids assigned to the passed ghid as
        a recipient.
        '''
        try:
            return frozenset(self._requests_for_recipient[ghid])
        except KeyError:
            return frozenset()
        
    def bind_status(self, ghid):
        ''' Return a frozenset of ghids binding the passed ghid.
        '''
        try:
            return frozenset(self._bound_by_ghid[ghid])
        except KeyError:
            return frozenset()
        
    def debind_status(self, ghid):
        ''' Return either a ghid, or None.
        '''
        try:
            return self._debound_by_ghid[ghid]
        except KeyError:
            return None
        
    def is_bound(self, obj):
        try:
            bindings = self._bound_by_ghid[obj.ghid]
        except KeyError:
            return False
        else:
            return True
            
    def is_debound(self, obj):
        try:
            debinding = self._debound_by_ghid[obj.ghid]
        except KeyError:
            return False
        else:
            return True
        
    def _add_binding(self, being_bound, doing_binding):
        try:
            self._bound_by_ghid[being_bound].add(doing_binding)
        except KeyError:
            self._bound_by_ghid[being_bound] = { doing_binding }
            
    def _remove_binding(self, obj):
        being_unbound = obj.target
        
        try:
            bindings_for_target = self._bound_by_ghid[being_unbound]
        except KeyError:
            logger.warning(
                'Attempting to remove a binding, but the bookie has no record '
                'of its existence.'
            )
        else:
            bindings_for_target.discard(obj.ghid)
            if len(bindings_for_target) == 0:
                del self._bound_by_ghid[being_unbound]
            
    def _remove_request(self, obj):
        recipient = obj.recipient
            
        try:
            self._requests_for_recipient[recipient].discard(obj.ghid)
        except KeyError:
            return
            
    def _remove_debinding(self, obj):
        target = obj.target
            
        try:
            del self._debound_by_ghid[target]
        except KeyError:
            return
        
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
        self._debound_by_ghid[obj.target] = obj.ghid
        
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
        try:
            self._requests_for_recipient[obj.recipient].add(obj.ghid)
        except KeyError:
            self._requests_for_recipient[obj.recipient] = { obj.ghid }
        
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
            
            
class _Librarian:
    ''' Keeps objects. Should contain the only strong references to the
    objects it keeps. Threadsafe.
    
    TODO: convert shelf, catalog to single object with function calls 
    instead of __getitem__ / __setitem__ / __delitem__
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
    bookie = bookie_class(librarian=librarian, **bookie_kwargs)
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
    
    return (core, doorman, enforcer, lawyer, bookie, librarian, undertaker, 
            postman)