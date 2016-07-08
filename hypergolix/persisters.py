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
0x0003: Unknown author or recipient.
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
from .exceptions import UnboundContainerError
from .exceptions import DoesNotExistError
from .exceptions import PersistenceWarning
from .exceptions import RequestError

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
    b'\x00\x04': UnboundContainerError,
    b'\x00\x08': DoesNotExistError,
}


class _PersisterBase(metaclass=abc.ABCMeta):
    ''' Base class for persistence providers.
    '''
    def __init__(self, *args, **kwargs):
        ''' Note: if this starts to do anything that links need to 
        include (ie WSPersister), you'll need to change them to call
        super().
        '''
        self._golix_provider = ThirdParty()
        
        super().__init__(*args, **kwargs)
    
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
            raise UnboundContainerError(
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
            raise DoesNotExistError('0x0008: Ghid not found in store.') from e
        
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
                raise NakError(
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
        

class MemoryPersister(UnsafeMemoryPersister):
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
            
            
class _Librarian:
    ''' Keeps objects. Should contain the only strong references to the
    objects it keeps. Threadsafe.
    '''
    def __init__(self):
        ''' Sets up internal tracking.
        '''
        # Lookup for ghid -> raw bytes
        self._shelf = {}
        # Lookup for ghid -> hypergolix description
        self._catalog = {}
        # Operations lock
        self._opslock = threading.Lock()
        
    def tend(self, raw_obj):
        ''' Starts tracking an object.
        raw_obj is a Golix object.
        '''
        for cls, converter in (
        (GIDC, self._convert_gidc),
        (GEOC, self._convert_geoc),
        (GOBS, self._convert_gobs),
        (GOBD, self._convert_gobd),
        (GDXX, self._convert_gdxx),
        (GARQ, self._convert_garq)):
            if isinstance(raw_obj, cls):
                obj = converter(raw_obj)
                break
                
        with self._opslock:
            self._shelf[obj.ghid] = raw_obj.packed
            self._catalog[obj.ghid] = obj
        
    def abandon(self, ghid):
        ''' Stops tracking an object. Will result in it being GC'd by
        python.
        
        Raises KeyError if not currently tending the ghid.
        '''
        with self._opslock:
            del self._shelf[ghid]
            del self._catalog[ghid]
        
    def dereference(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        return self._shelf[ghid]
        
    def whois(self, ghid):
        ''' Returns a lightweight Hypergolix description of the object.
        '''
        return self._catalog[ghid]
        
    @staticmethod
    def _convert_gidc(gidc):
        ''' Converts a Golix object into a Hypergolix description.
        '''
        return _GidcLite(gidc.ghid)
        
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
        
        
class _GidcLite:
    ''' Lightweight description of a GIDC.
    '''
    __slots__ = [
        'ghid',
        '__weakref__',
    ]
    
    def __init__(self, ghid):
        self.ghid = ghid
        # self.key_sign
        # self.key_encr
        # self.key_exch
        # self.cipher
        
        
class _GeocLite:
    ''' Lightweight description of a GEOC.
    '''
    __slots__ = [
        'ghid',
        'author',
        '__weakref__',
    ]
    
    def __init__(self, ghid, author):
        self.ghid = ghid
        self.author = author
        
        
class _GobsLite:
    ''' Lightweight description of a GOBS.
    '''
    __slots__ = [
        'ghid',
        'author',
        'target',
        '__weakref__',
    ]
    
    def __init__(self, ghid, author, target):
        self.ghid = ghid
        self.author = author
        self.target = target
    
        
class _GobdLite:
    ''' Lightweight description of a GOBD.
    '''
    __slots__ = [
        'ghid',
        'author',
        'target',
        'frame_ghid',
        'history',
        '__weakref__',
    ]
    
    def __init__(self, ghid, author, target, frame_ghid, history):
        self.ghid = ghid
        self.author = author
        self.target = target
        self.frame_ghid = frame_ghid
        self.history = history
    
        
class _GdxxLite:
    ''' Lightweight description of a GDXX.
    '''
    __slots__ = [
        'ghid',
        'author',
        'target',
        '__weakref__',
    ]
    
    def __init__(self, ghid, author, target):
        self.ghid = ghid
        self.author = author
        self.target = target
        
        
class _GarqLite:
    ''' Lightweight description of a GARQ.
    '''
    __slots__ = [
        'ghid',
        'recipient',
        '__weakref__',
    ]
    
    def __init__(self, ghid, recipient):
        self.ghid = ghid
        self.recipient = recipient
    