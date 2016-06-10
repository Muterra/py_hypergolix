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

from .comms import WSReqResServer
from .comms import WSReqResClient
from .comms import _ReqResWSConnection


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
            raise NakError(
                '0x0007: Dynamic binding author is inconsistent with an '
                'existing dynamic binding at the same address.'
            )
            
        
        # Verify history contains existing most recent frame
        if self._targets_dynamic[gobd.ghid_dynamic][1][0] not in gobd.history:
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
            raise NakError(
                '0x0005: Attempt to upload a debinding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        if gdxx.target not in self._forward_references:
            raise NakError(
                '0x0006: Invalid target for debinding. Debindings must target '
                'static/dynamic bindings, debindings, or asymmetric requests. '
                'This may indicate the target does not exist in local storage.'
            )
            
        # Check debinder is consistent with other (de)bindings in the chain
        if gdxx.debinder != self._forward_references[gdxx.target][0]:
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
                raise NakError(
                    '0x0003: Unknown author / recipient.'
                ) from e
            
        if garq.ghid in self._debindings:
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
        except ParseError as e:
            print(repr(e))
            raise NakError('0x0001: Does not appear to be a Golix object.') from e
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
    
    
class ProxyPersister(_PersisterBase):
    ''' Allows for customized persister implementation using an IPC 
    proxy.
    '''
    pass


class _PersisterBridgeConnection(_ReqResWSConnection):
    def __init__(self, transport, *args, **kwargs):
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(transport, weakref.ProxyTypes):
            self._transport = transport
        else:
            self._transport = weakref.proxy(transport)
            
        self._subscriptions = {}
        
        super().__init__(*args, **kwargs)
    
    @abc.abstractmethod
    def send_subs_update(self, subscribed_ghid, notification_ghid):
        ''' Sends a subscription update to the connected client.
        '''
        pass


class _PersisterBridgeBase:
    ''' Serialization mixins for persister bridges.
    '''
    def __init__(self, persister, *args, **kwargs):
        # Copying like this seems dangerous, but I think it should be okay.
        if isinstance(persister, weakref.ProxyTypes):
            self._persister = persister
        else:
            self._persister = weakref.proxy(persister)
            
        super().__init__(*args, **kwargs)
            
    def new_connection(self, *args, **kwargs):
        ''' Merge the connection and endpoint to the same thing.
        '''
        return _PersisterBridgeConnection(
            transport = self, 
            *args, **kwargs
        )
            
    def ping_wrapper(self, connection, request_body):
        ''' Deserializes a ping request; forwards it to the persister.
        '''
        try:
            if self._persister.ping():
                return b'\x01'
            else:
                return b'\x00'
        
        except Exception as exc:
            print('Error while receiving ping.')
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
            # return b'\x00'
            raise exc
            
    def publish_wrapper(self, connection, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        self._persister.publish(request_body)
        return b'\x01'
            
    def get_wrapper(self, connection, request_body):
        ''' Deserializes a get request; forwards it to the persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        return self._persister.get(ghid)
            
    def subscribe_wrapper(self, connection, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        
        def updater(notification_ghid, subscribed_ghid=ghid, 
        call=connection.send_subs_update):
            call(subscribed_ghid, notification_ghid)
        
        self._persister.subscribe(ghid, updater)
        connection._subscriptions[ghid] = updater
        return b'\x01'
            
    def unsubscribe_wrapper(self, connection, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        callback = connection._subscriptions[ghid]
        unsubbed = self._persister.unsubscribe(ghid, callback)
        del connection._subscriptions[ghid]
        if unsubbed:
            return b'\x01'
        else:
            return b'\x00'
            
    def list_subs_wrapper(self, connection, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghidlist = list(connection._subscriptions)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    def list_bindings_wrapper(self, connection, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        ghidlist = self._persister.list_bindings(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
            
    def list_debinding_wrapper(self, connection, request_body):
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
            
    def query_wrapper(self, connection, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        ghid = Ghid.from_bytes(request_body)
        status = self._persister.query(ghid)
        if status:
            return b'\x01'
        else:
            return b'\x00'
            
    def disconnect_wrapper(self, connection, request_body):
        ''' Deserializes a publish request and forwards it to the 
        persister.
        '''
        # print('----- Attempting disconnect...')
        # print('Persister has', len(self._persister._subscriptions), 'subscriptions')
        # print('Connection has', len(connection._subscriptions), 'subscriptions')
        for sub_ghid, sub_callback in connection._subscriptions.items():
            # print('    ', bytes(sub_ghid))
            # print('    ', sub_callback)
            # print('    ', self._persister._subscriptions)
            # print('')
            self._persister.unsubscribe(sub_ghid, sub_callback)
        connection._subscriptions.clear()
        return b'\x01'


class _WSBridgeConnection(_PersisterBridgeConnection):
    def send_subs_update(self, subscribed_ghid, notification_ghid):
        ''' Send the connection its subscription update.
        '''
        # TODO: fix this leaky abstraction
        response = self._transport.send_threadsafe(
            connection = self,
            msg = bytes(subscribed_ghid) + bytes(notification_ghid),
            request_code = self._transport.REQUEST_CODES['send_subs_update'],
            # Note: for now, just don't worry about failures.
            expect_reply = False
        )
        
            
class _WSBridgeBase(_PersisterBridgeBase):
    def new_connection(self, *args, **kwargs):
        ''' Merge the connection and endpoint to the same thing.
        '''
        return _WSBridgeConnection(
            transport = self, 
            *args, **kwargs
        )
        
    @asyncio.coroutine
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            # print(repr(exc))
            # traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            # print(repr(exc))
            # traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_autoresponder_exc(self, exc, token):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None and not isinstance(exc, NakError):
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
        return repr(exc)
    

class WSPersisterBridge(_WSBridgeBase, WSReqResServer):
    ''' Websockets request/response bridge to a persister, server half.
    '''
    REQUEST_CODES = {
        # Receive an update for an existing object.
        'send_subs_update': b'!!',
    }
    
    def __init__(self, *args, **kwargs):
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
            # 48 bits = 1% collisions at 2.4 e 10^6 connections
            birthday_bits = 48,
            *args, **kwargs
        )
        
    @asyncio.coroutine
    def init_connection(self, websocket, path):
        ''' Initializes the connection with the client, creating an 
        endpoint/connection object, and registering it with dispatch.
        '''
        connection = yield from super().init_connection(
            websocket = websocket, 
            path = path
        )
            
        print('Connection established with client', str(connection.connid))
        return connection


class WSPersister(_PersisterBase, WSReqResClient):
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
        
    @asyncio.coroutine
    def init_connection(self, websocket, path):
        ''' Initializes the connection with the client, creating an 
        endpoint/connection object, and registering it with dispatch.
        '''
        connection = yield from super().init_connection(
            websocket = websocket, 
            path = path
        )
            
        print('Connection established with persistence server.')
        return connection
        
    def deliver_update_wrapper(self, connection, response_body):
        ''' Handles update pings.
        '''
        # print('-------')
        # print('Receiving delivered subscription update.')
        # print('Sent ghid:', response_body)
        # print('All subscription keys: ')
        # for key in self._subscriptions:
        #     print('    ', str(bytes(key)))
        time.sleep(.01)
        # Shit, I have a race condition somewhere.
        subscribed_ghid = Ghid.from_bytes(response_body[0:65])
        notification_ghid = Ghid.from_bytes(response_body[65:130])
        
        for callback in self._subscriptions[subscribed_ghid]:
            callback(notification_ghid)
            
        # def run_callbacks(subscribed_ghid=subscribed_ghid, notification_ghid=notification_ghid):
        #     for callback in self._subscriptions[subscribed_ghid]:
        #         callback(notification_ghid)
                
        # # Well this is a huge hack. But something about the event loop 
        # # itself is fucking up control flow and causing the world to hang
        # # here. May want to create a dedicated thread just for pushing 
        # # updates? Like autoresponder, but autoupdater?
        # worker = threading.Thread(
        #     target = run_callbacks,
        #     daemon = True,
        # )
        # worker.start()
        
        return b'\x01'
    
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        response = self.send_threadsafe(
            connection = self.connection,
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
            connection = self.connection,
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
            connection = self.connection,
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
                connection = self.connection,
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
                connection = self.connection,
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
            connection = self.connection,
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
            connection = self.connection,
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
            connection = self.connection,
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
            connection = self.connection,
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
            connection = self.connection,
            msg = b'',
            request_code = self.REQUEST_CODES['disconnect']
        )
        
        if response == b'\x01':
            self._subscriptions.clear()
            return True
        else:
            raise RuntimeError('Unknown status code during disconnection.')
        
    @asyncio.coroutine
    def handle_producer_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the producer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            # print(repr(exc))
            # traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_listener_exc(self, connection, exc):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None:
            # print(repr(exc))
            # traceback.print_tb(exc.__traceback__)
            raise exc
        
    @asyncio.coroutine
    def handle_autoresponder_exc(self, exc, token):
        ''' Handles the exception (if any) created by the consumer task.
        
        exc is either:
        1. the exception, if it was raised
        2. None, if no exception was encountered
        '''
        if exc is not None and not isinstance(exc, NakError):
            print(repr(exc))
            traceback.print_tb(exc.__traceback__)
        return repr(exc)
    
    
class LocalhostServer(MemoryPersister):
    ''' Accepts connections over localhost using websockets.
    
    b'AK'       Ack
    b'NK'       Nak
    b'RF'       Response follows
    b'!!'       Subscription notification
    
    b'??'       ping
    b'PB'       publish
    b'GT'       get 
    b'+S'       subscribe
    b'xS'       unsubscribe
    b'LS'       list subs
    b'LB'       list binds
    b'QD'       query debind
    b'QE'       query (existence)
    b'XX'       disconnect
    
    Note that subscriptions are connection-specific. If a connection 
    dies, so do its subscriptions.
    '''
    def __init__(self, port, host='localhost', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ws_host = host
        self._ws_port = port
        self._admin_lock = asyncio.Lock()
        self._shutdown = False
        
    @asyncio.coroutine
    def _request_handler(self, websocket, exchange_lock, dispatch_lookup):
        ''' Handles incoming requests from the websocket in parallel 
        with outgoing subscription updates.
        '''
        rcv = yield from websocket.recv()
            
        # Acquire the exchange lock so we can guarantee an orderly response
        # print('Getting exchange lock for request.')
        yield from exchange_lock
        try:
            framed = memoryview(rcv)
            header = bytes(framed[0:2])
            body = framed[2:]
            
            # This will automatically dispatch the request based on the
            # two-byte header
            response_preheader = dispatch_lookup[header](body)
            # print('-----------------------------------------------')
            print('Successful ', header, ' ', bytes(body[:4]))
            response = self._frame_response(response_preheader)
            # print(bytes(body))
                
        except Exception as e:
            # We should log the exception, but exceptions here should never
            # be allowed to take down a server.
            response_preheader = False
            response_body = (str(e)).encode('utf8')
            response = self._frame_response(
                preheader = response_preheader, 
                body = response_body
            )
            print('Failed to dispatch ', header, ' ', bytes(body[:4]))
            # print(rcv)
            # print(repr(e))
            # traceback.print_tb(e.__traceback__)
            
        finally:
            yield from websocket.send(response)
            exchange_lock.release()
        # print('Released exchange lock for request.')
            
    @asyncio.coroutine
    def _update_handler(self, websocket, exchange_lock, sub_queue):
        ''' Handles updates through a queue into sending out.
        '''
        # This will grab the notification ghid from the queue.
        update = yield from sub_queue.get()
        
        # Immediately acquire the exchange lock when update yields.
        # print('Getting exchange lock for update.')
        yield from exchange_lock
        try:
            # print('Preparing to send sub update.')
            msg_preheader = b'!!'
            msg_body = bytes(update)
            msg = msg_preheader + msg_body
            yield from websocket.send(msg)
            # Note: should this expect a response?
            # print('Sub update sent.')
            
        finally:
            exchange_lock.release()
        # print('Released exchange lock for update.')
    
    @asyncio.coroutine
    def _ws_handler(self, websocket, path):
        ''' Handles a single websocket CONNECTION. Does NOT handle a 
        single websocket exchange.
        '''
        # Note that this overhead happens only once per connection.
        # It's also the easiest way to dispatch connection-specific methods,
        # like list_subs and disconnect.
        
        sub_queue = asyncio.Queue()
        exchange_lock = asyncio.Lock()
        subs = {}
        
        def sub_handler(packed):
            ''' Handles subscriptions for this connection.
            '''
            ghid = Ghid.from_bytes(packed)
            
            def callback(notification_ghid, sub_queue=sub_queue):
                self._loop.create_task(sub_queue.put(notification_ghid))
                
            self.subscribe(ghid, callback)
            subs[ghid] = callback
            return True
        
        def unsub_handler(packed):
            ''' Handles unsubscriptions for this connection. Courtesy of
            self.unsubscribe, will NakError if not currently subscribed
            to the passed ghid.
            '''
            ghid = Ghid.from_bytes(packed)
            
            try:
                callback = subs[ghid]
                del subs[ghid]
            except KeyError as e:
                raise NakError(
                    '0x0006: Invalid or unknown target for unsubscription.'
                ) from e
                
            return struct.pack('>?', self.unsubscribe(ghid, callback))
            
        def conn_list_subs(packed):
            ''' Lists all subscriptions for this connection.
            '''
            parser = generate_ghidlist_parser()
            return parser.pack(list(subs))
        
        def conn_disconnect(packed):
            ''' Clears all subscriptions for this connection.
            '''
            nonlocal subs
            for sub, callback in subs.items():
                self.unsubscribe(sub, callback)
            subs = {}
            return struct.pack('>?', True)
        
        # Note that publish doesn't need to be wrapped as it already handles
        # straight bytes.
        dispatch_lookup = {
            b'??': self.ping_wrapped,
            b'PB': self.publish,
            b'GT': self.get_wrapped,
            b'+S': sub_handler,
            b'xS': unsub_handler,
            b'LS': conn_list_subs,
            b'LB': self.list_bindings_wrapped,
            b'QD': self.list_debinding_wrapped,
            b'QE': self.query_wrapped,
            b'XX': conn_disconnect
        }
        
        # return super().subscribe(ghid, callback)
        # return super().unsubscribe(ghid)
        # return super().list_subs()
        # return super().disconnect()
        
        while True:
            listener_task = asyncio.ensure_future(
                self._request_handler(
                    websocket, 
                    exchange_lock, 
                    dispatch_lookup
                )
            )
            producer_task = asyncio.ensure_future(
                self._update_handler(
                    websocket, 
                    exchange_lock, 
                    sub_queue
                )
            )
            
            done, pending = yield from asyncio.wait(
                [
                    producer_task,
                    listener_task
                ],
                return_when=asyncio.FIRST_COMPLETED)
            
            for task in done:
                self._log_exc(task.exception())

            for task in pending:
                task.cancel()
                    
    def _log_exc(self, exc):
        ''' Handles any potential internal exceptions from tasks during
        a connection. If it's a ConnectionClosed, raise it to break the
        parent loop.
        '''
        if exc is None:
            return
        else:
            print(exc)
            if isinstance(exc, ConnectionClosed):
                raise exc
                
    def _frame_response(self, preheader, body=None):
        ''' Take a result from the _ws_handler and formulate a response.
        '''
        if body is None:
            body = b''
        
        if preheader is True:
            # This should never have a body.
            response = b'AK' + body
            
        elif preheader is False:
            # This will likely have a body.
            response = b'NK' + body
            
        else:
            # This is strange behavior, but emerges from the awkwardly not 
            # identical responses of self.publish vs self.list_bindings etc
            response = b'RF' + bytes(preheader) + body
            
        return response
        
    @asyncio.coroutine
    def catch_interrupt(self):
        ''' Workaround for Windows not passing signals well for doing
        interrupts.
        '''
        while not self._shutdown:
            yield from asyncio.sleep(5)
    
    def run(self):
        ''' Starts a LocalhostPersister server. Runs until the heat 
        death of the universe (or an interrupt is generated somehow).
        '''
        try:
            self._loop = asyncio.get_event_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        port = int(self._ws_port)
        server_task = websockets.serve(self._ws_handler, self._ws_host, port)
        self._intrrptng_cow = asyncio.ensure_future(
            self.catch_interrupt(), 
            loop=self._loop
        )
        
        try:
            self._server_future = self._loop.run_until_complete(server_task)
            _intrrpt_future = self._loop.run_until_complete(self._intrrptng_cow)
            # I don't think this is actually getting called, but the above is 
            # evidently still fixing the windows scheduler bug thig... Odd
            _block_on_result(intrrpt_future)
            
        # Catch and handle errors that fully propagate
        except KeyboardInterrupt as e:
            # Note that this will still call _halt.
            return
            
        # Also call halt on exit.
        finally:
            self._halt()
            
    def _halt(self):
        try:
            self._shutdown = True
            self._intrrptng_cow.cancel()
            # Restart the loop to close down the loop
            self._loop.stop()
            self._loop.run_forever()
        finally:
            self._loop.close()
            
    def ping_wrapped(self, packed):
        ''' Wraps super().ping with packing and unpacking.
        '''
        return struct.pack('>?', self.ping())
    
    def get_wrapped(self, packed):
        ''' Requests an object from the persistence provider, identified
        by its ghid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        ghid = Ghid.from_bytes(packed)
        return self.get(ghid)
    
    def list_bindings_wrapped(self, packed):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        ghid = Ghid.from_bytes(packed)
        ghidlist = self.list_bindings(ghid)
        parser = generate_ghidlist_parser()
        return parser.pack(ghidlist)
    
    def list_debinding_wrapped(self, packed):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise NakError
        '''
        ghid = Ghid.from_bytes(packed)
        
        result = self.list_debinding(ghid)
        
        if not result:
            result = struct.pack('>?', False)
        else:
            result = bytes(result)
        
        return result
        
    def query_wrapped(self, packed):
        ''' Proxies standard query behavior so we don't get internal
        errors from overriding it.
        '''
        ghid = Ghid.from_bytes(packed)
        status = self.query(ghid)
        return struct.pack('>?', status)


class LocalhostClient(MemoryPersister):
    ''' Creates connections over localhost using websockets.
    
    Caches everything locally.
    
    Notes:
    + The only thing I have to listen for is subscription updates.
    + Convert _ws_handler into _subs_listener
    + Everything else can use run_coroutine_threadsafe to add to the pile
    
    '''
    RESPONSE_CODES = {
        b'AK': lambda __: True, 
        b'NK': lambda message: NakError(str(message, encoding='utf8')),
        b'RF': lambda message: message
    }
    
    NOTIFIER_CODE = b'!!'
    
    REQUEST_CODES = {
        'ping':             b'??',
        'publish':          b'PB',
        'get':              b'GT',
        'subscribe':        b'+S',
        'unsubscribe':      b'xS',
        'list_subs':        b'LS',
        'list_bindings':    b'LB',
        'list_debinding': b'QD',
        'query':            b'QE',
        'disconnect':       b'XX',
    }
    
    def __init__(self, port, host='localhost', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ws_loc = 'ws://' + host + ':' + str(port) + '/'
        
        # Set up an event loop and some admin objects
        self._ws_loop = asyncio.new_event_loop()
        # self._ws_loop.set_debug(True)
        self._exchange_lock = asyncio.Lock()
        self._response_box = asyncio.Queue(maxsize=1, loop=self._ws_loop)
        self._admin_lock = threading.Lock()
        # This isn't threadsafe, so maybe it should switch to threading.Event?
        self._init_shutdown = asyncio.Event(loop=self._ws_loop)
        
        # Set up a thread for the websockets client
        self._ws_thread = threading.Thread(
            target = self._ws_robot,
            daemon = True
        )
        self._admin_lock.acquire()
        self._ws_thread.start()
        # asyncio.run_coroutine_threadsafe(self._ws_init(), self._ws_loop)
        
        # Start listening for subscription responses as soon as possible.
        # This also means that the persister is ready for external requests.
        with self._admin_lock:
            # asyncio.run_coroutine_threadsafe(self._ws_listener(), self._ws_loop)
            pass
        
        # Might want to send an initial ping here
        # time.sleep(2)
        # self._future2 = asyncio.run_coroutine_threadsafe(self._pusher(b'Hello guvna!'), self._ws_loop)
        
    def _halt(self):
        # Manually set the admin lock, to make sure we don't have a race 
        # condition with the event loop scheduler.
        self._admin_lock.acquire()
        print('Acquired lock.')
        # Schedule the websocket closure.
        # This will manually release the admin lock when finished. Note: must 
        # be done with a coroutine, and therefore elsewhere
        asyncio.run_coroutine_threadsafe(self._ws_close(), self._ws_loop)
        print('Sent to _ws_close')
        
        # Wait for the admin lock to release in _ws_close
        with self._admin_lock:
            # Stop the loop and set the shutdown flag 
            self._init_shutdown.set()
            print('Set shutdown flag.')
            _block_on_result(self._ws_future)
            self._ws_loop.close()
            print('Closed loop.')
            
    def _ws_robot(self):
        ''' Sets the local event loop and then starts running the 
        listener.
        '''
        asyncio.set_event_loop(self._ws_loop)
        # _ws_future is useful for blocking during halt.
        self._ws_future = asyncio.ensure_future(
            self._ws_listener(), 
            loop = self._ws_loop
        )
        self._ws_loop.run_until_complete(self._ws_future)
        
    @asyncio.coroutine
    def _ws_listener(self):
        self._websocket = yield from websockets.connect(self._ws_loc)
        print('Socket connected.')
        
        # Manually release the lock so that __init__ can proceed.
        self._admin_lock.release()
        
        while not self._init_shutdown.is_set():
            yield from self._ws_handler()
            
        print('Listener successfully shut down.')
        
    @asyncio.coroutine
    def _ws_close(self):
        yield from self._websocket.close()
        # We SHOULD have already set the admin lock, but catch the runtimeerror
        # just in case
        try:
            self._admin_lock.release()
        except RuntimeError:
            pass
            
    @asyncio.coroutine
    def _ws_handler(self):
        ''' This handles a single websocket REQUEST, not an entire 
        connection.
        '''
        listener_task = asyncio.ensure_future(self._websocket.recv())
        interrupt_task = asyncio.ensure_future(self._init_shutdown.wait())
        done, pending = yield from asyncio.wait(
            [
                interrupt_task,
                listener_task
            ],
            return_when=asyncio.FIRST_COMPLETED)

        # These should probably be moved into the actual tasks.

        if interrupt_task in done:
            print('Got shutdown signal.')
            self._ws_loop.stop()
            print('Stopped loop.')
            return
            
        # yield from self._websocket.send(message)
    
        if listener_task in done:
            exc = listener_task.exception()
            if exc:
                self._handle_listener_failure(exc)
            else:
                message = listener_task.result()
                # print('Trying to yield to consumer.')
                yield from self.consumer(message)
                # print('Successfully yielded to consumer.')
        
        for task in pending:
            task.cancel()
            
    def _handle_listener_failure(self, exc):
        if isinstance(exc, ConnectionClosed):
            print('Connection closed by server.')
            raise exc
        else:
            print(exc)
        
    @asyncio.coroutine
    def _pusher(self, data):
        # print('Waiting for exchange lock.')
        yield from self._exchange_lock
        try:
            # print('Exchange lock acquired.')
            yield from self._websocket.send(data)
            # print('Waiting for response.')
            header, body = yield from self._response_box.get()
            
        finally:
            # Hold the exchange lock only and exactly as long as we need it.
            self._exchange_lock.release()
        # print('Exchange lock released.')
            
        response = self.RESPONSE_CODES[header](body)
        
        if isinstance(response, NakError):
            raise response
        elif isinstance(response, Exception):
            # Note: This would be a good place to log the internal error.
            raise NakError('Internal server error.')
        else:
            return response
        
    @asyncio.coroutine
    def consumer(self, message):
        framed = memoryview(message)
        header = bytes(framed[0:2])
        body = framed[2:]
        
        if header in self.RESPONSE_CODES:
            yield from self._response_box.put((header, body))
            
        elif header == self.NOTIFIER_CODE:
            print('Subscription update: ', bytes(body))
            # Note that in this case, body is the ghid we want.
            ghid = Ghid.from_bytes(body)
            
            # I'm not 100% sure why I haven't gotten this working, buuuut...
            # # If it's a dynamic target, bypass the normal get method so that
            # # we don't short-circuit on our own object. See note in self.get.
            # if ghid in self._targets_dynamic:
            #     print('Found in dynamic.')
            #     thread_target = self._pull_from_remote
            # else:
            #     thread_target = self.get
                
            thread_target = self._pull_from_remote
            
            # We can't do this in our thread, because calling _block_on_future
            # in the synchronous code will block the coroutine AND the event 
            # loop scheduler. This is a little gross, but let's spin out a new
            # thread to handle it then.
            t = threading.Thread(
                target = thread_target,
                args = (ghid,),
                daemon = True
            )
            t.start()
            
            # Use put_nowait so that the scheduler doesn't immediately run
            # callbacks on the queue, causing us to reenter the exchange lock
            # yield from self._get_queue.put(ghid)
            # print('Exiting subscription update.')
            # # Note that we cannot call this directly, since we'll block on the
            # # exchange lock.
            # call = functools.partial(self.get, ghid)
            # self._ws_loop.call_soon(call)
            
        else:
            # Invalid code!
            raise RuntimeError('Invalid response code from server.')
            
        return True
        
    def _requestor(self, req_type, req_body):
        ''' Synchronously calls across the thread boundary to make an
        asynchronous request, and then cleverly awaits its completion.
        '''
        # This prevents us from accidentally blocking ourselves later on
        if not self._ws_loop.is_running():
            raise NakError('Internal server error.')
        
        framed = self.REQUEST_CODES[req_type] + req_body
        
        future = asyncio.run_coroutine_threadsafe(
            coro = self._pusher(data=framed), 
            loop = self._ws_loop
        )
        
        # print('Blocking on result of future.')
        # response = _block_on_result(future)
        # # print(response)
        # return response
        return _block_on_result(future)
        # print('Unblocked')
    
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        Note that this is going to (unfortunately) result in packing + 
        unpacking the object twice for ex. a MemoryPersister. At some 
        point, that should be fixed -- maybe through ex. publish_unsafe?
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        self._requestor(
            req_type = 'publish',
            req_body = packed
        )
        super().publish(packed)
        return True
    
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        self._requestor(
            req_type = 'ping',
            req_body = b''
        )
        super().ping()
        return True
    
    def get(self, ghid):
        ''' Requests an object from the persistence provider, identified
        by its ghid. In this case, caches all retrieved objects locally.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        # Check local cache.
        try:
            # Note that this will cause issues if we're looking for a dynamic
            # binding that we already have, but has been updated upstream;
            # we'll immediately short-circuit to the cached one.
            result = super().get(ghid)
            
        # We don't have it in our local cache; check the persistence server.
        except DoesNotExistError:
            # print('Trying to get from server.')
            result = self._pull_from_remote(ghid)
            
        return result
        
    def _pull_from_remote(self, ghid):
        ''' Handles everything needed to get a ghid from the websockets
        persistence server.
        '''
        result = self._requestor(
            req_type = 'get',
            req_body = bytes(ghid)
        )
        
        try:
            # print('Re-publishing to local cache.')
            super().publish(result)
            
        # This is a total hack. Would be much better to properly suppress 
        # returning a recursive update
        except NakError:
            pass
            
        # We don't have a binding for it locally, so get the most convenient
        # from the persistence server.
        except UnboundContainerError:
            binding_list = self.list_bindings(ghid)
            binding = self.get(binding_list[0])
            super().publish(binding)
            super().publish(result)
            
        return result
    
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
        result = self._requestor(
            req_type = 'subscribe',
            req_body = bytes(ghid)
        )
        
        super().subscribe(ghid, callback)
        
        return True
        
    def unsubscribe(self, ghid, callback):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed ghid at the persistence provider.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        result =  self._requestor(
            req_type = 'unsubscribe',
            req_body = bytes(ghid)
        )
        
        super().unsubscribe(ghid, callback)
        
        return True
    
    def list_subs(self):
        ''' List all currently subscribed ghids for the connected 
        client.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        # Ignore super() entirely? Currently we are.
        ghidlist = self._requestor(
            req_type = 'list_subs',
            req_body = b''
        )
        parser = generate_ghidlist_parser()
        return parser.unpack(ghidlist)
    
    def list_bindings(self, ghid):
        ''' Request a list of identities currently binding to the passed
        ghid.
        
        ACK/success is represented by returning a list of ghids.
        NAK/failure is represented by raise NakError
        '''
        # Ignore super() / local entirely? Currently we are.
        packed_ghidlist = self._requestor(
            req_type = 'list_bindings',
            req_body = bytes(ghid)
        )
        parser = generate_ghidlist_parser()
        ghidlist = parser.unpack(packed_ghidlist)
        return ghidlist
    
    def list_debinding(self, ghid):
        ''' Request a the address of any debindings of ghid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GHID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise NakError
        '''
        # Ignore super() entirely? Currently we are.
        packed_ghid = self._requestor(
            req_type = 'list_debinding',
            req_body = bytes(ghid)
        )
        if packed_ghid == struct.pack('>?', False):
            result = False
        else:
            result = Ghid.from_bytes(packed_ghid)
        return result
        
    def query(self, ghid):
        ''' Checks the persistence provider for the existence of the
        passed ghid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise NakError
        '''
        packed_result = self._requestor(
            req_type = 'query',
            req_body = bytes(ghid)
        )
        remote_exists = struct.unpack('>?', packed_result)[0]
        local_exists = super().query(ghid)
        return remote_exists or local_exists
    
    def disconnect(self):
        ''' Terminates all subscriptions and requests. Not required for
        a disconnect, but highly recommended, and prevents an window of
        attack for address spoofers. Note that such an attack would only
        leak metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        self._requestor(
            req_type = 'disconnect',
            req_body = b''
        )
        super().disconnect()
        return True