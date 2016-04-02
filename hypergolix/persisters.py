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
ERR#X: Internal error.
ERR#N: Does not appear to be a Golix object.
ERR#0: Failed to verify.
ERR#1: Unknown author or recipient.
ERR#2: Unbound GEOC; immediately garbage collected
ERR#3: Existing debinding for address; (de)binding rejected.
ERR#4: Invalid or unknown target.
ERR#5: Inconsistent author.
ERR#6: Object does not exist at persistence provider.
ERR#7: Attempt to upload illegal frame for dynamic binding. Indicates 
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

import asyncio
import websockets
from websockets.exceptions import ConnectionClosed

from golix import ThirdParty
from golix import SecondParty
from golix import Guid
from golix import Secret
from golix import ParseError
from golix import SecurityError

from golix.utils import generate_guidlist_parser

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

from .utils import _DeepDeleteChainMap
from .utils import _WeldedSetDeepChainMap
from .utils import _block_on_result


class _PersisterBase(metaclass=abc.ABCMeta):
    ''' Base class for persistence providers.
    '''
    def __init__(self):
        super().__init__()
        self._golix_provider = ThirdParty()
    
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
    def get(self, guid):
        ''' Requests an object from the persistence provider, identified
        by its guid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def subscribe(self, guid, callback):
        ''' Request that the persistence provider update the client on
        any changes to the object addressed by guid. Must target either:
        
        1. Dynamic guid
        2. Author identity guid
        
        Upon successful subscription, the persistence provider will 
        publish to client either of the above:
        
        1. New frames to a dynamic binding
        2. Asymmetric requests with the indicated GUID as a recipient
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def unsubscribe(self, guid):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed guid at the persistence provider.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def list_subs(self):
        ''' List all currently subscribed guids for the connected 
        client.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def list_bindings(self, guid):
        ''' Request a list of identities currently binding to the passed
        guid.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def query_debinding(self, guid):
        ''' Request a the address of any debindings of guid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GUID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise NakError
        '''
        pass
        
    @abc.abstractmethod
    def query(self, guid):
        ''' Checks the persistence provider for the existence of the
        passed guid.
        
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
        # Lookup for GIDC authors, {<Guid>: <secondparty>}
        self._id_bases = {}
        # Lookup for dynamic author proxies, {<Guid>: <secondparty>}
        self._id_proxies = {}
        # Lookup for all valid authors, {<Guid>: <secondparty>}
        self._secondparties = _DeepDeleteChainMap(
            self._id_bases,
            self._id_proxies
        )
        # All objects. {<Guid>: <packed object>}
        # Includes proxy reference from dynamic guids.
        self._store = {}
        # Dynamic states. {<Dynamic Guid>: tuple(<current history>)}
        # self._dynamic_states = {}
        
        # Forward lookup for static bindings, 
        # {
        #   <binding Guid>: (<binder Guid>, tuple(<bound Guid>))
        # }
        self._targets_static = {}
        
        # Forward lookup for dynamic bindings, 
        # {
        #   <dynamic guid>: (<binder guid>, tuple(<current history>))
        # }
        self._targets_dynamic = {}
        
        # Forward lookup for debindings, 
        # {
        #   <debinding Guid>: (<debinder Guid>, tuple(<debound Guid>))
        # }
        self._targets_debind = {}
        
        # Forward lookup for asymmetric requests
        # {
        #   <garq Guid>: (<recipient Guid>,)
        # }
        self._requests = {}
        
        # Forward lookup for targeted objects. Note that a guid can only be one 
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
        
        # Reverse lookup for bindings, {<bound Guid>: {<bound by Guid>}}
        self._bindings_static = {}
        # Reverse lookup for dynamic bindings {<bound Guid>: {<bound by Guid>}}
        self._bindings_dynamic = {}
        # Reverse lookup for implicit bindings {<bound Guid>: {<bound Guid>}}
        self._bindings_implicit = {}
        # Reverse lookup for any valid bindings
        self._bindings = _WeldedSetDeepChainMap(
            self._bindings_static,
            self._bindings_dynamic,
            self._bindings_implicit
        )
        # Reverse lookup for debindings, {<debound Guid>: {<debound by Guid>}}
        self._debindings = {}
        # Reverse lookup for everything, same format as other bindings
        self._reverse_references = _WeldedSetDeepChainMap(
            self._bindings_static,
            self._bindings_dynamic,
            self._bindings_implicit,
            self._debindings
        )
        
        # Lookup for subscriptions, {<subscribed Guid>: [callbacks]}
        self._subscriptions = {}
        # Parallel lookup for any subscription notifications that are blocking
        # on an updated target. {<pending target>: <sub guid, notify guid>}
        self._pending_notifications = {}
        
    def publish(self, unpacked):
        ''' Handles publishing for an unpacked object. DOES NOT perform
        guid verification. ONLY USE THIS IF YOU CREATED THE UNPACKED 
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
            raise NakError('ERR#N: Does not appear to be a Golix object.')
        
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
                    'ERR#1: Unknown author / recipient.'
                ) from e
    
        try:
            # This will raise a SecurityError if verification fails.
            self._golix_provider.verify_object(
                second_party = self._secondparties[assignee],
                obj = obj
            )
        except SecurityError as e:
            raise NakError(
                'ERR#0: Failed to verify GEOC.'
            ) from e
            
    def _dispatch_gidc(self, gidc):
        ''' Does whatever is needed to preprocess a GIDC.
        '''
        # Note that GIDC do not require verification beyond unpacking.
        author = gidc.guid
        
        if author not in self._id_bases:
            secondparty = SecondParty.from_identity(gidc)
            self._id_bases[author] = secondparty
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gidc.guid] = gidc
            
    def _dispatch_geoc(self, geoc):
        ''' Does whatever is needed to preprocess a GEOC.
        '''
        self._verify_obj(
            assignee = geoc.author,
            obj = geoc
        )
            
        if geoc.guid not in self._bindings:
            raise UnboundContainerError(
                'ERR#2: Attempt to upload unbound GEOC; object immediately '
                'garbage collected.'
            )
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[geoc.guid] = geoc
        
        # Update any pending notifications and then clean up.
        if geoc.guid in self._pending_notifications:
            self._check_for_subs(
                subscription_guid = self._pending_notifications[geoc.guid][0],
                notification_guid = self._pending_notifications[geoc.guid][1]
            )
            del self._pending_notifications[geoc.guid]
            
    def _dispatch_gobs(self, gobs):
        ''' Does whatever is needed to preprocess a GOBS.
        '''
        self._verify_obj(
            assignee = gobs.binder,
            obj = gobs
        )
        
        if gobs.guid in self._debindings:
            raise NakError(
                'ERR#3: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        # Check for any KNOWN illegal targets, and fail loudly if so
        self._check_illegal_gobs_target(gobs)
        
        # Check to see if someone has already uploaded an illegal binding for 
        # this static binding (due to the race condition in target inspection).
        # If so, force garbage collection.
        self._check_illegal_binding(gobs.guid)
        
        # Update the state of local bindings
        # Assuming this is an atomic change, we'll always need to do
        # both of these, or neither.
        if gobs.target in self._bindings_static:
            self._bindings_static[gobs.target].add(gobs.guid)
        else:
            self._bindings_static[gobs.target] = { gobs.guid }
        # These must, by definition, be identical for any repeated guid, so 
        # it doesn't much matter if we already have them.
        self._targets_static[gobs.guid] = gobs.binder, (gobs.target,)
        self._bindings_implicit[gobs.guid] = { gobs.guid }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gobs.guid] = gobs
            
    def _dispatch_gobd(self, gobd):
        ''' Does whatever is needed to preprocess a GOBD.
        '''
        self._verify_obj(
            assignee = gobd.binder,
            obj = gobd
        )
        
        if gobd.guid_dynamic in self._debindings:
            raise NakError(
                'ERR#3: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        # Check for any KNOWN illegal targets, and fail loudly if so
        self._check_illegal_gobd_target(gobd)
        
        # This needs to be done before updating local bindings, since it does
        # some verification.
        if gobd.guid_dynamic in self._targets_dynamic:
            self._dispatch_updated_dynamic(gobd)
        else:
            self._dispatch_new_dynamic(gobd)
            
        # Status check: we now know we have a legal binding, and that
        # self._targets_dynamic[guid_dynamic] exists, and that any existing
        # **targets** have been removed and GC'd.
        
        old_history = set(self._targets_dynamic[gobd.guid_dynamic][1])
        new_history = set(gobd.history)
        
        # Remove implicit bindings for any expired frames and call GC_check on
        # them.
        # Might want to make this suppress persistence warnings.
        for expired in old_history - new_history:
            # Remove any explicit bindings from the dynamic guid to the frames
            self._bindings_dynamic[expired].remove(gobd.guid_dynamic)
            self._gc_check(expired)
            
        # Update the state of target bindings
        if gobd.target in self._bindings_dynamic:
            self._bindings_dynamic[gobd.target].add(gobd.guid_dynamic)
        else:
            self._bindings_dynamic[gobd.target] = { gobd.guid_dynamic }
        
        # Create a new state.
        self._targets_dynamic[gobd.guid_dynamic] = (
            gobd.binder,
            (gobd.guid,) + tuple(gobd.history) + (gobd.target,)
        )
        self._bindings_dynamic[gobd.guid] = { gobd.guid_dynamic }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gobd.guid] = gobd
        # Overwrite any existing value, or create a new one.
        self._store[gobd.guid_dynamic] = gobd
        
        # Update any subscribers (if they exist) that an updated frame has 
        # been issued. May need to delay until target is uploaded (race cond)
        self._check_for_subs(
            subscription_guid = gobd.guid_dynamic,
            notification_guid = gobd.guid,
            target_guid = gobd.target
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
                'ERR#7: Illegal dynamic frame. Cannot upload a frame with '
                'history as the first frame in a persistence provider.'
            )
            
        # self._dynamic_states[gobd.guid_dynamic] = collections.deque()
        self._targets_dynamic[gobd.guid_dynamic] = (None, tuple())
        self._bindings_implicit[gobd.guid_dynamic] = { gobd.guid_dynamic }
        
    def _dispatch_updated_dynamic(self, gobd):
        ''' Performs validation for a dynamic binding from which the 
        persister has an existing frame. Checks author consistency, 
        historical progression, and calls garbage collection on any 
        expired frames.
        '''
        # Verify consistency of binding author
        if gobd.binder != self._targets_dynamic[gobd.guid_dynamic][0]:
            raise NakError(
                'ERR#5: Dynamic binding author is inconsistent with an '
                'existing dynamic binding at the same address.'
            )
            
        
        # Verify history contains existing most recent frame
        if self._targets_dynamic[gobd.guid_dynamic][1][0] not in gobd.history:
            raise NakError(
                'ERR#7: Illegal dynamic frame. Attempted to upload a new '
                'dynamic frame, but its history did not contain the most '
                'recent frame.'
            )
            
        # This is unnecessary -- it's all handled through self._targets_dynamic
        # in the dispatch_gobd method.
        # # Release hold on previous target. WARNING: currently this means that
        # # updating dynamic bindings is a non-atomic change.
        # old_frame = self._targets_dynamic[gobd.guid_dynamic][1][0]
        # old_target = self._store[old_frame].target
        # # This should always be true, unless something was forcibly GC'd
        # if old_target in self._bindings_dynamic:
        #     self._bindings_dynamic[old_target].remove(gobd.guid_dynamic)
        #     self._gc_check(old_target)
            
    def _dispatch_gdxx(self, gdxx):
        ''' Does whatever is needed to preprocess a GDXX.
        
        Also performs a garbage collection check.
        '''
        self._verify_obj(
            assignee = gdxx.debinder,
            obj = gdxx
        )
        
        if gdxx.guid in self._debindings:
            raise NakError(
                'ERR#3: Attempt to upload a debinding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        if gdxx.target not in self._forward_references:
            raise NakError(
                'ERR#4: Invalid target for debinding. Debindings must target '
                'static/dynamic bindings, debindings, or asymmetric requests. '
                'This may indicate the target does not exist in local storage.'
            )
            
        # Check debinder is consistent with other (de)bindings in the chain
        if gdxx.debinder != self._forward_references[gdxx.target][0]:
            raise NakError(
                'ERR#5: Debinding author is inconsistent with the resource '
                'being debound.'
            )
            
        # Note: if the entire rest of this is not performed atomically, errors
        # will result.
            
        # Must be added to the store before updating subscribers.
        self._store[gdxx.guid] = gdxx
            
        # Update any subscribers (if they exist) that a GDXX has been issued.
        # Must be called before removing bindings, or the subscription record
        # will be removed.
        self._check_for_subs(
            subscription_guid = gdxx.target,
            notification_guid = gdxx.guid
        )
            
        # Debindings can only target one thing, so it doesn't much matter if it
        # already exists (we've already checked for replays)
        self._debindings[gdxx.target] = { gdxx.guid }
        self._targets_debind[gdxx.guid] = gdxx.debinder, (gdxx.target,)
        self._bindings_implicit[gdxx.guid] = { gdxx.guid }
        
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
                    'ERR#1: Unknown author / recipient.'
                ) from e
            
        if garq.guid in self._debindings:
            raise NakError(
                'ERR#3: Attempt to upload a request for which a debinding '
                'already exists. Remove the debinding first.'
            )
        
        # Check to see if someone has already uploaded an illegal binding for 
        # this request (due to the race condition in target inspection).
        # If so, force garbage collection.
        self._check_illegal_binding(garq.guid)
            
        # Update persister state information
        self._requests[garq.guid] = (garq.recipient,)
        self._bindings_implicit[garq.guid] = { garq.guid }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[garq.guid] = garq
        
        # Call the subscribers after adding, in case they request it.
        # Also in case they error out.
        self._check_for_subs(
            subscription_guid = garq.recipient,
            notification_guid = garq.guid
        )
                
    def _check_for_subs(self, subscription_guid, notification_guid, target_guid=None):
        ''' Check for subscriptions, and update them if any exist.
        '''
        if subscription_guid in self._subscriptions:
            # If the target isn't defined, or if it exists, clear to update
            if not target_guid or self.query(target_guid):
                for callback in self._subscriptions[subscription_guid]:
                    callback(notification_guid)
            # Otherwise it's defined and missing; wait until we get the target.
            else:
                self._pending_notifications[target_guid] = \
                    subscription_guid, notification_guid
                
    def _check_illegal_binding(self, guid):
        ''' Checks for an existing binding for guid. If it exists,
        removes the binding, and forces its garbage collection. Used to
        overcome race condition inherent to binding.
        
        Should this warn?
        '''
        # Make sure not to check implicit bindings, or dynamic bindings will
        # show up as illegal if/when we statically bind them
        if guid in self._bindings_static or guid in self._bindings_dynamic:
            illegal_binding = self._bindings[guid]
            del self._bindings[guid]
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
                    'ERR#4: Attempt to bind to an invalid target.'
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
                    'ERR#4: Attempt to bind to an invalid target.'
                )
                
        return True
        
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        return True
        
    def get(self, guid):
        ''' Returns an unpacked Golix object. Only use this if you 100%
        trust the PersistenceProvider to perform all public verification
        of objects.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        try:
            return self._store[guid]
        except KeyError as e:
            raise DoesNotExistError('ERR#6: Guid not found in store.') from e
        
    def subscribe(self, guid, callback):
        ''' Request that the persistence provider update the client on
        any changes to the object addressed by guid. Must target either:
        
        1. Dynamic guid
        2. Author identity guid
        
        Upon successful subscription, the persistence provider will 
        publish to client either of the above:
        
        1. New frames to a dynamic binding
        2. Asymmetric requests with the indicated GUID as a recipient
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''    
        if not callable(callback):
            raise TypeError('callback must be callable.')
            
        if (guid not in self._targets_dynamic and 
            guid not in self._secondparties):
                raise NakError(
                    'ERR#4: Invalid or unknown target for subscription.'
                )
                
        if guid in self._subscriptions:
            self._subscriptions[guid].add(callback)
        else:
            self._subscriptions[guid] = { callback }
            
        return True
        
    def unsubscribe(self, guid):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed guid at the persistence provider.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        if guid in self._subscriptions:
            del self._subscriptions[guid]
        else:
            raise NakError(
                'ERR#4: Invalid or unknown target for unsubscription.'
            )
            
        return True
        
    def list_subs(self):
        ''' List all currently subscribed guids for the connected 
        client.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        return tuple(self._subscriptions)
    
    def list_bindings(self, guid):
        ''' Request a list of identities currently binding to the passed
        guid.
        
        NOTE: does not currently satisfy condition 3 in the spec (aka,
        it is not currently preferentially listing the author binding 
        first).
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        result = []
        # Bindings can only be static XOR dynamic, so we need not worry about
        # set intersections. But note that the lookup dicts contain sets as 
        # keys, so we need to extend, not append.
        if guid in self._bindings_static:
            result.extend(self._bindings_static[guid])
        if guid in self._bindings_dynamic:
            result.extend(self._bindings_dynamic[guid])
        return result
        
    def query_debinding(self, guid):
        ''' Request a the address of any debindings of guid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GUID if it exists
            2. False if it does not exist
        NAK/failure is represented by raise NakError
        '''
        if guid in self._debindings:
            return tuple(self._debindings[guid])[0]
        else:
            return False
            
    def query(self, guid):
        ''' Checks the persistence provider for the existence of the
        passed guid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise NakError
        '''
        if guid in self._store:
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
        
    def _gc_check(self, guid):
        ''' Checks for, and if needed, performs, garbage collection. 
        Only checks the passed guid.
        '''
        # Case 1: not even in the store. Gitouttahere.
        if guid not in self._store:
            return
            
        # Case 2: guid is an identity (GIDC). Basically never delete those.
        elif guid in self._id_bases:
            return
        
        # Case 3: Still bound?
        elif guid in self._bindings:
            # Case 3a: still bound; something else is blocking. Warn.
            if len(self._bindings[guid]) > 0:
                warnings.warn(
                    message = str(guid) + ' has outstanding bindings.',
                    category = PersistenceWarning
                )
            # Case 3b: the binding length is zero; unbound; also remove.
            # This currently only happens when updating dynamic bindings.
            else:
                del self._bindings[guid]
                self._gc_execute(guid)
        # The resource is unbound. Perform GC.
        else:
            self._gc_execute(guid)
                
    def _gc_execute(self, guid):
        ''' Performs garbage collection on guid.
        '''
        # This means it's a binding or debinding
        if guid in self._targets:
            # Clean up the reverse lookup, then remove any empty sets
            for target in self._targets[guid][1]:
                self._reverse_references[target].remove(guid)
                self._reverse_references.remove_empty(target)
                # Perform recursive garbage collection check on the target
                self._gc_check(target)
            
            # Clean up the forward lookup
            del self._targets[guid]
        # It cannot be both a _target and a _request
        elif guid in self._requests:
            del self._requests[guid]
            
        # Clean up any subscriptions.
        if guid in self._subscriptions:
            del self._subscriptions[guid]
            
        # Warn of state problems if still in bindings.
        if guid in self._bindings:
            warnings.warn(
                message = str(guid) + ' has conflicted state. It was removed '
                        'through forced garbage collection, but it still has '
                        'outstanding bindings.'
            )
            
        # And finally, clean up the store.
        del self._store[guid]
        

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
            raise NakError('ERR#N: Does not appear to be a Golix object.') from e
        # We are now guaranteed a Golix object.
        
        return super().publish(obj)
        
    def get(self, guid):
        ''' Requests an object from the persistence provider, identified
        by its guid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        return super().get(guid).packed
        

class DiskPersister(_PersisterBase):
    pass
    
    
class ProxyPersister(_PersisterBase):
    ''' Allows for customized persister implementation using an IPC 
    proxy.
    '''
    pass
    
        
import time
import random
import string
import threading
import traceback
    
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
    def __init__(self, port, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        # This will grab the notification guid from the queue.
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
            guid = Guid.from_bytes(packed)
            
            def callback(notification_guid, sub_queue=sub_queue):
                self._loop.create_task(sub_queue.put(notification_guid))
                
            self.subscribe(guid, callback)
            subs[guid] = callback
            return True
        
        def unsub_handler(packed):
            ''' Handles unsubscriptions for this connection. Courtesy of
            self.unsubscribe, will NakError if not currently subscribed
            to the passed guid.
            '''
            guid = Guid.from_bytes(packed)
            
            try:
                callback = subs[guid]
                del subs[guid]
            except KeyError as e:
                raise NakError(
                    'ERR#4: Invalid or unknown target for unsubscription.'
                ) from e
                
            return struct.pack('>?', self.unsubscribe_wrapped(guid, callback))
            
        def conn_list_subs(packed):
            ''' Lists all subscriptions for this connection.
            '''
            parser = generate_guidlist_parser()
            return parser.pack(list(subs))
        
        def conn_disconnect(packed):
            ''' Clears all subscriptions for this connection.
            '''
            nonlocal subs
            for sub, callback in subs.items():
                self.unsubscribe_wrapped(sub, callback)
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
            b'QD': self.query_debinding_wrapped,
            b'QE': self.query_wrapped,
            b'XX': conn_disconnect
        }
        
        # return super().subscribe(guid, callback)
        # return super().unsubscribe(guid)
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
                
    def unsubscribe_wrapped(self, guid, callback):
        ''' Wraps (actually, overrides) super().unsubscribe behavior to
        only remove the specified callback from self.
        '''
        self._subscriptions[guid].remove(callback)
        return True
                    
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
        server_task = websockets.serve(self._ws_handler, 'localhost', port)
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
        by its guid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        guid = Guid.from_bytes(packed)
        return self.get(guid)
    
    def list_bindings_wrapped(self, packed):
        ''' Request a list of identities currently binding to the passed
        guid.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        guid = Guid.from_bytes(packed)
        guidlist = self.list_bindings(guid)
        parser = generate_guidlist_parser()
        return parser.pack(guidlist)
    
    def query_debinding_wrapped(self, packed):
        ''' Request a the address of any debindings of guid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GUID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise NakError
        '''
        guid = Guid.from_bytes(packed)
        
        result = self.query_debinding(guid)
        
        if not result:
            result = struct.pack('>?', False)
        else:
            result = bytes(result)
        
        return result
        
    def query_wrapped(self, packed):
        ''' Proxies standard query behavior so we don't get internal
        errors from overriding it.
        '''
        guid = Guid.from_bytes(packed)
        status = self.query(guid)
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
        'query_debindings': b'QD',
        'query':            b'QE',
        'disconnect':       b'XX',
    }
    
    def __init__(self, port, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ws_loc = 'ws://localhost:' + str(port) + '/'
        
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
            raise NakError('ERR#X: Internal error.')
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
            # Note that in this case, body is the guid we want.
            guid = Guid.from_bytes(body)
            
            # I'm not 100% sure why I haven't gotten this working, buuuut...
            # # If it's a dynamic target, bypass the normal get method so that
            # # we don't short-circuit on our own object. See note in self.get.
            # if guid in self._targets_dynamic:
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
                args = (guid,),
                daemon = True
            )
            t.start()
            
            # Use put_nowait so that the scheduler doesn't immediately run
            # callbacks on the queue, causing us to reenter the exchange lock
            # yield from self._get_queue.put(guid)
            # print('Exiting subscription update.')
            # # Note that we cannot call this directly, since we'll block on the
            # # exchange lock.
            # call = functools.partial(self.get, guid)
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
            raise NakError('ERR#X: Internal error.')
        
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
    
    def get(self, guid):
        ''' Requests an object from the persistence provider, identified
        by its guid. In this case, caches all retrieved objects locally.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        # Check local cache.
        try:
            # Note that this will cause issues if we're looking for a dynamic
            # binding that we already have, but has been updated upstream;
            # we'll immediately short-circuit to the cached one.
            result = super().get(guid)
            
        # We don't have it in our local cache; check the persistence server.
        except DoesNotExistError:
            # print('Trying to get from server.')
            result = self._pull_from_remote(guid)
            
        return result
        
    def _pull_from_remote(self, guid):
        ''' Handles everything needed to get a guid from the websockets
        persistence server.
        '''
        result = self._requestor(
            req_type = 'get',
            req_body = bytes(guid)
        )
        
        try:
            # print('Re-publishing to local cache.')
            super().publish(result)
            
        # We don't have a binding for it locally, so get the most convenient
        # from the persistence server.
        except UnboundContainerError:
            binding_list = self.list_bindings(guid)
            binding = self.get(binding_list[0])
            super().publish(binding)
            super().publish(result)
            
        return result
    
    def subscribe(self, guid, callback):
        ''' Request that the persistence provider update the client on
        any changes to the object addressed by guid. Must target either:
        
        1. Dynamic guid
        2. Author identity guid
        
        Upon successful subscription, the persistence provider will 
        publish to client either of the above:
        
        1. New frames to a dynamic binding
        2. Asymmetric requests with the indicated GUID as a recipient
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        result = self._requestor(
            req_type = 'subscribe',
            req_body = bytes(guid)
        )
        
        super().subscribe(guid, callback)
        
        return True
        
    def unsubscribe(self, guid):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed guid at the persistence provider.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        result =  self._requestor(
            req_type = 'unsubscribe',
            req_body = bytes(guid)
        )
        
        super().unsubscribe(guid)
        
        return True
    
    def list_subs(self):
        ''' List all currently subscribed guids for the connected 
        client.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        # Ignore super() entirely? Currently we are.
        guidlist = self._requestor(
            req_type = 'list_subs',
            req_body = b''
        )
        parser = generate_guidlist_parser()
        return parser.unpack(guidlist)
    
    def list_bindings(self, guid):
        ''' Request a list of identities currently binding to the passed
        guid.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        # Ignore super() / local entirely? Currently we are.
        packed_guidlist = self._requestor(
            req_type = 'list_bindings',
            req_body = bytes(guid)
        )
        parser = generate_guidlist_parser()
        guidlist = parser.unpack(packed_guidlist)
        return guidlist
    
    def query_debinding(self, guid):
        ''' Request a the address of any debindings of guid, if they
        exist.
        
        ACK/success is represented by returning:
            1. The debinding GUID if it exists
            2. None if it does not exist
        NAK/failure is represented by raise NakError
        '''
        # Ignore super() entirely? Currently we are.
        packed_guid = self._requestor(
            req_type = 'query_debindings',
            req_body = bytes(guid)
        )
        if packed_guid == struct.pack('>?', False):
            result = False
        else:
            result = Guid.from_bytes(packed_guid)
        return result
        
    def query(self, guid):
        ''' Checks the persistence provider for the existence of the
        passed guid.
        
        ACK/success is represented by returning:
            True if it exists
            False otherwise
        NAK/failure is represented by raise NakError
        '''
        packed_result = self._requestor(
            req_type = 'query',
            req_body = bytes(guid)
        )
        remote_exists = struct.unpack('>?', packed_result)[0]
        local_exists = super().query(guid)
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