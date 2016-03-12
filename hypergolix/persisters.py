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
ERR#0: Failed to verify.
ERR#1: Unknown author or recipient.
ERR#2: Unbound GEOC; immediately garbage collected
ERR#3: Existing debinding for address; (de)binding rejected.
ERR#4: Invalid or unknown target.
ERR#5: Inconsistent author.

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

from golix import ThirdParty
from golix import SecondParty
from golix import Guid
from golix import Secret
from golix import ParseError
from golix import SecurityError

from golix._getlow import GIDC
from golix._getlow import GEOC
from golix._getlow import GOBS
from golix._getlow import GOBD
from golix._getlow import GDXX
from golix._getlow import GARQ

# Local dependencies
from .utils import NakError
from .utils import PersistenceWarning
from .utils import _DeepDeleteChainMap
from .utils import _WeldedSetDeepChainMap


class _PersisterBase(metaclass=abc.ABCMeta):
    ''' Base class for persistence providers.
    '''
    def __init__(self):
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
        ''' List all currently subscribed guids.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        pass
    
    @abc.abstractmethod
    def list_binders(self, guid):
        ''' Request a list of identities currently binding to the passed
        guid.
        
        ACK/success is represented by returning a list of guids.
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
        

class MemoryPersister(_PersisterBase):
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
        self._store = {}
        
        # Forward lookup for static bindings, 
        # {
        #   <binding Guid>: (<binder Guid>, <bound Guid>)
        # }
        self._targets_static = {}
        
        # Forward lookup for dynamic bindings, 
        # {
        #   <dynamic guid>: (<binder guid>, (<history>))
        # }
        self._targets_dynamic = {}
        
        # Forward lookup for debindings, 
        # {
        #   <debinding Guid>: (<debinder Guid>, <debound Guid>)
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
        
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        Should MemoryPersister have a publish_unsafe that does not 
        verify the object?
        
        ACK is represented by a return True
        NAK is represented by raise NakError
        '''
        # This will raise if improperly formatted.
        try:
            obj = self._golix_provider.unpack_object(packed)
        except ParseError as e:
            raise TypeError('Packed must be a packed golix object.') from e
        # We are now guaranteed a Golix object.
        
        return self.publish_unsafe(obj)
        
    def publish_unsafe(self, unpacked):
        ''' Handles publishing for an unpacked object. DOES NOT perform
        guid verification. ONLY USE THIS IF YOU CREATED THE UNPACKED 
        OBJECT YOURSELF, or have already performed your own unpacking 
        step. However, will still perform signature verification.
        '''
        if isinstance(unpacked, GIDC):
            self._dispatch_gidc(unpacked)
        elif isinstance(unpacked, GEOC):
            self._dispatch_geoc(unpacked)
        elif isinstance(unpacked, GOBS):
            self._dispatch_gobs(unpacked)
        elif isinstance(unpacked, GOBD):
            self._dispatch_gobd(unpacked)
        elif isinstance(unpacked, GDXX):
            self._dispatch_gdxx(unpacked)
        elif isinstance(unpacked, GARQ):
            self._dispatch_garq(unpacked)
        else:
            raise TypeError('Unpacked must be an unpacked Golix object.')
        
        return True
        
    def _verify_obj(self, assignee, obj):
        ''' Ensures assignee (author/binder/recipient/etc) is known to
        the storage provider and verifies obj.
        '''
        if assignee not in self._secondparties:
            raise NakError(
                'ERR#1: Unknown author / recipient.'
            )
    
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
        self._store[gidc.guid] = gidc.packed
            
    def _dispatch_geoc(self, geoc):
        ''' Does whatever is needed to preprocess a GEOC.
        '''
        self._verify_obj(
            assignee = geoc.author,
            obj = geoc
        )
            
        if geoc.guid not in self._bindings:
            raise NakError(
                'ERR#2: Attempt to upload unbound GEOC; object immediately '
                'garbage collected.'
            )
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[geoc.guid] = geoc.packed
            
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
        if (
            gobs.target in self._targets_static or
            gobs.target in self._targets_debind or
            gobs.target in self._requests or
            gobs.target in self._id_bases
        ):
            raise NakError(
                'ERR#4: Attempt to bind to an invalid target.'
            )
        
        # Check to see if someone has already uploaded an illegal binding for 
        # this static binding (due to the race condition in target inspection).
        # If so, force garbage collection.
        if gobs.guid in self._bindings:
            illegal_binding = self._bindings[gobs.guid]
            del self._bindings[illegal_binding]
            self._gc_execute(illegal_binding)
        
        # Update the state of local bindings
        # Assuming this is an atomic change, we'll always need to do
        # both of these, or neither.
        if gobs.target in self._bindings_static:
            self._bindings_static[gobs.target].add(gobs.guid)
        else:
            self._bindings_static[gobs.target] = { gobs.guid }
        # These must, by definition, be identical for any repeated guid, so 
        # it doesn't much matter if we already have them.
        self._targets_static[gobs.guid] = gobs.binder, gobs.target
        self._bindings_implicit[gobs.guid] = { gobs.guid }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gobs.guid] = gobs.packed
            
    def _dispatch_gobd(self, gobd):
        ''' Does whatever is needed to preprocess a GOBD.
        '''
        self._verify_obj(
            assignee = gobd.binder,
            obj = gobd
        )
        
        if gobd.guid in self._debindings:
            raise NakError(
                'ERR#3: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        # Check for any KNOWN illegal targets, and fail loudly if so
        if gobd.target in self._targets_static or \
            gobd.target in self._requests or \
            gobd.target in self._id_bases:
                raise NakError(
                    'ERR#4: Attempt to bind to an invalid target.'
                )
        
        # Update the state of local bindings
        # Assuming this is an atomic change, we'll always need to do
        # both of these, or neither.
        if gobd.target in self._bindings_dynamic:
            self._bindings_dynamic[gobd.target].add(gobd.guid)
        else:
            self._bindings_dynamic[gobd.target] = { gobd.guid }
        # These must, by definition, be identical for any repeated guid, so 
        # it doesn't much matter if we already have them.
        
        # NOTE: This needs to be fixed to incorporate history. Or does it?
        self._targets_dynamic[gobd.guid] = gobd.binder, gobd.target
        self._bindings_implicit[gobd.guid] = { gobd.guid }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gobd.guid] = gobd.packed
        
    def _dispatch_new_dynamic(self, gobd):
        pass
        
    def _dispatch_updated_dynamic(self, gobd):
        pass
        
        # NOTE: this needs to call _gc_check on any updated frames. Should it 
        # also suppress any PersistenceWarnings? Impossible to know if it was
        # intentionally persistent or not.
            
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
            
        # Update any subscribers (if they exist) that a GDXX has been issued
        self._check_for_subs(
            subscription_guid = gdxx.target,
            notification_guid = gdxx.guid
        )
            
        # Debindings can only target one thing, so it doesn't much matter if it
        # already exists (we've already checked for replays)
        self._debindings[gdxx.target] = { gdxx.guid }
        self._targets_debind[gdxx.guid] = gdxx.debinder, gdxx.target
        self._bindings_implicit[gdxx.guid] = { gdxx.guid }
        
        # Check for (and possibly perform) garbage collect on the target.
        # Cannot blindly call _gc_execute because of dynamic bindings.
        # Note that, to correctly handle implicit bindings, this MUST come
        # after adding the current debinding to the internal store.
        self._gc_check(gdxx.target)
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[gdxx.guid] = gdxx.packed
            
    def _dispatch_garq(self, garq):
        ''' Does whatever is needed to preprocess a GARQ.
        
        Also notifies any subscribers to that recipient address.
        '''
        # Don't call verify, since it would error out, as GARQ are not
        # verifiable by a third party.
        if garq.recipient not in self._secondparties:
            raise NakError(
                'ERR#1: Unknown author / recipient.'
            )
            
        if garq.guid in self._debindings:
            raise NakError(
                'ERR#3: Attempt to upload a request for which a debinding '
                'already exists. Remove the debinding first.'
            )
        
        # Check to see if someone has already uploaded an illegal binding for 
        # this request (due to the race condition in target inspection).
        # If so, force garbage collection.
        if garq.guid in self._bindings:
            illegal_binding = self._bindings[garq.guid]
            del self._bindings[illegal_binding]
            self._gc_execute(illegal_binding)
            
        # Update persister state information
        self._requests[garq.guid] = (garq.recipient,)
        self._bindings_implicit[garq.guid] = { garq.guid }
            
        # It doesn't matter if we already have it, it must be the same.
        self._store[garq.guid] = garq.packed
        
        # Call the subscribers after adding, in case they request it.
        # Also in case they error out.
        self._check_for_subs(
            subscription_guid = garq.recipient,
            notification_guid = garq.guid
        )
                
    def _check_for_subs(self, subscription_guid, notification_guid):
        ''' Check for subscriptions, and update them if any exist.
        '''
        if subscription_guid in self._subscriptions:
            for callback in self._subscriptions[subscription_guid]:
                callback(notification_guid)
        
    def ping(self):
        ''' Queries the persistence provider for availability.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
        
    def get(self, guid):
        ''' Requests an object from the persistence provider, identified
        by its guid.
        
        ACK/success is represented by returning the object
        NAK/failure is represented by raise NakError
        '''
        pass
        
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
        
    def unsubscribe(self, guid):
        ''' Unsubscribe. Client must have an existing subscription to 
        the passed guid at the persistence provider.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
        
    def list_subs(self):
        ''' List all currently subscribed guids.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        pass
    
    def list_binders(self, guid):
        ''' Request a list of identities currently binding to the passed
        guid.
        
        ACK/success is represented by returning a list of guids.
        NAK/failure is represented by raise NakError
        '''
        pass
        
    def disconnect(self):
        ''' Terminates all subscriptions and requests. Not required for
        a disconnect, but highly recommended, and prevents an window of
        attack for address spoofers. Note that such an attack would only
        leak metadata.
        
        ACK/success is represented by a return True
        NAK/failure is represented by raise NakError
        '''
        pass
    
    def _gc_orphan_bindings(self):
        ''' Removes any orphaned (target does not exist) dynamic or 
        static bindings.
        '''
        pass
        
    def _gc_check(self, guid):
        ''' Checks for, and if needed, performs, garbage collection. 
        Only checks the passed guid.
        '''
        # First make sure it's actually in the store.
        if guid not in self._store:
            return
            
        # It exists? Preprocess GC check.
        # Case 1: guid is an identity (GIDC). Basically never delete those.
        if guid in self._id_bases:
            return
            
        # Case 2: guid has an implicit binding. GOBS, GOBD, GDXX, GARQ.
        # Check it for an explicit debinding; remove if appropriate.
        if guid in self._bindings_implicit and guid in self._debindings:
            del self._bindings_implicit[guid]
        
        # Case 3: guid has explicit binding (GEOC). No preprocess necessary.
        
        # Now check for bindings. If still bound, something else is blocking.
        if guid in self._bindings:
            warnings.warn(
                message = str(guid) + ' has outstanding bindings.',
                category = PersistenceWarning
            )
        # The resource is unbound. Perform GC.
        else:
            self._gc_execute(guid)
                
    def _gc_execute(self, guid):
        ''' Performs garbage collection on guid.
        '''
        # This means it's a binding or debinding
        if guid in self._targets:
            target = self._targets[guid][1]
            # Clean up the forward lookup
            del self._targets[guid]
            # Clean up the reverse lookup, then remove any empty sets
            self._reverse_references[target].remove(guid)
            self._reverse_references.remove_empty(target)
            
            # Perform a recursive garbage collection check on the target
            self._gc_check(target)
            
        # And add a wee bit of special processing for requests.
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
        

class DiskPersister(_PersisterBase):
    pass