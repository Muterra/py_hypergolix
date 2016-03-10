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
ERR#4: Invalid target.
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
from .utils import _WeldedSetChainMap


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
        # Forward lookup for containers, simple set of guids.
        self._containers = set()
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
        self._requests = set()
        
        # Reverse lookup for bindings, {<bound Guid>: {<bound by Guid>}}
        self._bindings_static = {}
        # Reverse lookup for dynamic bindings {<bound Guid>: {<bound by Guid>}}
        self._bindings_dynamic = {}
        # Reverse lookup for implicit bindings {<bound Guid>: {<bound Guid>}}
        self._bindings_implicit = {}
        # Reverse lookup for any valid bindings
        self._bindings = _WeldedSetChainMap(
            self._bindings_static,
            self._bindings_dynamic,
            self._bindings_implicit
        )
        # Reverse lookup for debindings, {<debound Guid>: <debound by Guid>}
        self._debindings = {}
        
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
            
        if isinstance(obj, GIDC):
            self._dispatch_gidc(obj)
        elif isinstance(obj, GEOC):
            self._dispatch_geoc(obj)
        elif isinstance(obj, GOBS):
            self._dispatch_gobs(obj)
        elif isinstance(obj, GOBD):
            self._dispatch_gobd(obj)
        elif isinstance(obj, GDXX):
            self._dispatch_gdxx(obj)
        else:
            self._dispatch_garq(obj)
            
        self._publish_unsafe(obj)
        
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
            
        # Note that publishing the object to store is handled upstream.
            
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
            
        self._containers.add(geoc.guid)
            
        # Note that publishing the object to store is handled upstream.
            
    def _dispatch_gobs(self, gobs):
        ''' Does whatever is needed to preprocess a GOBS.
        
        May want to add some target checking to this at some point, 
        though it's impossible to catch everything (race condition) and
        not required by spec
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
        # All checks done, now we're clear to add.
        
        # Update the state of local bindings
        if gobs.target in self._bindings_static:
            # One is a set and the other would be overwritten, so it doesn't
            # much matter if we already have these.
            # Assuming this is an atomic change, we'll always need to do
            # both of these, or neither.
            self._bindings_static[gobs.target].add(gobs.guid)
            self._targets_static[gobs.guid] = gobs.binder, gobs.target
        else:
            # Same note re: atomic changes
            self._bindings_static[gobs.target] = { gobs.guid }
            self._targets_static[gobs.guid] = gobs.binder, gobs.target
            
        # Note that publishing the object to store is handled upstream.
            
    def _dispatch_gobd(self, gobd):
        ''' Does whatever is needed to preprocess a GOBD.
        '''
        self._verify_obj(
            assignee = gobd.binder,
            obj = gobd
        )
        
        # NOTE: add own guid to self._bindings_implicit as preventing own GC.
        # When _debind_dynamic is called, remove that. That way, _gc_check
        # should always work as desired.
        
        # NOTE: this needs to call _gc_check on any updated frames. Should it 
        # also suppress any PersistenceWarnings? Impossible to know if it was
        # intentionally persistent or not.
        
        if gobd.guid in self._debindings:
            raise NakError(
                'ERR#3: Attempt to upload a binding for which a debinding '
                'already exists. Remove the debinding first.'
            )
            
        # Note that publishing the object to store is handled upstream.
            
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
            
        # Handle based on target
        if gdxx.target in self._targets_static:
            self._debind_static(gdxx)
            
        elif gdxx.target in self._targets_dynamic:
            pass
            
        elif gdxx.target in self._targets_debind:
            pass
            
        elif gdxx.target in self._requests:
            pass
            
        else:
            raise NakError(
                'ERR#4: Invalid target for debinding. Debindings must target '
                'static/dynamic bindings, debindings, or asymmetric requests.'
            )
            
        # Debindings can only target one thing, so it doesn't much matter if it
        # already exists (we've already checked for replays)
        self._debindings[gdxx.target] = gdxx.guid
        self._targets_debind[gdxx.guid] = gdxx.debinder, gdxx.target
            
        # Note that publishing the object to store is handled upstream.
        
    def _debind_static(self, gdxx):
        ''' Performs all checks, etc, necessary to release a static 
        binding.
        '''
        # Check that the debinder is the same as the binder
        if gdxx.debinder != self._targets_static[gdxx.target][0]:
            raise NakError(
                'ERR#5: Debinding author is inconsistent with the resource '
                'being debound.'
            )
        
        # We need this before continuing
        binding_target = self._targets_static[gdxx.target][1]
        # We have a consistent author, now remove the original binding.
        # Debindings ALWAYS win, if they've gotten this far.
        self._bindings_static[binding_target].remove(gdxx.target)
        del self._targets_static[gdxx.target]
        del self._store[gdxx.target]
        
        # Garbage collect the binding's target.
        # These are the only two allowable targets for a dynamic binding.
        if binding_target in self._targets_dynamic or \
            binding_target in self._containers:
                self._gc_check(binding_target)
                
        # If it wasn't found, we still appear to have a valid debinding, so 
        # remove the binding anyways, but warn about unexpected behavior
        else:
            raise RuntimeWarning(
                'Successfully removed static binding, but its target was '
                'unknown, or of incorrect type (only GEOC static guids and '
                'GOBD dynamic guids are valid as static binding targets).'
            )
            
    def _debind_garq(self, gdxx):
        ''' Performs all checks, etc, necessary to release a GARQ.
        
        NOTE that currently, if you blindly add to _bindings_implicit, 
        and just remove that and then call GC, there will be problems if
        someone successfully binds to the garq independently, due to the
        inherent race condition if the binding is uploaded before the 
        garq.
        
        THOUGH, we could also add checks for existing bindings into all
        the dispatch functions that don't allow it. That would silently
        remove them, which is probably a better solution than allowing
        them to mess up normal debinding processes.
        '''
        pass
            
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
            
        # Note that publishing the object to store is handled upstream.
        
    def _publish_unsafe(self, obj):
        ''' Adds the object to the internal object store used by the 
        persistence provider. obj should be ex. GEOC, not bytes. 
        Performs NO verification or type checking.
        '''
        if obj.guid not in self._store:
            self._store[obj.guid] = obj.packed
            
        return True
        
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
        if len(self._bindings[guid]) == 0:
            del self._bindings[guid]
            del self._store[guid]
        else:
            raise PersistenceWarning(str(guid) + ' has outstanding bindings.')
            
            
class DiskPersister(_PersisterBase):
    pass