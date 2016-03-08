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

# Control * imports.
__all__ = [
    'MemoryPersister', 
    'DiskPersister'
]

# Global dependencies
import abc
import collections

from golix import ThirdParty
from golix import Guid
from golix import Secret

from golix._getlow import GIDC
from golix._getlow import GEOC
from golix._getlow import GOBS
from golix._getlow import GOBD
from golix._getlow import GDXX
from golix._getlow import GARQ


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
    def subscribe(self, guid):
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
        self._id_bases = {}
        self._id_proxies = {}
        self._secondparties = collections.ChainMap(
            self._id_bases,
            self._id_proxies
        )
        self._store = {}
        
    def publish(self, packed):
        ''' Submits a packed object to the persister.
        
        Should MemoryPersister have a publish_unsafe that does not 
        verify the object?
        
        ACK is represented by a return True
        NAK is represented by raise NakError
        '''
        # This will raise if improperly formatted.
        obj = self._golix_provider.unpack_object(packed)
        # We are now guaranteed a Golix object.
            
        self._handle_publish(obj)
        self._update_store(obj)
        
        return True
        
    def _update_store(self, obj):
        ''' Adds the object to the internal object store used by the 
        persistence provider. obj should be ex. GEOC, not bytes.
        '''
        if obj.geoc not in self._store:
            self._store[obj.geoc] = obj.packed
            
    def _dispatch_object(self, obj):
        ''' Calls the individual dispatch functions depending on the 
        golix type of obj.
        
        Note that the _dispatch_<object> methods must raise NakError 
        internally on failure, or publish will add them to self._store.
        
        Note that the objects are, as of this point, unverified, as they
        require public keys for that to be possible.
        '''
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
        elif isinstance(obj, GARQ):
            self._dispatch_garq(obj)
        else:
            raise TypeError('Object must be an unpacked golix object.')
            
    def _dispatch_gidc(self, gidc):
        ''' Does whatever is needed to preprocess a GIDC.
        '''
        author = gidc.geoc
        secondparty = SecondParty.from_identity(gidc)
        if author not in self._id_bases:
            self._id_bases[author] = secondparty
            
    def _dispatch_geoc(self, geoc):
        ''' Does whatever is needed to preprocess a GEOC.
        '''
        if geoc.author not in self._secondparties:
            raise NakError(
                'ERR#1: Memory persisters cannot add GEOCs whose authors are '
                'unknown to the memory persister.'
            )
    
        try:
            # This will raise a SecurityError if verification fails.
            self._golix_provider.verify_object(
                second_party = self._secondparties[geoc.author]
                obj = geoc
            )
        except SecurityError as e:
            raise NakError('ERR#0: Failed to verify GEOC.') from e
            
            
class DiskPersister(_PersisterBase):
    pass