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

# External dependencies
import logging
import collections
import weakref
import queue
import threading
import traceback
import asyncio
import loopa

# Internal dependencies
from .persistence import _GidcLite
from .persistence import _GeocLite
from .persistence import _GobsLite
from .persistence import _GobdLite
from .persistence import _GdxxLite
from .persistence import _GarqLite

from .utils import SetMap
from .utils import WeakSetMap
from .utils import weak_property

from .gao import GAO

from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api
from .hypothetical import fixture_noop

from .exceptions import HypergolixException
from .exceptions import RemoteNak
from .exceptions import MalformedGolixPrimitive
from .exceptions import VerificationFailure
from .exceptions import UnboundContainer
from .exceptions import InvalidIdentity
from .exceptions import DoesNotExist
from .exceptions import AlreadyDebound
from .exceptions import InvalidTarget
from .exceptions import StillBoundWarning
from .exceptions import RequestError
from .exceptions import InconsistentAuthor
from .exceptions import IllegalDynamicFrame
from .exceptions import IntegrityError
from .exceptions import UnavailableUpstream


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports.
__all__ = [
    # 'PersistenceCore',
]


# ###############################################
# Lib
# ###############################################
            
            
class BookieCore(metaclass=API):
    ''' Tracks state relationships between objects using **only weak
    references** to them. ONLY CONCERNED WITH LIFETIMES! Does not check
    (for example) consistent authorship.
    '''
    _lawyer = weak_property('__lawyer')
    _librarian = weak_property('__librarian')
    _undertaker = weak_property('__undertaker')
    
    def __init__(self):
        # Lookup for debindings flagged as illegal. So long as the local state
        # is successfully propagated upstream, this can be a local-only object.
        self._illegal_debindings = set()
        
        # Lookup <bound ghid>: set(<binding obj>)
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        self._bound_by_ghid = SetMap()
        
        # Lookup <debound ghid>: <debinding ghid>
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        # Note that any particular object can have exactly zero or one VALID
        # debinds, but that a malicious actor could find a race condition and
        # debind something FOR SOMEONE ELSE before the bookie knows about the
        # original object authorship.
        self._debound_by_ghid = SetMap()
        self._debound_by_ghid_staged = SetMap()
        
        # Lookup <recipient>: set(<request ghid>)
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        self._requests_for_recipient = SetMap()
        
    def assemble(self, librarian, lawyer, undertaker):
        # Call before using.
        self._lawyer = lawyer
        self._librarian = librarian
        # We need to be able to initiate GC on illegal debindings detected
        # after the fact.
        self._undertaker = undertaker
        
    def recipient_status(self, ghid):
        ''' Return a frozenset of ghids assigned to the passed ghid as
        a recipient.
        '''
        return self._requests_for_recipient.get_any(ghid)
        
    def bind_status(self, ghid):
        ''' Return a frozenset of ghids binding the passed ghid.
        '''
        return self._bound_by_ghid.get_any(ghid)
        
    def debind_status(self, ghid):
        ''' Return either a ghid, or None.
        '''
        total = set()
        total.update(self._debound_by_ghid.get_any(ghid))
        total.update(self._debound_by_ghid_staged.get_any(ghid))
        return total
        
    def is_illegal(self, obj):
        ''' Check to see if this is an illegal debinding.
        '''
        return obj.ghid in self._illegal_debindings
        
    def is_bound(self, obj):
        ''' Check to see if the object has been bound.
        '''
        return obj.ghid in self._bound_by_ghid
            
    def is_debound(self, obj):
        # Well we have an object and a debinding, so now let's validate them.
        # NOTE: this needs to be converted to also check for debinding validity
        for debinding_ghid in self._debound_by_ghid_staged.get_any(obj.ghid):
            # Get the existing debinding
            debinding = self._librarian.summarize(debinding_ghid)
            
            # Validate existing binding against newly-known target
            try:
                self._lawyer.validate_gdxx(debinding, target_obj=obj)
                
            # Validation failed. Remove illegal debinding.
            except Exception:
                logger.warning(
                    'Removed invalid existing binding. \n'
                    '    Illegal debinding author: ' + str(debinding.author) +
                    '    Valid object author:      ' + str(obj.author)
                )
                self._illegal_debindings.add(debinding.ghid)
                self._undertaker.triage(debinding.ghid)
            
            # It's valid, so move it out of staging.
            else:
                self._debound_by_ghid_staged.discard(obj.ghid, debinding.ghid)
                self._debound_by_ghid.add(obj.ghid, debinding.ghid)
            
        # Now we can just easily check to see if it's debound_by_ghid.
        return obj.ghid in self._debound_by_ghid
        
    def _add_binding(self, being_bound, doing_binding):
        # Exactly what it sounds like. Should remove this stub to reduce the
        # number of function calls.
        self._bound_by_ghid.add(being_bound, doing_binding)
            
    def _remove_binding(self, obj):
        being_unbound = obj.target
        
        try:
            self._bound_by_ghid.remove(being_unbound, obj.ghid)
        except KeyError:
            logger.warning(
                'Attempting to remove a binding, but the bookie has no record '
                'of its existence.'
            )
            
    def _remove_request(self, obj):
        recipient = obj.recipient
        self._requests_for_recipient.discard(recipient, obj.ghid)
            
    def _remove_debinding(self, obj):
        target = obj.target
        self._illegal_debindings.discard(obj.ghid)
        self._debound_by_ghid.discard(target, obj.ghid)
        self._debound_by_ghid_staged.discard(target, obj.ghid)
        
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
            existing = self._librarian.summarize(obj.ghid)
        except KeyError:
            if obj.history:
                logger.warning(
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
        if obj.target in self._librarian:
            self._debound_by_ghid.add(obj.target, obj.ghid)
        else:
            self._debound_by_ghid_staged.add(obj.target, obj.ghid)
        
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
        self._requests_for_recipient.add(obj.recipient, obj.ghid)
        
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
