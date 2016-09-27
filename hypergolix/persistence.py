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

RemoteNak status code conventions:
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


# Global dependencies
import abc
import collections
import warnings
import functools
import struct
import weakref
import queue
import pathlib
import base64

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

from .utils import _DeepDeleteChainMap
from .utils import _WeldedSetDeepChainMap
from .utils import _block_on_result
from .utils import _JitSetDict
from .utils import TruthyLock
from .utils import SetMap
from .utils import WeakSetMap
from .utils import _generate_threadnames


# ###############################################
# Boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    'PersistenceCore',
]


# ###############################################
# Library
# ###############################################
                
        
class PersistenceCore:
    ''' Provides the core functions for storing Golix objects. Required
    for the hypergolix service to start.
    
    Can coordinate with both "upstream" and "downstream" persisters.
    Other persisters should pass through the "ingestive tract". Local
    objects can be published directly through calling the ingest_<type>
    methods.
    
    TODO: add librarian validation, so that attempting to update an
    object we already have an identical copy to silently exits.
    '''
    
    def __init__(self):
        # REALLY not crazy about this being an RLock, but lazy loading in
        # librarian is causing problems with anything else.
        self._opslock = threading.RLock()
        
        self.doorman = None
        self.enlitener = None
        self.enforcer = None
        self.lawyer = None
        self.bookie = None
        self.postman = None
        self.undertaker = None
        self.librarian = None
        self.salmonator = None
        
    def assemble(self, doorman, enforcer, lawyer, bookie, librarian, postman,
                 undertaker, salmonator):
        self.doorman = weakref.proxy(doorman)
        self.enlitener = Enlitener
        self.enforcer = weakref.proxy(enforcer)
        self.lawyer = weakref.proxy(lawyer)
        self.bookie = weakref.proxy(bookie)
        self.postman = weakref.proxy(postman)
        # This breaks all context managers, unfortunately
        # self.undertaker = weakref.proxy(undertaker)
        self.undertaker = undertaker
        self.librarian = weakref.proxy(librarian)
        self.salmonator = weakref.proxy(salmonator)
        
    def ingest(self, packed, remotable=True):
        ''' Called on an untrusted and unknown object. May be bypassed
        by locally-created, trusted objects (by calling the individual
        ingest methods directly). Parses, validates, and stores the
        object, and returns True; or, raises an error.
        '''
        for loader, ingester in (
        (self.doorman.load_gidc, self.ingest_gidc),
        (self.doorman.load_geoc, self.ingest_geoc),
        (self.doorman.load_gobs, self.ingest_gobs),
        (self.doorman.load_gobd, self.ingest_gobd),
        (self.doorman.load_gdxx, self.ingest_gdxx),
        (self.doorman.load_garq, self.ingest_garq)):
            # Attempt this loader
            try:
                golix_obj = loader(packed)
            # This loader failed. Continue to the next.
            except MalformedGolixPrimitive:
                continue
            # This loader succeeded. Ingest it and then break out of the loop.
            else:
                obj = ingester(golix_obj, remotable)
                break
        # Running into the else means we could not find a loader.
        else:
            raise MalformedGolixPrimitive(
                '0x0001: Packed bytes do not appear to be a Golix primitive.'
            )
                    
        # If the object is identical to what we already have, the ingester will
        # return None, so don't schedule that.
        if obj is not None:
            # Note that individual ingest methods are only called directly for 
            # locally-built objects, which do not need a mail run.
            self.postman.schedule(obj)
        else:
            logger.debug('Object unchanged; postman scheduling not required.')
            
            # Note: this is not the place for salmonator pushing! Locally 
            # created/updated objects call the individual ingest methods 
            # directly, so they have to be the ones that actually deal with
                    
        return obj
        
    def ingest_gidc(self, obj, remotable=True):
        raw = obj.packed
        obj = self.enlitener._convert_gidc(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # Validate to make sure that we don't already have an identical
            # object. If so, short-circuit immediately.
            if not self.librarian.validate_gidc(obj):
                return None
                
            # First need to enforce target selection
            self.enforcer.validate_gidc(obj)
            # Now make sure authorship requirements are satisfied
            self.lawyer.validate_gidc(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self.undertaker:
                # Finally make sure persistence rules are followed
                self.bookie.validate_gidc(obj)
        
            # Force GC pass after every mutation
            with self.undertaker:
                # And now prep the undertaker for any necessary GC
                self.undertaker.prep_gidc(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self.bookie.place_gidc(obj)
                # And finally add it to the librarian
                self.librarian.store(obj, raw)
        
        # TODO: push this to a delayed callback within an event loop...
        if remotable:
            self.salmonator.push(obj.ghid)
        
        return obj
        
    def ingest_geoc(self, obj, remotable=True):
        raw = obj.packed
        obj = self.enlitener._convert_geoc(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # Validate to make sure that we don't already have an identical
            # object. If so, short-circuit immediately.
            if not self.librarian.validate_geoc(obj):
                return None
                
            # First need to enforce target selection
            self.enforcer.validate_geoc(obj)
            # Now make sure authorship requirements are satisfied
            self.lawyer.validate_geoc(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self.undertaker:
                # Finally make sure persistence rules are followed
                self.bookie.validate_geoc(obj)
        
            # Force GC pass after every mutation
            with self.undertaker:
                # And now prep the undertaker for any necessary GC
                self.undertaker.prep_geoc(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self.bookie.place_geoc(obj)
                # And finally add it to the librarian
                self.librarian.store(obj, raw)
        
        # TODO: push this to a delayed callback within an event loop...
        if remotable:
            self.salmonator.push(obj.ghid)
        
        return obj
        
    def ingest_gobs(self, obj, remotable=True):
        raw = obj.packed
        obj = self.enlitener._convert_gobs(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # Validate to make sure that we don't already have an identical
            # object. If so, short-circuit immediately.
            if not self.librarian.validate_gobs(obj):
                return None
                
            # First need to enforce target selection
            self.enforcer.validate_gobs(obj)
            # Now make sure authorship requirements are satisfied
            self.lawyer.validate_gobs(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self.undertaker:
                # Finally make sure persistence rules are followed
                self.bookie.validate_gobs(obj)
        
            # Force GC pass after every mutation
            with self.undertaker:
                # And now prep the undertaker for any necessary GC
                self.undertaker.prep_gobs(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self.bookie.place_gobs(obj)
                # And finally add it to the librarian
                self.librarian.store(obj, raw)
        
        # TODO: push this to a delayed callback within an event loop...
        if remotable:
            self.salmonator.push(obj.ghid)
        
        return obj
        
    def ingest_gobd(self, obj, remotable=True):
        raw = obj.packed
        obj = self.enlitener._convert_gobd(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # Validate to make sure that we don't already have an identical
            # object. If so, short-circuit immediately.
            if not self.librarian.validate_gobd(obj):
                return None
                
            # First need to enforce target selection
            self.enforcer.validate_gobd(obj)
            # Now make sure authorship requirements are satisfied
            self.lawyer.validate_gobd(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self.undertaker:
                # Finally make sure persistence rules are followed
                self.bookie.validate_gobd(obj)
        
            # Force GC pass after every mutation
            with self.undertaker:
                # And now prep the undertaker for any necessary GC
                self.undertaker.prep_gobd(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self.bookie.place_gobd(obj)
                # And finally add it to the librarian
                self.librarian.store(obj, raw)
        
        # TODO: push this to a delayed callback within an event loop...
        if remotable:
            self.salmonator.push(obj.ghid)
        
        return obj
        
    def ingest_gdxx(self, obj, remotable=True):
        raw = obj.packed
        obj = self.enlitener._convert_gdxx(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # Validate to make sure that we don't already have an identical
            # object. If so, short-circuit immediately.
            if not self.librarian.validate_gdxx(obj):
                return None
                
            # First need to enforce target selection
            self.enforcer.validate_gdxx(obj)
            # Now make sure authorship requirements are satisfied
            self.lawyer.validate_gdxx(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self.undertaker:
                # Finally make sure persistence rules are followed
                self.bookie.validate_gdxx(obj)
        
            # Force GC pass after every mutation
            with self.undertaker:
                # And now prep the undertaker for any necessary GC
                self.undertaker.prep_gdxx(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self.bookie.place_gdxx(obj)
                # And finally add it to the librarian
                self.librarian.store(obj, raw)
        
        # TODO: push this to a delayed callback within an event loop...
        if remotable:
            self.salmonator.push(obj.ghid)
        
        return obj
        
    def ingest_garq(self, obj, remotable=True):
        raw = obj.packed
        obj = self.enlitener._convert_garq(obj)
        
        # Take the lock first, since this will mutate state
        with self._opslock:
            # Validate to make sure that we don't already have an identical
            # object. If so, short-circuit immediately.
            if not self.librarian.validate_garq(obj):
                return None
                
            # First need to enforce target selection
            self.enforcer.validate_garq(obj)
            # Now make sure authorship requirements are satisfied
            self.lawyer.validate_garq(obj)
            
            # Add GC pass in case of illegal existing debindings.
            with self.undertaker:
                # Finally make sure persistence rules are followed
                self.bookie.validate_garq(obj)
        
            # Force GC pass after every mutation
            with self.undertaker:
                # And now prep the undertaker for any necessary GC
                self.undertaker.prep_garq(obj)
                # Everything is validated. Place with the bookie first, so that 
                # it has access to the old librarian state
                self.bookie.place_garq(obj)
                # And finally add it to the librarian
                self.librarian.store(obj, raw)
        
        # TODO: push this to a delayed callback within an event loop...
        if remotable:
            self.salmonator.push(obj.ghid)
        
        return obj
        
        
class Doorman:
    ''' Parses files and enforces crypto. Can be bypassed for trusted 
    (aka locally-created) objects. Only called from within the typeless
    PersisterCore.ingest() method.
    '''
    def __init__(self):
        self._librarian = None
        self._golix = ThirdParty()
        
    def assemble(self, librarian):
        # Called to link to the librarian.
        self._librarian = weakref.proxy(librarian)
        
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
            author = self._librarian.summarize(obj.author)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient: ' + str(obj.author)
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
            author = self._librarian.summarize(obj.binder)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient: ' + str(obj.binder)
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
            author = self._librarian.summarize(obj.binder)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient: ' + str(obj.binder)
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
            author = self._librarian.summarize(obj.debinder)
        except KeyError as exc:
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient: ' + 
                str(obj.debinder)
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
        
        
class Enlitener:
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
        
        
class Enforcer:
    ''' Enforces valid target selections.
    '''
    def __init__(self):
        self._librarian = None
        
    def assemble(self, librarian):
        # Call before using.
        self._librarian = weakref.proxy(librarian)
        
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
            target = self._librarian.summarize(obj.target)
        # TODO: think more about this, and whether everything has been updated
        # appropriately to raise a DoesNotExist instead of a KeyError.
        # This could be more specific and say DoesNotExist
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
            target = self._librarian.summarize(obj.target)
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
        
    def validate_gdxx(self, obj, target_obj=None):
        ''' Check if target is known, and if it is, validate it.
        '''
        try:
            if target_obj is None:
                target = self._librarian.summarize(obj.target)
            else:
                target = target_obj
        except KeyError:
            logger.warning(
                'GDXX was validated by Enforcer, but its target was unknown '
                'to the librarian. May indicated targeted attack.\n'
                '    GDXX ghid:   ' + str(obj.ghid) + '\n'
                '    Target ghid: ' + str(obj.target)
            )
            # raise InvalidTarget(
            #     '0x0006: Unknown debinding target. Cannot debind an unknown '
            #     'resource, to prevent a malicious party from preemptively '
            #     'uploading a debinding for a resource s/he did not bind.'
            # )
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
        
        NOTE: the "zeroth binding must not have history" requirement has
        been relaxed, since it will be superseded in the next version of
        the golix protocol, and it causes SERIOUS problems with the 
        operational flow of, like, literally everything.
        '''
        # Try getting an existing binding.
        try:
            existing = self._librarian.summarize(obj.ghid)
            
        except KeyError:
            # if obj.history:
            #     raise IllegalDynamicFrame(
            #         '0x0009: Illegal frame. Cannot upload a frame with '
            #         'history as the first frame in a persistence provider.'
            #     )
            pass
                
        else:
            if existing.frame_ghid not in obj.history:
                logger.debug('New obj frame:     ' + str(obj.frame_ghid))
                logger.debug('New obj hist:      ' + str(obj.history))
                logger.debug('Existing frame:    ' + str(existing.frame_ghid))
                logger.debug('Existing hist:     ' + str(existing.history))
                raise IllegalDynamicFrame(
                    '0x0009: Illegal frame. Frame history did not contain the '
                    'most recent frame.'
                )
        
        
class Lawyer:
    ''' Enforces authorship requirements, including both having a known
    entity as author/recipient and consistency for eg. bindings and 
    debindings.
    
    Threadsafe.
    '''
    def __init__(self):
        # Lookup for all known identity ghids
        # This must remain valid at all persister instances regardless of the
        # python runtime state
        self._librarian = None
        
    def assemble(self, librarian):
        # Call before using.
        self._librarian = weakref.proxy(librarian)
        
    def _validate_author(self, obj):
        try:
            author = self._librarian.summarize(obj.author)
        except KeyError as exc:
            logger.info('0x0003: Unknown author / recipient.')
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient: ' + str(obj.author)
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
            existing = self._librarian.summarize(obj.ghid)
        except KeyError:
            pass
        else:
            if existing.author != obj.author:
                logger.info(
                    '0x0007: Inconsistent binding author. \n'
                    '    Existing author:  ' + str(existing.author) +
                    '\n    Attempted author: ' + str(obj.author)
                )
                raise InconsistentAuthor(
                    '0x0007: Inconsistent binding author. \n'
                    '    Existing author:  ' + str(existing.author) +
                    '\n    Attempted author: ' + str(obj.author)
                )
        return True
        
    def validate_gdxx(self, obj, target_obj=None):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding.
        
        If other is not None, specifically checks it against that object
        instead of obtaining it from librarian.
        '''
        self._validate_author(obj)
        try:
            if target_obj is None:
                existing = self._librarian.summarize(obj.target)
            else:
                existing = target_obj
                
        except KeyError:
            pass
            
        else:
            if isinstance(existing, _GarqLite):
                if existing.recipient != obj.author:
                    logger.info(
                        '0x0007: Inconsistent debinding author. \n'
                        '    Existing recipient:  ' + str(existing.recipient) +
                        '\n    Attempted debinder: ' + str(obj.author)
                    )
                    raise InconsistentAuthor(
                        '0x0007: Inconsistent debinding author. \n'
                        '    Existing recipient:  ' + str(existing.recipient) +
                        '\n    Attempted debinder: ' + str(obj.author)
                    )
                
            else:
                if existing.author != obj.author:
                    logger.info(
                        '0x0007: Inconsistent debinding author. \n'
                        '    Existing binder:   ' + str(existing.author) +
                        '\n    Attempted debinder: ' + str(obj.author)
                    )
                    raise InconsistentAuthor(
                        '0x0007: Inconsistent debinding author. \n'
                        '    Existing binder:   ' + str(existing.author) +
                        '\n    Attempted debinder: ' + str(obj.author)
                    )
        return True
        
    def validate_garq(self, obj):
        ''' Validate recipient.
        '''
        try:
            recipient = self._librarian.summarize(obj.recipient)
        except KeyError as exc:
            logger.info(
                '0x0003: Unknown author / recipient: ' + str(obj.recipient)
            )
            raise InvalidIdentity(
                '0x0003: Unknown author / recipient: ' + str(obj.recipient)
            ) from exc
        else:
            if not isinstance(recipient, _GidcLite):
                logger.info('0x0003: Invalid author / recipient.')
                raise InvalidIdentity(
                    '0x0003: Invalid author / recipient.'
                )
                
        return True
            
            
class Bookie:
    ''' Tracks state relationships between objects using **only weak
    references** to them. ONLY CONCERNED WITH LIFETIMES! Does not check
    (for example) consistent authorship.
    
    (Not currently) threadsafe.
    '''
    def __init__(self):
        self._opslock = threading.Lock()
        self._undertaker = None
        self._librarian = None
        self._lawyer = None
        
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
        self._librarian = weakref.proxy(librarian)
        self._lawyer = weakref.proxy(lawyer)
        # We need to be able to initiate GC on illegal debindings detected 
        # after the fact.
        self._undertaker = weakref.proxy(undertaker)
        
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
            except:
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
            
            
class _LibrarianCore(metaclass=abc.ABCMeta):
    ''' Base class for caching systems common to non-volatile librarians
    such as DiskLibrarian, S3Librarian, etc.
    
    TODO: make ghid vs frame ghid usage more consistent across things.
    '''
    def __init__(self):
        # Link to core, which will be assigned after __init__
        self._percore = None
        
        # Operations and restoration lock
        self._restoring = TruthyLock()
        
        # Lookup for dynamic ghid -> frame ghid
        # Must be consistent across all concurrently connected librarians
        self._dyn_resolver = {}
        
        # Lookup for ghid -> hypergolix description
        # This may be GC'd by the python process.
        self._catalog = {}
        self._opslock = threading.Lock()
        
    def assemble(self, persistence_core):
        # Creates a weakref proxy to core.
        # This is needed for lazy loading and restoring.
        self._percore = weakref.proxy(persistence_core)
        
    def force_gc(self, obj):
        ''' Forces erasure of an object. Does not notify the undertaker.
        Indempotent. Should never raise KeyError.
        '''
        with self._restoring.mutex:
            try:
                ghid = self._ghid_resolver(obj.ghid)
                self.remove_from_cache(ghid)
            except:
                logger.warning(
                    'Exception while removing from cache during object GC. '
                    'Probably a bug.\n' + ''.join(traceback.format_exc())
                )
            
            try:
                del self._catalog[ghid]
            except KeyError:
                pass
                
            if isinstance(obj, _GobdLite):
                del self._dyn_resolver[obj.ghid]
        
    def store(self, obj, data):
        ''' Starts tracking an object.
        obj is a hypergolix representation object.
        raw is bytes-like.
        '''  
        with self._restoring.mutex:
            # We need to do some resolver work if it's a dynamic object.
            if isinstance(obj, _GobdLite):
                reference = obj.frame_ghid
            else:
                reference = obj.ghid
            
            # Only add to cache if we are not restoring from it.
            if not self._restoring:
                self.add_to_cache(reference, data)
                
            self._catalog[reference] = obj
            
            # Finally, only if successful should we update
            if isinstance(obj, _GobdLite):
                # Remove any existing frame.
                if obj.ghid in self._dyn_resolver:
                    old_ghid = self._ghid_resolver(obj.ghid)
                    self.remove_from_cache(old_ghid)
                    # Remove any existing _catalog entry
                    self._catalog.pop(old_ghid, None)
                # Update new frame.
                self._dyn_resolver[obj.ghid] = obj.frame_ghid
                
    def _ghid_resolver(self, ghid):
        ''' Convert a dynamic ghid into a frame ghid, or return the ghid
        immediately if not dynamic.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('Ghid must be a Ghid.')
            
        if ghid in self._dyn_resolver:
            return self._dyn_resolver[ghid]
        else:
            return ghid
            
    def retrieve(self, ghid):
        ''' Returns the raw data associated with the ghid, checking only
        locally.
        '''
        with self._restoring.mutex:
            ghid = self._ghid_resolver(ghid)
            return self.get_from_cache(ghid)
        
    def summarize(self, ghid):
        ''' Returns a lightweight Hypergolix description of the object.
        Checks only locally.
        '''
        ghid = self._ghid_resolver(ghid)
        
        # No need to block if it exists, especially if we're restoring.
        try:
            return self._catalog[ghid]
            
        except KeyError as exc:
            # Put this inside the except block so that we don't have to do any
            # weird fiddling to make restoration work.
            with self._restoring.mutex:
                # Bypass lazy-load if restoring and re-raise
                if self._restoring:
                    raise DoesNotExist() from exc
                else:
                    # Lazy-load a new one if possible.
                    self._lazy_load(ghid, exc)
                    return self._catalog[ghid]
                
    def _lazy_load(self, ghid, exc):
        ''' Does a lazy load restore of the ghid.
        '''
        if self._percore is None:
            raise RuntimeError(
                'Core must be linked to lazy-load from cache.'
            ) from exc
            
        # This will raise if missing. Connect the new_exc to the old one tho.
        try:
            data = self.get_from_cache(ghid)
            
            # I guess we might as well validate on every lazy load.
            with self._restoring:
                self._percore.ingest(data)
                
        except Exception as new_exc:
            raise new_exc from exc
        
    def __contains__(self, ghid):
        ghid = self._ghid_resolver(ghid)
        # Catalog may only be accurate locally. Shelf is accurate globally.
        return self.check_in_cache(ghid)
    
    def restore(self):
        ''' Loads any existing files from the cache.  All existing
        files there will be attempted to be loaded, so it's best not to
        have extraneous stuff in the directory. Will be passed through
        to the core for processing.
        '''
        # Suppress all warnings during restoration.
        logger.setLevel(logging.ERROR)
        try:
            if self._percore is None:
                raise RuntimeError(
                    'Cannot restore a librarian\'s cache without first ' +
                    'linking to its corresponding core.'
                )
            
            # This prevents us from wasting time rewriting existing entries in
            # the cache.
            with self._restoring:
                gidcs = []
                geocs = []
                gobss = []
                gobds = []
                gdxxs = []
                garqs = []
                
                # This will mutate the lists in-place.
                for candidate in self.walk_cache():
                    self._attempt_load_inplace(
                        candidate, gidcs, geocs, gobss, gobds, gdxxs, garqs
                    )
                    
                # Okay yes, unfortunately this will result in unpacking all of
                # the files twice. However, we need to verify the crypto.
                
                # First load all identities, so that we have authors for
                # everything
                for gidc in gidcs:
                    self._percore.ingest(gidc.packed)
                    # self._percore.ingest_gidc(gidc)
                    
                # Now all debindings, so that we can check state while we're at
                # it
                for gdxx in gdxxs:
                    self._percore.ingest(gdxx.packed)
                    # self._percore.ingest_gdxx(gdxx)
                    
                # Now all bindings, so that objects aren't gc'd. Note: can't
                # combine into single list, because of different ingest methods
                for gobs in gobss:
                    self._percore.ingest(gobs.packed)
                    # self._percore.ingest_gobs(gobs)
                for gobd in gobds:
                    self._percore.ingest(gobd.packed)
                    # self._percore.ingest_gobd(gobd)
                    
                # Next the objects themselves, so that any requests will have
                # their targets available (not that it would matter yet,
                # buuuuut)...
                for geoc in geocs:
                    self._percore.ingest(geoc.packed)
                    # self._percore.ingest_geoc(geoc)
                    
                # Last but not least
                for garq in garqs:
                    self._percore.ingest(garq.packed)
                    # self._percore.ingest_garq(garq)
                
        # Restore the logging level to notset
        finally:
            logger.setLevel(logging.NOTSET)
                
    def _attempt_load_inplace(self, candidate, gidcs, geocs, gobss, gobds, 
                            gdxxs, garqs):
        ''' Attempts to do an inplace addition to the passed lists based
        on the loading.
        '''
        for loader, target in ((GIDC.unpack, gidcs),
                                (GEOC.unpack, geocs),
                                (GOBS.unpack, gobss),
                                (GOBD.unpack, gobds),
                                (GDXX.unpack, gdxxs),
                                (GARQ.unpack, garqs)):
            # Attempt this loader
            try:
                golix_obj = loader(candidate)
            # This loader failed. Continue to the next.
            except ParseError:
                continue
            # This loader succeeded. Ingest it and then break out of the loop.
            else:
                obj = target.append(golix_obj)
                break
                
        # HOWEVER, unlike usual, don't raise if this isn't a correct object,
        # just don't bother adding it either.
        
    def validate_gidc(self, obj):
        ''' GIDC need no validation.
        '''
        if not self._restoring:
            if obj.ghid in self:
                return None
        return True
        
    def validate_geoc(self, obj):
        ''' Ensure author is known and valid.
        '''
        if not self._restoring:
            if obj.ghid in self:
                return None
        return True
        
    def validate_gobs(self, obj):
        ''' Ensure author is known and valid.
        '''
        if not self._restoring:
            if obj.ghid in self:
                return None
        return True
        
    def validate_gobd(self, obj):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding (if it already exists).
        '''
        # NOTE THE CHANGE OF FLOW HERE! We check the frame ghid instead of the
        # standard ghid.
        if not self._restoring:
            if obj.frame_ghid in self:
                return None
        return True
        
    def validate_gdxx(self, obj, target_obj=None):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding.
        
        If other is not None, specifically checks it against that object
        instead of obtaining it from librarian.
        '''
        if not self._restoring:
            if obj.ghid in self:
                return None
        return True
        
    def validate_garq(self, obj):
        ''' Validate recipient.
        '''
        if not self._restoring:
            if obj.ghid in self:
                return None
        return True
            
    @abc.abstractmethod
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        pass
        
    @abc.abstractmethod
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the 
        cache.
        '''
        pass
        
    @abc.abstractmethod
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        pass
        
    @abc.abstractmethod
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        pass
        
    @abc.abstractmethod
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        pass
    
    
class DiskLibrarian(_LibrarianCore):
    ''' Librarian that caches stuff to disk.
    '''
    def __init__(self, cache_dir):
        ''' cache_dir should be relative to current.
        '''
        cache_dir = pathlib.Path(cache_dir)
        if not cache_dir.exists():
            raise ValueError(
                'Path does not exist: ' + cache_dir.as_posix()
            )
        if not cache_dir.is_dir():
            raise ValueError(
                'Path is not an available directory: ' + cache_dir.as_posix()
            )
        
        self._cachedir = cache_dir
        super().__init__()
        
    def _make_path(self, ghid):
        ''' Converts the ghid to a file path.
        '''
        fname = ghid.as_str() + '.ghid'
        fpath = self._cachedir / fname
        return fpath
        
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        for child in self._cachedir.iterdir():
            if child.is_file():
                yield child.read_bytes()
            
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        fpath = self._make_path(ghid)
        fpath.write_bytes(data)
        
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the 
        cache.
        '''
        fpath = self._make_path(ghid)
        try:
            fpath.unlink()
        except FileNotFoundError as exc:
            raise DoesNotExist(
                'Ghid does not exist at persister: ' + str(ghid)
            ) from exc
        
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        fpath = self._make_path(ghid)
        try:
            return fpath.read_bytes()
        except FileNotFoundError as exc:
            raise DoesNotExist(
                'Ghid does not exist at persister: ' + str(ghid)
            ) from exc
        
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        fpath = self._make_path(ghid)
        return fpath.exists()
        
        
class MemoryLibrarian(_LibrarianCore):
    def __init__(self):
        self._shelf = {}
        super().__init__()
        
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        pass
            
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        self._shelf[ghid] = data
        
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the 
        cache.
        '''
        del self._shelf[ghid]
        
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        return self._shelf[ghid]
        
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        return ghid in self._shelf
            

_MrPostcard = collections.namedtuple(
    typename = '_MrPostcard',
    field_names = ('subscription', 'notification'),
)

            
class _PostmanBase(metaclass=abc.ABCMeta):
    ''' Tracks, delivers notifications about objects using **only weak
    references** to them. Threadsafe.
    
    ♫ Please Mister Postman...
    
    Question: should the distributed state management of GARQ recipients
    be managed here, or in the bookie (where it currently is)?
    '''
    def __init__(self):
        self._bookie = None
        self._librarian = None
        
        self._out_for_delivery = threading.Event()
        
        # The scheduling queue
        self._scheduled = queue.Queue()
        # The delayed lookup. <awaiting ghid>: set(<subscribed ghids>)
        self._opslock_defer = threading.Lock()
        self._deferred = SetMap()
        
    def assemble(self, librarian, bookie):
        # Links the librarian and bookie.
        self._librarian = weakref.proxy(librarian)
        self._bookie = weakref.proxy(bookie)
        
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
            debinding_ghids = self._bookie.debind_status(obj.ghid)
            if not debinding_ghids:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            for debinding_ghid in debinding_ghids:
                self._scheduled.put(
                    _MrPostcard(obj.ghid, debinding_ghid)
                )
        else:
            notifier = _MrPostcard(obj.ghid, obj.frame_ghid)
            if obj.target not in self._librarian:
                self._defer_update(
                    awaiting_ghid = obj.target,
                    postcard = notifier,
                )
            else:
                self._scheduled.put(notifier)
        
    def _schedule_gdxx(self, obj, removed):
        # GDXX will never directly trigger a subscription. If they are removing
        # a subscribed object, the actual removal (in the undertaker GC) will 
        # trigger a subscription without us.
        pass
        
    def _schedule_garq(self, obj, removed):
        # GARQ might trigger a subscription! Or we might be removing it.
        if removed:
            debinding_ghids = self._bookie.debind_status(obj.ghid)
            if not debinding_ghids:
                raise RuntimeError(
                    'Obj flagged removed, but bookie lacks debinding for it.'
                )
            for debinding_ghid in debinding_ghids:
                self._scheduled.put(
                    _MrPostcard(obj.recipient, debinding_ghid)
                )
        else:
            self._scheduled.put(
                _MrPostcard(obj.recipient, obj.ghid)
            )
            
    def _defer_update(self, awaiting_ghid, postcard):
        ''' Defer a subscription notification until the awaiting_ghid is
        received as well.
        '''
        # Note that deferred updates will always be dynamic bindings, so the
        # subscribed ghid will be identical to the notification ghid.
        with self._opslock_defer:
            self._deferred.add(awaiting_ghid, postcard)
        logger.debug('Postman update deferred for ' + str(awaiting_ghid))
            
    def _has_deferred(self, obj):
        ''' Checks to see if a subscription is waiting on the obj, and 
        if so, returns the originally subscribed ghid.
        '''
        with self._opslock_defer:
            return self._deferred.pop_any(obj.ghid)
            
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
            logger.debug(
                'Postman out for delivery on ' + 
                str(self._scheduled.qsize()) + ' (or more) items. '
                'Additionally, ' + str(len(self._deferred)) + ' missing ghids '
                'are blocking updates for other subscriptions.'
            )
            
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
                # We can't spin this out into a thread because some of our 
                # delivery mechanisms want this to have an event loop.
                self._deliver(subscription, notification)
            
    @abc.abstractmethod
    def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        # We need to freeze the listeners before we operate on them, but we 
        # don't need to lock them while we go through all of the callbacks.
        # Instead, just sacrifice any subs being added concurrently to the 
        # current delivery run.
        pass


class MrPostman(_PostmanBase):
    ''' Postman to use for local persistence systems.
    
    Note that MrPostman doesn't need to worry about silencing updates,
    because the persistence ingestion tract will only result in a mail
    run if there's a new object there. So, by definition, any re-sent
    objects will be DOA.
    '''
    def __init__(self):
        super().__init__()
        self._rolodex = None
        self._golcore = None
        
        # self._listeners = SetMap()
        # NOTE! This means that the listeners CANNOT be methods, as methods
        # will be DOA.
        self._listeners = WeakSetMap()
        
    def assemble(self, golix_core, librarian, bookie, rolodex):
        super().assemble(librarian, bookie)
        self._golcore = weakref.proxy(golix_core)
        self._rolodex = weakref.proxy(rolodex)
        
    def register(self, gao):
        ''' Registers a GAO with the postman, so that it will receive
        any updates from upstream/downstream remotes. By using the handy
        WeakSetMap and gao._weak_touch, we can ensure that python GCing
        the GAO will also result in removal of the listener.
        
        Theoretically, we should only ever have one registered GAO for
        a given ghid at the same time, so maybe in the future there's
        some optimization to be had there.
        '''
        self._listeners.add(gao.ghid, gao._weak_touch)
            
    def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        if subscription == self._golcore.whoami:
            self._rolodex.notification_handler(subscription, notification)
        else:
            callbacks = self._listeners.get_any(subscription)
            logger.debug(
                'MrPostman starting delivery for ' + str(len(callbacks)) + 
                ' updates on sub ' + str(subscription) + ' with notif ' + 
                str(notification)
            )
            for callback in callbacks:
                callback(subscription, notification)
        
        
class PostOffice(_PostmanBase):
    ''' Postman to use for remote persistence servers.
    '''
    def __init__(self):
        super().__init__()
        # By using WeakSetMap we can automatically handle dropped connections
        # Lookup <subscribed ghid>: set(<subscribed callbacks>)
        self._opslock_listen = threading.Lock()
        self._listeners = WeakSetMap()
        
    def subscribe(self, ghid, callback):
        ''' Tells the postman that the watching_session would like to be
        updated about ghid.
        '''
        # First add the subscription listeners
        with self._opslock_listen:
            self._listeners.add(ghid, callback)
            
        # Now manually reinstate any desired notifications for garq requests
        # that have yet to be handled
        for existing_mail in self._bookie.recipient_status(ghid):
            obj = self._librarian.summarize(existing_mail)
            self.schedule(obj)
            
    def unsubscribe(self, ghid, callback):
        ''' Remove the callback for ghid. Indempotent; will never raise
        a keyerror.
        '''
        self._listeners.discard(ghid, callback)
            
    def _deliver(self, subscription, notification):
        ''' Do the actual subscription update.
        '''
        # We need to freeze the listeners before we operate on them, but we 
        # don't need to lock them while we go through all of the callbacks.
        # Instead, just sacrifice any subs being added concurrently to the 
        # current delivery run.
        callbacks = self._listeners.get_any(subscription)
        postcard = _MrPostcard(subscription, notification)
                
        for callback in callbacks:
            callback(*postcard)
        
        
class Undertaker:
    ''' Note: what about post-facto removal of bindings that have 
    illegal targets? For example, if someone uploads a binding for a 
    target that isn't currently known, and then it turns out that the
    target, once uploaded, actually doesn't support that binding, what
    should we do?
    
    In theory it shouldn't affect other operations. Should we just bill
    for it and call it a day? We'd need to make some kind of call to the
    bookie to handle that.
    '''
    def __init__(self):
        self._librarian = None
        self._bookie = None
        self._postman = None
        
        # This, if defined, handles removal of secrets when objects are debound
        self._psychopomp = None
        
        self._staging = None
        
    def assemble(self, librarian, bookie, postman, psychopomp=None):
        # Call before using.
        self._librarian = weakref.proxy(librarian)
        self._bookie = weakref.proxy(bookie)
        self._postman = weakref.proxy(postman)
        
        if psychopomp is not None:
            self._psychopomp = weakref.proxy(psychopomp)
        
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
                obj = self._librarian.summarize(ghid)
                
            except KeyError:
                logger.warning(
                    'Attempt to GC an object not found in librarian.'
                )
            
            else:
                for primitive, gcollector in ((_GidcLite, self._gc_gidc),
                                            (_GeocLite, self._gc_geoc),
                                            (_GobsLite, self._gc_gobs),
                                            (_GobdLite, self._gc_gobd),
                                            (_GdxxLite, self._gc_gdxx),
                                            (_GarqLite, self._gc_garq)):
                    if isinstance(obj, primitive):
                        gcollector(obj)
                        break
                else:
                    # No appropriate GCer found (should we typerror?); so 
                    # continue with WHILE loop
                    continue
                    logger.error('No appropriate GC routine found!')
            
        self._staging = None
        
    def triage(self, ghid):
        ''' Schedule GC check for object.
        
        Note: should triaging be order-dependent?
        '''
        logger.debug('Performing triage.')
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
        logger.debug('Entering gobs GC.')
        if self._bookie.is_debound(obj):
            logger.debug('Gobs is debound. Staging target and executing GC.')
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
        if self._bookie.is_debound(obj) or self._bookie.is_illegal(obj):
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
        # Finally, if we have a psychopomp (secrets remover), notify her, too
        if self._psychopomp is not None:
            # Protect this with a thread to prevent reentrancy
            worker = threading.Thread(
                target = self._psychopomp.schedule,
                daemon = True,
                args = (obj,),
                name = _generate_threadnames('styxwrkr')[0],
            )
            worker.start()
        
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
        try:
            existing = self._librarian.summarize(obj.ghid)
        except KeyError:
            # This will always happen if it's the first frame, so let's be sure
            # to ignore that for logging.
            if obj.history:
                logger.warning('Could not find gobd to check existing target.')
        else:
            self.triage(existing.target)
            
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
        
        
class UnderReaper(Undertaker):
    ''' An undertaker that also propagates garbage collection of 
    container objects into abandonment of their secrets, by way of the
    inquisition.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._privateer = None
    
    def assemble(self, librarian, bookie, postman, privateer, *args, **kwargs):
        super().assemble(librarian, bookie, postman, *args, **kwargs)
        self._privateer = weakref.proxy(privateer)
        
    def _gc_execute(self, obj):
        super()._gc_execute(obj)
        # To the reaper!
        if obj.ghid in self._privateer:
            self._privateer.abandon(obj.ghid)


class Salmonator:
    ''' Responsible for disseminating Golix objects upstream and 
    downstream. Handles all comms with them as well.
    
    # TODO: (f)reebase and separate into components for both local and
    # server use
    '''
    def __init__(self):
        ''' Yarp.
        '''
        self._opslock = threading.Lock()
        
        self._percore = None
        self._golcore = None
        self._postman = None
        self._librarian = None
        self._doorman = None
        
        self._upstream_remotes = set()
        self._downstream_remotes = set()
        
        # WVD lookup for <registered ghid>: GAO.
        self._registered = weakref.WeakValueDictionary()
        
    def assemble(self, golix_core, persistence_core, doorman, postman, 
                librarian):
        self._golcore = weakref.proxy(golix_core)
        self._percore = weakref.proxy(persistence_core)
        self._postman = weakref.proxy(postman)
        self._librarian = weakref.proxy(librarian)
    
    def add_upstream_remote(self, persister):
        ''' Adds an upstream persister.
        
        PersistenceCore will attempt to have a constantly consistent 
        state with upstream persisters. That means that any local 
        resources will either be subscribed to upstream, or checked for
        updates before ingestion by the Hypergolix service.
        
        HOWEVER, the Salmonator will make no attempt to synchronize 
        state **between** upstream remotes.
        '''
        # Before we do anything, we should try pushing up our identity.
        persister.publish(self._golcore._identity.second_party.packed)
        
        with self._opslock:
            self._upstream_remotes.add(persister)
            # Subscribe to our identity, assuming we actually have a golix core
            try:
                persister.subscribe(self._golcore.whoami, 
                                    self._remote_callback)
            except AttributeError:
                pass
                
            # Subscribe to every active GAO's ghid
            for registrant in self._registered:
                persister.subscribe(registrant, self._remote_callback)
        
    def remove_upstream_remote(self, persister):
        ''' Inverse of above.
        '''
        with self._opslock:
            self._upstream_remotes.remove(persister)
            # Remove all subscriptions
            persister.disconnect()
        
    def add_downstream_remote(self, persister):
        ''' Adds a downstream persister.
        
        PersistenceCore will not attempt to keep a consistent state with
        downstream persisters. Instead, it will simply push updates to
        local objects downstream. It will not, however, look to them for
        updates.
        
        Therefore, to create synchronization **between** multiple 
        upstream remotes, also add them as downstream remotes.
        '''
        raise NotImplementedError()
        self._downstream_remotes.add(persister)
        
    def remove_downstream_remote(self, persister):
        ''' Inverse of above.
        '''
        raise NotImplementedError()
        self._downstream_remotes.remove(persister)
        
    def _remote_callback(self, subscription, notification):
        ''' Callback to use when subscribing to things at remotes.
        '''
        logger.debug('Hitting remote callback.')
        self.pull(notification)
        
    def push(self, ghid):
        ''' Grabs the ghid from librarian and sends it to all applicable
        remotes.
        '''
        data = self._librarian.retrieve(ghid)
        # This is, again, definitely a lame way of doing this
        for remote in self._upstream_remotes:
            remote.publish(data)
        
    def pull(self, ghid, quiet=False):
        ''' Grabs the ghid from remotes, if available, and puts it into
        the ingestion pipeline.
        '''
        # TODO: check locally, run _inspect, check if mutable before blindly
        # pulling.
        # This is the lame way of doing this, fo sho
        for remote in self._upstream_remotes:
            # Pull, and then see if we got anything back
            obj = self._attempt_pull_single(ghid, remote)
            
            # If we got a gobd, make sure its target is in the librarian
            if isinstance(obj, _GobdLite):
                if obj.target not in self._librarian:
                    # Catch unavailableupstream and log a warning.
                    # TODO: add logic to retry a few times and then discard
                    try:
                        self.pull(obj.target)
                        
                    except UnavailableUpstream:
                        logger.warning(
                            'Received a subscription notification for ' +
                            str(ghid) + ', but the sub\'s target was missing '
                            'both locally and upstream.'
                        )
                        
                self._postman.do_mail_run()
                break
            
            # For all other successful pulls, just go ahead and do a mail run.
            elif obj:
                self._postman.do_mail_run()
                break
                
        else:
            if not quiet:
                raise UnavailableUpstream(
                    'Object was unavailable or unacceptable at all '
                    'currently-registered remotes.'
                )
            else:
                logger.info(
                    'Object was unavailable or unacceptable upstream, but '
                    'pull was called quietly: ' + str(ghid)
                )
        
    def _attempt_pull_single(self, ghid, remote):
        ''' Once we've successfully acquired data from
        '''
        # Try to get it from the remote.
        try:
            data = remote.get(ghid)
            
        # Unsuccessful pull. Log the error and return False.
        except:
            logger.warning('Error while pulling from upstream: \n' + 
                            ''.join(traceback.format_exc()))
            return False
        
        # Only if we actually successfully got something back should we
        # continue on to ingest.
        else:
            # This may or may not be an update we already have.
            try:
                # Call as remotable=False to avoid infinite loops.
                obj = self._percore.ingest(data, remotable=False)
            
            # Couldn't load. Return False.
            except:
                logger.warning(
                    'Error while pulling from upstream: \n' + 
                    ''.join(traceback.format_exc())
                )
                return False
                
            # As soon as we have it, return True so parent can stop checking 
            # other remotes.
            else:
                # Note that ingest can either return None, if we already have
                # the object, or the object itself, if it's new.
                if obj is None:
                    return True
                else:
                    return obj
        
    def _inspect(self, ghid):
        ''' Checks librarian for an existing ghid. If it has it, checks
        the object's integrity by re-parsing it. If it is dynamic, also
        queries upstream remotes for newer versions.
        
        returns None if no local copy exists
        returns True if local copy exists and is valid
        raises IntegrityError if local copy exists, but is corrupted.
        ~~(returns False if dynamic and local copy is out of date)~~
            Note: this is unimplemented currently, blocking on several 
            upstream changes.
        '''
        # Load the object locally
        try:
            obj = self._librarian.summarize(ghid)
        
        # Librarian has no copy.    
        except KeyError:
            return None
            
        # The only object that can mutate is a Gobd
        # This is blocking on updates to the remote persistence spec, which is
        # in turn blocking on changes to the Golix spec.
        # if isinstance(obj, _GobdLite):
        #     self._check_for_updates(obj)
        
        self._verify_existing(obj)
        return True
            
    def _check_for_updates(self, obj):
        ''' Checks with all upstream remotes for new versions of a 
        dynamic object.
        '''
        # Oh wait, we can't actually do this, because the remote persistence
        # protocol doesn't support querying what the current binding frame is
        # without just loading the whole binding.
        # When the Golix spec changes to semi-stateless dynamic bindings, using
        # a counter for validating monotonicity instead of a hash chain, then
        # the remote persistence spec should be expanded to include a query_ctr
        # command for ultra-lightweight checks. For now, we're stuck with ~1kB
        # dynamic bindings.
        raise NotImplementedError()
            
    def _verify_existing(self, obj):
        ''' Re-loads an object to make sure it's still good.
        Obj should be a lightweight hypergolix representation, not the
        packed Golix object, unpacked Golix object, nor the GAO.
        '''
        packed = self._librarian.retrieve(obj.ghid)
        
        for primitive, loader in ((_GidcLite, self._doorman.load_gidc),
                                (_GeocLite, self._doorman.load_geoc),
                                (_GobsLite, self._doorman.load_gobs),
                                (_GobdLite, self._doorman.load_gobd),
                                (_GdxxLite, self._doorman.load_gdxx),
                                (_GarqLite, self._doorman.load_garq)):
            # Attempt this loader
            if isinstance(obj, primitive):
                try:
                    loader(packed)
                except (MalformedGolixPrimitive, SecurityError) as exc:
                    logger.error('Integrity of local object appears '
                                'compromised.')
                    raise IntegrityError('Local copy of object appears to be '
                                        'corrupt or compromised.') from exc
                else:
                    break
                    
        # If we didn't find a loader, typeerror.
        else:
            raise TypeError('Invalid object type while verifying object.')
            
    def register(self, gao, skip_refresh=False):
        ''' Tells the Salmonator to listen upstream for any updates
        while the gao is retained in memory.
        '''
        with self._opslock:
            self._registered[gao.ghid] = gao
        
            if gao.dynamic:
                for remote in self._upstream_remotes:
                    try:
                        remote.subscribe(gao.ghid, self._remote_callback)
                    except:
                        logger.warning(
                            'Exception while subscribing to upstream updates '
                            'for GAO at ' + str(gao.ghid) + '\n' +
                            ''.join(traceback.format_exc())
                        )
                    else:
                        logger.debug(
                            'Successfully subscribed to upstream updates for '
                            'GAO at ' + str(gao.ghid)
                        )
                        
            # Add deregister as a finalizer, but don't call it atexit.
            finalizer = weakref.finalize(gao, self.deregister, gao.ghid)
            finalizer.atexit = False
            
        # This should also catch any upstream deletes.
        if not skip_refresh:
            self.pull(gao.ghid, quiet=True)
                
    def deregister(self, ghid):
        ''' Tells the salmonator to stop listening for upstream 
        object updates. Primarily intended for use as a finalizer
        for GAO objects.
        '''
        with self._opslock:
            try:
                # We shouldn't really actually need to do this, but let's
                # explicitly do it anyways, to ensure there is no race 
                # condition between object removal and someone else sending an 
                # update. The weak-value-ness of the _registered lookup can be 
                # used as a fallback.
                del self._registered[ghid]
            except KeyError:
                pass
            
            for remote in self._upstream_remotes:
                try:
                    remote.unsubscribe(ghid, self._remote_callback)
                except:
                    logger.warning(
                        'Exception while unsubscribing from upstream updates '
                        'during GAO cleanup for ' + str(ghid) + '\n' +
                        ''.join(traceback.format_exc())
                    )
                
                
class SalmonatorNoop:
    ''' Currently used in remote persistence servers to hush everything
    upstream/downstream while still making use of standard librarians.
    
    And by "used", I mean "unused, but is intended to be added in at 
    some point, because I forgot I was just using a standard Salmonator
    because the overhead is unimportant right now".
    '''
    def assemble(*args, **kwargs):
        pass
        
    def add_upstream_remote(*args, **kwargs):
        pass
        
    def remove_upstream_remote(*args, **kwargs):
        pass
        
    def add_downstream_remote(*args, **kwargs):
        pass
        
    def remove_downstream_remote(*args, **kwargs):
        pass
        
    def push(*args, **kwargs):
        pass
        
    def pull(*args, **kwargs):
        pass
        
    def register(*args, **kwargs):
        pass