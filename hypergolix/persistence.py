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
import abc
import collections
import warnings
import functools
import struct
import weakref
import queue
import pathlib
import base64
import concurrent.futures
import asyncio
import websockets
from websockets.exceptions import ConnectionClosed
import threading
import traceback

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

from .utils import weak_property
from .utils import readonly_property
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
# Lib
# ###############################################
        
        
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
        
    @classmethod
    def from_golix(cls, golix_obj):
        ''' Convert the golix object to a lightweight representation.
        '''
        identity = SecondParty.from_identity(golix_obj)
        return cls(
            ghid = golix_obj.ghid,
            identity = identity,
        )
        
        
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
        
    @classmethod
    def from_golix(cls, golix_obj):
        ''' Convert the golix object to a lightweight representation.
        '''
        return cls(
            ghid = golix_obj.ghid,
            author = golix_obj.author,
        )
        
        
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
        
    @classmethod
    def from_golix(cls, golix_obj):
        ''' Convert the golix object to a lightweight representation.
        '''
        return cls(
            ghid = golix_obj.ghid,
            author = golix_obj.binder,
            target = golix_obj.target,
        )
    
        
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
        
    @classmethod
    def from_golix(cls, golix_obj):
        ''' Convert the golix object to a lightweight representation.
        '''
        return cls(
            ghid = golix_obj.ghid_dynamic,
            author = golix_obj.binder,
            target = golix_obj.target,
            frame_ghid = golix_obj.ghid,
            history = golix_obj.history,
        )
    
        
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
        
    @classmethod
    def from_golix(cls, golix_obj):
        ''' Convert the golix object to a lightweight representation.
        '''
        return cls(
            ghid = golix_obj.ghid,
            author = golix_obj.debinder,
            target = golix_obj.target,
        )
        
        
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
        
    @classmethod
    def from_golix(cls, golix_obj):
        ''' Convert the golix object to a lightweight representation.
        '''
        return cls(
            ghid = golix_obj.ghid,
            recipient = golix_obj.recipient,
        )
        
        
class Doorman:
    ''' Parses files and enforces crypto. Can be bypassed for trusted
    (aka locally-created) objects. Only called from within the typeless
    PersisterCore.ingest() method.
    '''
    _librarian = weak_property('__librarian')
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._golix = ThirdParty()
        self._parselock_gidc = threading.Lock()
        self._parselock_geoc = threading.Lock()
        self._parselock_gobs = threading.Lock()
        self._parselock_gobd = threading.Lock()
        self._parselock_gdxx = threading.Lock()
        self._parselock_garq = threading.Lock()
        
    def assemble(self, librarian):
        # Called to link to the librarian.
        self._librarian = librarian
        
    def load_gidc(self, packed):
        try:
            with self._parselock_gidc:
                obj = GIDC.unpack(packed)
        except Exception as exc:
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GIDC object.'
            ) from exc
            
        # No further verification required.
            
        return obj
        
    def load_geoc(self, packed):
        try:
            with self._parselock_geoc:
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
            with self._parselock_gobs:
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
            with self._parselock_gobd:
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
            with self._parselock_gdxx:
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
                '0x0003: Unknown author / recipient: ' + str(obj.debinder)
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
            with self._parselock_garq:
                obj = GARQ.unpack(packed)
        
        except Exception as exc:
            raise MalformedGolixPrimitive(
                '0x0001: Invalid formatting for GARQ object.'
            ) from exc
            
        # Persisters cannot further verify the object.
            
        return obj
                
        
class PersistenceCore(metaclass=API):
    ''' Provides the core functions for storing Golix objects. Required
    for the hypergolix service to start.
    
    Can coordinate with both "upstream" and "downstream" persisters.
    Other persisters should pass through the "ingestive tract". Local
    objects can be published directly through calling the ingest_<type>
    methods.
    
    TODO: add librarian validation, so that attempting to update an
    object we already have an identical copy to silently exits.
    '''
    _doorman = weak_property('__doorman')
    _enforcer = weak_property('__enforcer')
    _lawyer = weak_property('__lawyer')
    _bookie = weak_property('__bookie')
    _postman = weak_property('__postman')
    _undertaker = weak_property('__undertaker')
    _librarian = weak_property('__librarian')
    _salmonator = weak_property('__salmonator')
    
    # Async stuff
    _executor = readonly_property('__executor')
    _loop = readonly_property('__loop')
    
    # This is a messy way of getting the suffix for validation and stuff but
    # it's getting the job done.
    _ATTR_LOOKUP = {
        _GidcLite: 'gidc',
        _GeocLite: 'geoc',
        _GobsLite: 'gobs',
        _GobdLite: 'gobd',
        _GdxxLite: 'gdxx',
        _GarqLite: 'garq'
    }
    _LITE_LOOKUP = {
        GIDC: _GidcLite,
        GEOC: _GeocLite,
        GOBS: _GobsLite,
        GOBD: _GobdLite,
        GDXX: _GdxxLite,
        GARQ: _GarqLite
    }
    
    @public_api
    def __init__(self, executor, loop, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Async-specific stuff
        setattr(self, '__executor', executor)
        setattr(self, '__loop', loop)
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' Right now, well, this is basically a noop. Anticipating
        substantial changes!
        '''
        super(PersistenceCore.__fixture__, self).__init__(
            executor = None,
            loop = None,
            *args,
            **kwargs
        )
        
    def assemble(self, doorman, enforcer, lawyer, bookie, librarian, postman,
                 undertaker, salmonator):
        self._doorman = doorman
        self._enforcer = enforcer
        self._lawyer = lawyer
        self._bookie = bookie
        self._postman = postman
        self._undertaker = undertaker
        self._librarian = librarian
        self._salmonator = salmonator
        
    def _validators(self, obj):
        ''' Late-binding access to the things that validate objects. If
        not late, would hold strong references to stuff, which is bad.
        '''
        # Calculate "validate_gidc", etc
        method = 'validate_' + self._ATTR_LOOKUP[type(obj)]
        
        # This determines the order, and who checks.
        # Check for redundant items.
        yield getattr(self._librarian, method)
        # Enforce target selection
        yield getattr(self._enforcer, method)
        # Now make sure authorship requirements are satisfied
        yield getattr(self._lawyer, method)
        # Finally make sure persistence rules are followed
        yield getattr(self._bookie, method)
            
    def _preppers(self, obj):
        ''' Late-binding access to the things that intake validated
        objects. Same note as above re: strong references vs memoize.
        '''
        # Calculate "validate_gidc", etc
        suffix = self._ATTR_LOOKUP[type(obj)]
        
        # And now prep the undertaker for any necessary GC
        yield getattr(self._undertaker, 'prep_' + suffix)
        # Everything is validated. Place with the bookie before librarian, so
        # that it has access to the old librarian state
        yield getattr(self._bookie, 'place_' + suffix)
        
    async def direct_ingest(self, obj, packed, remotable):
        ''' Standard ingestion flow for stuff. To be called from ingest
        above, or directly (for objects created "in-house").
        '''
        # Validate the object. A return of None indicates a redundant object,
        # which results in immediate short-circuit. If any of the validators
        # find an invalid object, they will raise.
        for validate in self._validators(obj):
            if not validate(obj):
                return None
        
        for prep in self._preppers(obj):
            prep(obj)
            
        self._librarian.store(obj, packed)
        
        if remotable:
            self._salmonator.schedule_push(obj.ghid)
            
        return obj
        
    async def _attempt_load(self, packed):
        ''' Attempt to load a packed golix object.
        '''
        tasks = set()
        
        # First run through all of the loaders and see if anything catches.
        for loader in (self._doorman.load_gidc, self._doorman.load_geoc,
                       self._doorman.load_gobs, self._doorman.load_gobd,
                       self._doorman.load_gdxx, self._doorman.load_garq):
            # Attempt this loader and record which ingester it is
            tasks.add(asyncio.ensure_future(
                # But run the actual loader in the executor
                self._loop.run_in_executor(
                    self._executor,
                    loader,
                    packed
                )
            ))
            
        # Do all of the ingesters. This could be smarter (short-circuit as soon
        # as a successful loader completes) but for now this is good enough.
        finished, pending = await asyncio.wait(
            fs = tasks,
            return_when = asyncio.ALL_COMPLETED
        )
        
        # Don't bother with pending tasks because THEORETICALLY we can't have
        # any.
        result = None
        for task in finished:
            exc = task.exception()
            
            # No exception means we successfully loaded. Ingest it! But don't
            # immediately exit the loop, because we need to collect the rest of
            # the exceptions first.
            if exc is None:
                result = task.result()
            
            # This loader task failed, so try the next.
            elif isinstance(task, MalformedGolixPrimitive):
                continue
            
            # Raise the first exception that we encounter that isn't from it
            # being an incorrect primitive.
            else:
                raise exc
                
        # We might return None here, but let the parent function raise in that
        # case.
        return result
        
    async def ingest(self, packed, remotable=True):
        ''' Called on an untrusted and unknown object. May be bypassed
        by locally-created, trusted objects (by calling the individual
        ingest methods directly). Parses, validates, and stores the
        object, and returns True; or, raises an error.
        '''
        # This may return None, but that will be caught by the KeyError below.
        golix_obj = self._attempt_load(packed)
        
        try:
            litecls = self._LITE_LOOKUP[type(golix_obj)]
        
        # We did not find a successful loader (or something went funky with the
        # type lookup)
        except KeyError:
            raise MalformedGolixPrimitive(
                'Packed bytes do not appear to be a Golix primitive.'
            ) from None
        
        obj_lite = litecls.from_golix(golix_obj)
        ingested = await self.direct_ingest(obj_lite, packed, remotable)
        
        # Note that individual ingest methods are only called directly for
        # locally-built objects, which do not need a mail run.
        # If the object is identical to what we already have, the ingester
        # will return None.
        if ingested:
            await self._postman.schedule(ingested)
            
        else:
            logger.debug(
                str(obj_lite.ghid) + ' scheduling aborted on unchanged object.'
            )
        
        # Note: this is not the place for salmonator pushing! Locally
        # created/updated objects call the individual ingest methods
        # directly, so they have to be the ones that actually deal with it.
        
        return ingested
        
        
class Enforcer:
    ''' Enforces valid target selections.
    '''
    _librarian = weak_property('__librarian')
        
    def assemble(self, librarian):
        # Call before using.
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
            target = self._librarian.summarize(obj.target)
        # TODO: think more about this, and whether everything has been updated
        # appropriately to raise a DoesNotExist instead of a KeyError.
        # This could be more specific and say DoesNotExist
        except KeyError:
            logger.debug(
                str(obj.target) + ' missing from librarian w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
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
            logger.debug(
                str(obj.target) + ' missing from librarian w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
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
            logger.debug(
                'Traceback for missing ' + str(obj.ghid) + ':\n' +
                ''.join(traceback.format_exc())
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
            logger.debug(
                str(obj.ghid) + ' uploaded zeroth frame WITH history.'
            )
                
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
    _librarian = weak_property('__librarian')
        
    def assemble(self, librarian):
        # Call before using.
        self._librarian = librarian
        
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
            logger.debug(
                str(obj.ghid) + ' missing from librarian w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
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
            logger.debug(
                str(obj.target) + ' missing from librarian w/ traceback:\n' +
                ''.join(traceback.format_exc())
            )
            
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
            
            
class Bookie(metaclass=API):
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
            
            
class LibrarianCore(metaclass=API):
    ''' Base class for caching systems common to non-volatile librarians
    such as DiskLibrarian, S3Librarian, etc.
    
    TODO: make ghid vs frame ghid usage more consistent across things.
    '''
    _percore = weak_property('__percore')
    
    @public_api
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Operations and restoration lock
        self._restoring = TruthyLock()
        
        # Lookup for dynamic ghid -> frame ghid
        # Must be consistent across all concurrently connected librarians
        self._dyn_resolver = {}
        
        # Lookup for ghid -> hypergolix description
        # This may be GC'd by the python process.
        self._catalog = {}
        self._opslock = threading.Lock()
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' Construct an in-memory-only version of librarian.
        '''
        super(LibrarianCore.__fixture__, self).__init__(*args, **kwargs)
        self._shelf = {}
        
    def assemble(self, percore):
        # Hmmm
        self._percore = percore
        
    def force_gc(self, obj):
        ''' Forces erasure of an object. Does not notify the undertaker.
        Indempotent. Should never raise KeyError.
        '''
        with self._restoring.mutex:
            try:
                ghid = self._ghid_resolver(obj.ghid)
                self.remove_from_cache(ghid)
            except Exception:
                logger.warning(
                    'Exception while removing from cache during object GC. '
                    'Probably a bug.\n' + ''.join(traceback.format_exc())
                )
            
            try:
                del self._catalog[ghid]
            except KeyError:
                logger.debug(
                    str(ghid) + ' missing from librarian w/ traceback:\n' +
                    ''.join(traceback.format_exc())
                )
                
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
            logger.debug('Attempting lazy-load for ' + str(ghid))
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
                    self._percore.ingest(gidc.packed, remotable=False)
                    # self._percore.ingest_gidc(gidc)
                    
                # Now all debindings, so that we can check state while we're at
                # it
                for gdxx in gdxxs:
                    self._percore.ingest(gdxx.packed, remotable=False)
                    # self._percore.ingest_gdxx(gdxx)
                    
                # Now all bindings, so that objects aren't gc'd. Note: can't
                # combine into single list, because of different ingest methods
                for gobs in gobss:
                    self._percore.ingest(gobs.packed, remotable=False)
                    # self._percore.ingest_gobs(gobs)
                for gobd in gobds:
                    self._percore.ingest(gobd.packed, remotable=False)
                    # self._percore.ingest_gobd(gobd)
                    
                # Next the objects themselves, so that any requests will have
                # their targets available (not that it would matter yet,
                # buuuuut)...
                for geoc in geocs:
                    self._percore.ingest(geoc.packed, remotable=False)
                    # self._percore.ingest_geoc(geoc)
                    
                # Last but not least
                for garq in garqs:
                    self._percore.ingest(garq.packed, remotable=False)
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
                target.append(golix_obj)
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
    
    @public_api
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        pass
            
    @add_to_cache.fixture
    def add_to_cache(self, ghid, data):
        ''' Adds the passed raw data to the cache.
        '''
        self._shelf[ghid] = data
        
    @public_api
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the
        cache.
        '''
        pass
        
    @remove_from_cache.fixture
    def remove_from_cache(self, ghid):
        ''' Removes the data associated with the passed ghid from the
        cache.
        '''
        del self._shelf[ghid]
        
    @public_api
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        pass
        
    @get_from_cache.fixture
    def get_from_cache(self, ghid):
        ''' Returns the raw data associated with the ghid.
        '''
        return self._shelf[ghid]
        
    @public_api
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        pass
        
    @check_in_cache.fixture
    def check_in_cache(self, ghid):
        ''' Check to see if the ghid is contained in the cache.
        '''
        return ghid in self._shelf
        
    @public_api
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        pass
        
    @walk_cache.fixture
    def walk_cache(self):
        ''' Iterator to go through the entire cache, returning possible
        candidates for loading. Loading will handle malformed primitives
        without error.
        '''
        pass
    
    
class DiskLibrarian(LibrarianCore):
    ''' Librarian that caches stuff to disk.
    '''
    
    def __init__(self, cache_dir, *args, **kwargs):
        ''' cache_dir should be relative to current.
        '''
        super().__init__(*args, **kwargs)
        
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
        
        
class MemoryLibrarian(LibrarianCore):
    ''' DEPRECATED. Use LibrarianCore.__fixture__ instead.
    '''
    
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
