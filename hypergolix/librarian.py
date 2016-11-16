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
import pathlib

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

# Internal dependencies
from .persistence import _GidcLite
from .persistence import _GeocLite
from .persistence import _GobsLite
from .persistence import _GobdLite
from .persistence import _GdxxLite
from .persistence import _GarqLite

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

from .utils import weak_property
from .utils import readonly_property
from .utils import TruthyLock
from .utils import SetMap
from .utils import WeakSetMap
from .utils import _generate_threadnames


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
