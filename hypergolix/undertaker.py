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
from .hypothetical import fixture_return

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
            
            
class UndertakerCore(loopa.TaskLooper, metaclass=API):
    ''' Note: what about post-facto removal of bindings that have
    illegal targets? For example, if someone uploads a binding for a
    target that isn't currently known, and then it turns out that the
    target, once uploaded, actually doesn't support that binding, what
    should we do?
    
    In theory it shouldn't affect other operations. Should we just bill
    for it and call it a day? We'd need to make some kind of call to the
    bookie to handle that.
    '''
    _librarian = weak_property('__librarian')
    _postman = weak_property('__postman')
    
    def __init__(self, *args, maxlen=25, **kwargs):
        super().__init__(*args, **kwargs)
        self._maxlen = maxlen
        self._triage = None
        
        self._check_lookup = {
            _GidcLite: self._check_gidc,
            _GeocLite: self._check_geoc,
            _GobsLite: self._check_gobs,
            _GobdLite: self._check_gobd,
            _GdxxLite: self._check_gdxx,
            _GarqLite: self._check_garq
        }
        
    async def await_idle(self):
        ''' Wait until the undertaker has no more GC to perform.
        '''
        while self._triage is None:
            await asyncio.sleep(.01)
        
        await self._triage.join()
        
    async def loop_init(self):
        ''' Set up the staging queue.
        '''
        self._triage = asyncio.Queue(maxsize=self._maxlen)
        
    async def loop_stop(self):
        ''' Clear the staging queue.
        '''
        self._triage = None
        
    async def loop_run(self):
        ''' Wait for stuff to be added to the queue, then execute GC on
        it.
        '''
        ghid_to_collect, skip_conn = await self._triage.get()
        logger.debug('GC ' + str(ghid_to_collect) + ' check starting.')
        try:
            obj = await self._librarian.summarize(ghid_to_collect)
            
            try:
                collection_check = self._check_lookup[type(obj)]
            
            except KeyError as exc:
                raise TypeError(
                    'Invalid object type received from librarian.'
                ) from exc
                
            else:
                removable = await collection_check(obj, skip_conn)
                if removable:
                    logger.info(
                        'GC ' + str(ghid_to_collect) + ' collection starting.'
                    )
                    await self._gc_execute(obj, skip_conn)
                else:
                    logger.debug(
                        'GC ' + str(ghid_to_collect) + ' collection survived.'
                    )
            
        # except DoesNotExist:
        except KeyError:
            logger.warning(
                'GC ' + str(ghid_to_collect) +
                ' missing in librarian. Already collected?'
            )
            
        finally:
            self._triage.task_done()
        
    def assemble(self, librarian, postman):
        # Call before using.
        self._librarian = librarian
        self._postman = postman
            
    async def _check_gidc(self, obj, skip_conn=None):
        ''' Check whether we should remove a GIDC, and then remove it
        if appropriate. Currently we don't do that, so just leave it
        alone.
        '''
        return False
            
    async def _check_geoc(self, obj, skip_conn=None):
        ''' Check whether we should remove a GEOC, and then remove it if
        appropriate. Pretty simple: is it bound?
        '''
        # Keep if bound
        if (await self._librarian.is_bound(obj)):
            return False
        # Remove if unbound
        else:
            return True
            
    async def _check_gobs(self, obj, skip_conn=None):
        if (await self._librarian.is_debound(obj)):
            # Add our target to the list of GC checks
            await self._triage.put((obj.target, skip_conn))
            return True
        else:
            return False
            
    async def _check_gobd(self, obj, skip_conn=None):
        # If we've been debound, it might be time to die
        if (await self._librarian.is_debound(obj)):
            # Except child bindings can prevent GCing GOBDs
            if (await self._librarian.is_bound(obj)):
                return False
            
            # Dead binding.
            else:
                # Still need to add target
                await self._triage.put((obj.target, skip_conn))
                return True
        
        # Nope, still alive.
        else:
            return False
            
    async def _check_gdxx(self, obj, skip_conn=None):
        # Note that removing a debinding cannot result in a downstream target
        # being GCd, because it wouldn't exist.
        if (await self._librarian.is_debound(obj)):
            return True
        else:
            return False
            
    async def _check_garq(self, obj, skip_conn=None):
        if (await self._librarian.is_debound(obj)):
            return True
        else:
            return False
        
    async def _gc_execute(self, obj, skip_conn):
        # Next, goodbye object.
        await self._librarian.abandon(obj)
        # Now notify the postman, and tell her it's a removal.
        await self._postman.schedule(obj, removed=True, skip_conn=skip_conn)
    
    @fixture_return(None)
    @public_api
    async def alert_gidc(self, obj, skip_conn=None):
        ''' GIDC do not affect GC.
        '''
        # GIDC creates zero triage calls.
        return None
        
    @fixture_return(None)
    @public_api
    async def alert_geoc(self, obj, skip_conn=None):
        ''' GEOC do not affect GC.
        '''
        # GEOC creates zero triage calls.
        return None
        
    @fixture_return(None)
    @public_api
    async def alert_gobs(self, obj, skip_conn=None):
        ''' GOBS do not affect GC.
        '''
        return None
        
    @fixture_return(None)
    @public_api
    async def alert_gobd(self, obj, skip_conn=None):
        ''' GOBD require triage for previous targets.
        '''
        try:
            existing = await self._librarian.summarize(obj.ghid)
        
        except KeyError:
            # This will always happen if it's the first frame, so let's be sure
            # to ignore that for logging.
            if len(obj.target_vector) > 1:
                logger.warning('Could not find gobd to check existing target.')
            triaged = None
        
        else:
            triaged = existing.target
            await self._triage.put((triaged, skip_conn))
            
        return triaged
        
    @public_api
    async def alert_gdxx(self, obj, skip_conn=None):
        ''' GDXX require triage for new targets.
        '''
        triaged = obj.target
        await self._triage.put((triaged, skip_conn))
        return triaged
        
    @alert_gdxx.fixture
    async def alert_gdxx(self, obj, skip_conn=None):
        ''' Return the obj.target without triaging when fixtured.
        '''
        return obj.target
        
    @fixture_return(None)
    @public_api
    async def alert_garq(self, obj, skip_conn=None):
        ''' GARQ do not affect GC.
        '''
        return None
        
        
class Ferryman(UndertakerCore):
    ''' An undertaker that also calls privateer.abandon and
    oracle.forget.
    '''
    # This is needed so gc_execute can abandon secrets
    _privateer = weak_property('__privateer')
    # This is needed so gc_execute can flush the oracle cache
    _oracle = weak_property('__oracle')
    
    def assemble(self, librarian, oracle, postman, privateer):
        super().assemble(librarian, postman)
        self._privateer = privateer
        self._oracle = oracle
        
    async def _gc_execute(self, obj, skip_conn):
        # Do our stuff first so that there's still access to librarian
        # state, if it ends up being needed
        self._oracle.forget(obj.ghid)
        self._privateer.abandon(obj.ghid, quiet=True)
        # Don't flush any of the GAOs associated with that; just let it be
        # rolled into the next push-upstream-update
        await super()._gc_execute(obj, skip_conn)
