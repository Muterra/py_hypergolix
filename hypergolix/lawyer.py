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
        
        
class LawyerCore(metaclass=API):
    ''' Enforces authorship requirements, including both having a known
    entity as author/recipient and consistency for eg. bindings and
    debindings.
    
    Threadsafe.
    '''
    _librarian = weak_property('__librarian')
    
    @fixture_api
    def __init__(self, librarian, *args, **kwargs):
        super(LawyerCore.__fixture__, self).__init__(*args, **kwargs)
        self._librarian = librarian
        
    def assemble(self, librarian):
        # Call before using.
        self._librarian = librarian
    
    @fixture_return(True)
    @public_api
    async def _validate_author(self, obj):
        try:
            author = await self._librarian.summarize(obj.author)
        
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
        
    async def validate_gidc(self, obj):
        ''' GIDC need no validation.
        '''
        return True
        
    async def validate_geoc(self, obj):
        ''' Ensure author is known and valid.
        '''
        return (await self._validate_author(obj))
        
    async def validate_gobs(self, obj):
        ''' Ensure author is known and valid.
        '''
        return (await self._validate_author(obj))
        
    async def validate_gobd(self, obj):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding (if it already exists).
        '''
        await self._validate_author(obj)
        
        if obj.counter > 0:
            try:
                existing = await self._librarian.summarize(obj.ghid)
            
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
        
    async def validate_gdxx(self, obj, target_obj=None):
        ''' Ensure author is known and valid, and consistent with the
        previous author for the binding.
        
        If other is not None, specifically checks it against that object
        instead of obtaining it from librarian.
        '''
        await self._validate_author(obj)
        
        try:
            if target_obj is None:
                existing = await self._librarian.summarize(obj.target)
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
        
    async def validate_garq(self, obj):
        ''' Validate recipient.
        '''
        try:
            recipient = await self._librarian.summarize(obj.recipient)
        
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
