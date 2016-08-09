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
# import weakref
# import traceback
# import threading

# from golix import SecondParty
from golix import Ghid

# from golix.utils import AsymHandshake
# from golix.utils import AsymAck
# from golix.utils import AsymNak

# Local dependencies
# from .persistence import _GarqLite
# from .persistence import _GdxxLite


# ###############################################
# Boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    'Inquisitor', 
]


# ###############################################
# Library
# ###############################################


class Inquisitor:
    ''' The inquisitor handles resource utilization, locally removing
    GAOs from memory when they are no longer sufficiently used to 
    justify their overhead.
    '''
    pass
    # Note: you're probably not going to want to use the _GAO to maintain the
    # librarian retention directly, because not all _GAO have a librarian 
    # counterpart. For example, debindings will basically never be associated
    # with a live GAO, so, if you decided to let GAO live-ness dictate the
    # librarian caching of "lite"weight objects, you would never cache a GDXX.