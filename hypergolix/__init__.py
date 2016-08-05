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

# Add in core module
from .core import *
# TODO: change this to select HERE what, exactly, we want as a toplevel 
# namespace on import

# Submodules
from . import accounting
from . import comms
from . import core
from . import dispatch
from . import exceptions
from . import ipc
from . import persistence
from . import privateer
from . import remotes
from . import rolodex
from . import service
from . import utils

# Logging shenanigans
import logging
# Py2.7+, but this is Py3.5.1+
from logging import NullHandler
logging.getLogger(__name__).addHandler(NullHandler())