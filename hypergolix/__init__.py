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

# TODO: audit entire library for late-binding closure safety, particularly with
# internal function definitions. See the wrapped share handlers within
# ipc.IPCEmbed.register_share_handler_loopsafe as an example of proper safety.


# Submodules
from . import accounting
from . import app
from . import bootstrapping
from . import comms
from . import core
from . import dispatch
from . import embed
from . import exceptions
from . import hypothetical
from . import inquisition
from . import ipc
from . import lawyer
from . import librarian
from . import logutils
from . import objproxy
from . import persistence
from . import postal
from . import privateer
from . import remotes
from . import rolodex
from . import service
from . import utils
from . import undertaker

# Add in toplevel stuff
from golix import Ghid

from .objproxy import Obj
from .objproxy import Proxy
from .objproxy import PickleObj
from .objproxy import PickleProxy
from .objproxy import JsonObj
from .objproxy import JsonProxy

from .embed import HGXLink


# ###############################################
# Boilerplate
# ###############################################


# Logging shenanigans
import logging
# Py2.7+, but this is Py3.5.1+
from logging import NullHandler
logging.getLogger(__name__).addHandler(NullHandler())


# Control * imports.
__all__ = [
    'HGXLink',
    'Ghid',
    'Obj',
    'Proxy',
    'PickleObj',
    'PickleProxy',
    'JsonObj',
    'JsonProxy',
]
