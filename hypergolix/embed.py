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
import logging
import collections
import weakref
import queue
import threading
import traceback
import asyncio
import loopa

# Local dependencies
from .persistence import _GidcLite
from .persistence import _GeocLite
from .persistence import _GobsLite
from .persistence import _GobdLite
from .persistence import _GdxxLite
from .persistence import _GarqLite

from .utils import SetMap
from .utils import WeakSetMap


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
            

class HGXLink:
    ''' Do the thing with the thing.
    '''
    
    def __init__(self, ipc_port=7772, debug=False, *args, **kwargs):
        super().__init__(*args, **kwargs)


def HGXLink(ipc_port=7772, debug=False, aengel=None):
    if not aengel:
        aengel = utils.Aengel()
        
    embed = ipc.IPCEmbed(
        aengel = aengel,
        threaded = True,
        thread_name = utils._generate_threadnames('em-aure')[0],
        debug = debug,
    )
    
    embed.add_ipc_threadsafe(
        client_class = comms.WSBasicClient,
        host = 'localhost',
        port = ipc_port,
        debug = debug,
        aengel = aengel,
        threaded = True,
        thread_name = utils._generate_threadnames('emb-ws')[0],
        tls = False
    )
        
    embed.aengel = aengel
    return embed
