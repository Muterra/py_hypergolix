'''
Start a hypergolix service.

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

# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    # 'HypergolixService', 
    # 'HypergolixApplication'
]

import warnings
import collections
import threading
import time
import argparse
import signal

from golix import Ghid

from .bootstrapping import AgentBootstrap

from .core import GolixCore
from .core import Oracle
from .dispatch import Dispatcher
from .dispatch import _Dispatchable
from .privateer import Privateer

from .utils import Aengel
from .utils import threading_autojoin
from .utils import _generate_threadnames

from .comms import Autocomms
from .comms import WSBasicClient
from .comms import WSBasicServer

from .remotes import PersisterBridgeClient
from .remotes import PersisterBridgeServer
from .remotes import MemoryPersister
from .remotes import RemotePersistenceServer

from .ipc import IPCCore
from .ipc import IPCEmbed


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################


def _hgx_server(host, port, debug, traceur, foreground=True, aengel=None):
    ''' Simple remote persistence server over websockets.
    '''
    if not aengel:
        aengel = Aengel()
        
    remote = RemotePersistenceServer()
    server = Autocomms(
        autoresponder_name = 'reremser',
        autoresponder_class = PersisterBridgeServer,
        connector_name = 'wsremser',
        connector_class = WSBasicServer,
        connector_kwargs = {
            'host': host,
            'port': port,
            'tls': False,
            # 48 bits = 1% collisions at 2.4 e 10^6 connections
            'birthday_bits': 48,
        },
        debug = debug,
        aengel = aengel,
    )
    remote.assemble(server)
    
    if foreground:
        threading_autojoin()
    else:
        return remote, server
    
    
def HGXLink(ipc_port=7772, debug=False, aengel=None):
    if not aengel:
        aengel = Aengel()
        
    embed = IPCEmbed(
        aengel = aengel, 
        threaded = True, 
        thread_name = _generate_threadnames('em-aure')[0],
        debug = debug, 
    )
    
    embed.add_ipc_threadsafe(
        client_class = WSBasicClient,
        host = 'localhost',
        port = ipc_port,
        debug = debug,
        aengel = aengel,
        threaded = True,
        thread_name = _generate_threadnames('emb-ws')[0],
        tls = False
    )
        
    embed.aengel = aengel
    return embed