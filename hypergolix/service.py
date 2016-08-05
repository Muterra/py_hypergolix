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
    'HypergolixService', 
    'HypergolixApplication'
]

import warnings
import collections
import threading
import time
import argparse
import signal

from golix import Ghid

from .accounting import AgentBootstrap

from .core import GolixCore
from .core import Oracle
from .dispatch import Dispatcher
from .dispatch import _Dispatchable
from .privateer import Privateer

from .utils import Aengel
from .utils import threading_autojoin

from .comms import Autocomms
from .comms import WSBasicClient
from .comms import WSBasicServer

from .remotes import PersisterBridgeClient
from .remotes import PersisterBridgeServer
from .remotes import MemoryPersister
from .remotes import RemotePersistenceServer as _hgx_server

from .ipc import IPCHost
from .ipc import IPCEmbed


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################
    
    
def HypergolixLink(ipc_port=7772, debug=False, aengel=None, *args, **kwargs):
    if not aengel:
        aengel = Aengel()
        
    acomms = Autocomms(
        autoresponder_class = IPCEmbed,
        connector_class = WSBasicClient,
        connector_kwargs = {
            'host': 'localhost', # IPC host
            'port': ipc_port, # IPC port
        },
        debug = debug,
        aengel = aengel,
    )
    acomms.aengel = aengel
    return acomms
    
    
def HGXService(host, port, ipc_port, debug, traceur, foreground=True, 
                aengel=None):
    ''' This is where all of the UX goes for the service itself. From 
    here, we build a credential, then a bootstrap, and then persisters,
    IPC, etc.
    
    Expected defaults:
    host:       'localhost'
    port:       7770
    ipc_port:   7772
    debug:      False
    logfile:    None
    verbosity:  'warning'
    traceur:    False
    '''
        
    debug = bool(debug)
    # Note: this isn't currently used.
    traceur = bool(traceur)
    
    # # Override the module-level logger definition to root
    # logger = logging.getLogger()
    # # For now, log to console
    # log_handler = logging.StreamHandler()
    # log_handler.setLevel(log_level)
    # logger.addHandler(log_handler)
    
    # Todo: add traceur argument to dump stack traces into debug log, and/or
    # use them to auto-detect deadlocks/hangs
    
    if not aengel:
        aengel = Aengel()
        
    persister = Autocomms(
        autoresponder_class = PersisterBridgeClient,
        connector_class = WSBasicClient,
        connector_kwargs = {
            'host': host,
            'port': port,
        },
        debug = debug,
        aengel = aengel,
    )
    
    # TODO: make the bootstrap work without the persister for init, and
    # then have an add_persister method
    core = AgentBootstrap(persister, credential=None, aengel=aengel)
    
    # TODO: make the bootstrap have an add_ipc method.
    ipc = Autocomms(
        autoresponder_class = IPCHost,
        autoresponder_kwargs = {'dispatch': core.dispatch},
        connector_class = WSBasicServer,
        connector_kwargs = {
            'host': 'localhost',
            'port': ipc_port,
        },
        debug = debug,
        aengel = aengel,
    )
    
    # Automatically detect if we're the main thread. If so, wait indefinitely
    # until signal caught.
    if foreground:
        threading_autojoin()
        
    return persister, core, ipc


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start the Hypergolix service.')
    parser.add_argument(
        '--host', 
        action = 'store',
        default = 'localhost', 
        type = str,
        help = 'Specify the persistence provider host [default: localhost]'
    )
    parser.add_argument(
        '--port', 
        action = 'store',
        default = 7770, 
        type = int,
        help = 'Specify the persistence provider port [default: 7770]'
    )
    parser.add_argument(
        '--ipc_port', 
        action = 'store',
        default = 7772, 
        type = int,
        help = 'Specify the ipc port [default: 7772]'
    )
    parser.add_argument(
        '--debug', 
        action = 'store_true',
        help = 'Set debug mode. Automatically sets verbosity to debug.'
    )
    parser.add_argument(
        '--logfile', 
        action = 'store',
        default = None, 
        type = str,
        help = 'Log to a specified file, relative to current directory.',
    )
    parser.add_argument(
        '--verbosity', 
        action = 'store',
        default = 'warning', 
        type = str,
        help = 'Specify the logging level. '
                '"extreme" -> ultramaxx verbose,'
                '"debug" -> normal most verbose, '
                '"info" -> somewhat verbose, '
                '"warning" -> default python verbosity, '
                '"error" -> quiet.',
    )
    parser.add_argument(
        '--traceur', 
        action = 'store_true',
        help = 'Enable thorough analysis, including stack tracing. '
                'Implies verbosity of debug.'
    )

    args = parser.parse_args()
    
    if args.debug:
        log_level = logging.DEBUG
    else:
        log_level = {
            'extreme': logging.DEBUG,
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
        }[args.verbosity.lower()]
    logging.getLogger('hypergolix').setLevel(log_level)
        
    if args.verbosity.lower() == 'extreme':
        logging.getLogger('websockets').setLevel(logging.DEBUG)
        logging.getLogger('asyncio').setLevel(logging.DEBUG)
    else:
        logging.getLogger('websockets').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    if args.logfile:
        loghandler = logging.FileHandler(logfile)
        loghandler.setFormatter(
            logging.Formatter(
                '%(threadName)-7s %(name)-12s: %(levelname)-8s %(message)s'
            )
        )
        logging.getLogger('hypergolix').addHandler(loghandler)
    
    HGXService(
        args.host, 
        args.port, 
        args.ipc_port, 
        args.debug, 
        args.traceur
    )