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

import unittest
import warnings
import collections
import threading
import time
import argparse
import signal

from golix import Ghid

from hypergolix.core import AgentBase
from hypergolix.core import Dispatcher

from hypergolix.utils import Aengel
from hypergolix.utils import threading_autojoin

from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicClient
from hypergolix.comms import WSBasicServer

from hypergolix.persisters import PersisterBridgeClient
from hypergolix.persisters import PersisterBridgeServer
from hypergolix.persisters import MemoryPersister

from hypergolix.ipc import IPCHost
from hypergolix.ipc import IPCEmbed


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################


def _hgx_server(host, port, debug, verbosity, logfile, traceur, foreground=True):
    ''' Simple persistence server.
    Expected defaults:
    host:       'localhost'
    port:       7770
    logfile:    None
    verbosity:  'warning'
    debug:      False
    traceur:    False
    '''
    backend = MemoryPersister()
    aengel = Aengel()
    server = Autocomms(
        autoresponder_class = PersisterBridgeServer,
        autoresponder_kwargs = { 'persister': backend, },
        connector_class = WSBasicServer,
        connector_kwargs = {
            'host': host,
            'port': port,
            # 48 bits = 1% collisions at 2.4 e 10^6 connections
            'birthday_bits': 48,
        },
        debug = debug,
        aengel = aengel,
    )
    if foreground:
        threading_autojoin()
    return backend, server


class _HGXCore(Dispatcher, AgentBase):
    def __init__(self, persister, *args, **kwargs):
        super().__init__(dispatcher=self, persister=persister, *args, **kwargs)
    
    
def HypergolixLink(ipc_port=7772, debug=False, *args, **kwargs):
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
    
    
def main(host, port, ipc_port, debug, verbosity, logfile, traceur, foreground=True):
    ''' Expected defaults:
    host:       'localhost'
    port:       7770
    ipc_port:   7772
    debug:      False
    logfile:    None
    verbosity:  'warning'
    traceur:    False
    '''
    if debug:
        log_level = logging.DEBUG
        debug = True
    else:
        log_level = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
        }[verbosity.lower()]
        debug = False
        
    # Note: this isn't currently used.
    traceur = bool(traceur)
        
    if logfile:
        logging.basicConfig(filename=logfile, level=log_level)
    else:
        logging.basicConfig(level=log_level)
    
    # # Override the module-level logger definition to root
    # logger = logging.getLogger()
    # # For now, log to console
    # log_handler = logging.StreamHandler()
    # log_handler.setLevel(log_level)
    # logger.addHandler(log_handler)
    
    # Todo: add traceur argument to dump stack traces into debug log, and/or
    # use them to auto-detect deadlocks/hangs
    
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
    core = _HGXCore(
        persister = persister,
    )
    ipc = Autocomms(
        autoresponder_class = IPCHost,
        autoresponder_kwargs = {'dispatch': core},
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
                '"debug" -> most verbose, '
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
    main(
        args.host, 
        args.port, 
        args.ipc_port, 
        args.debug, 
        args.verbosity, 
        args.logfile, 
        args.traceur
    )