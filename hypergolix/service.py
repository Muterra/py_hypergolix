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

from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicClient
from hypergolix.comms import WSBasicServer

from hypergolix.persisters import PersisterBridgeClient

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
        '--logfile', 
        action = 'store',
        default = None, 
        type = str,
        help = 'Log to a specified file, relative to current directory.',
    )
    parser.add_argument(
        '--verbosity', 
        action = 'store',
        default = None, 
        type = str,
        help = 'Set debug mode and specify the logging level. '
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
    
    if args.verbosity is not None:
        debug = True
        log_level = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
        }[args.verbosity.lower()]
    else:
        debug = False
        log_level = logging.WARNING
        
    if args.traceur:
        traceur = True
        debug = True
        log_level = logging.DEBUG
    else:
        traceur = False
        
    if args.logfile:
        logging.basicConfig(filename=args.logfile, level=log_level)
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
            'host': args.host,
            'port': args.port,
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
            'port': args.ipc_port,
        },
        debug = debug,
        aengel = aengel,
    )
    
    # SO BEGINS the "cross-platform signal wait workaround"
    
    signame_lookup = {
        signal.SIGINT: 'SIGINT',
        signal.SIGTERM: 'SIGTERM',
    }
    def sighandler(signum, sigframe):
        raise ZeroDivisionError('Caught ' + signame_lookup[signum])

    try:
        signal.signal(signal.SIGINT, sighandler)
        signal.signal(signal.SIGTERM, sighandler)
        
        # This is a little gross, but will be broken out of by the signal handlers
        # erroring out.
        while True:
            time.sleep(600)
            
    except ZeroDivisionError as exc:
        logging.info(str(exc))