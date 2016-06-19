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

from golix import Ghid

from hypergolix.core import AgentBase
from hypergolix.core import Dispatcher
from hypergolix.persisters import WSPersister
from hypergolix.ipc_hosts import WebsocketsIPC
from hypergolix.embeds import WebsocketsEmbed


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################


class HypergolixService(WebsocketsIPC, Dispatcher, AgentBase):
    def __init__(self, host, debug=False, *args, **kwargs):
        super().__init__(
            dispatcher = self, 
            dispatch = self, 
            persister = WSPersister(
                host = host, 
                port = 7770, 
                threaded = True, 
                debug = debug
            ), 
            host = 'localhost', # IPC host
            port = 7772, # IPC port
            # threaded = True,
            debug = debug,
            *args, **kwargs
        )
        
    
class HypergolixLink(WebsocketsEmbed):
    def __init__(self, debug=False, *args, **kwargs):
        super().__init__(
            host = 'localhost', # IPC host
            port = 7772, # IPC port
            debug = debug,
            # threaded = True,
            *args, **kwargs
        )


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
        '--verbosity', 
        action = 'store',
        default = None, 
        type = str,
        help = 'Set debug mode and specify the logging level. '
                '"debug" -> most verbose, '
                '"info" -> somewhat verbose, '
                '"error" -> quiet.',
    )
    parser.add_argument(
        '--traceur', 
        action = 'store_true',
        help = 'Enable thorough analysis, including stack tracing. '
                'Implies verbosity of debug.'
    )

    args = parser.parse_args()
    
    if args.debug is not None:
        debug = True
        log_level = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'error': logging.ERROR,
        }[args.debug.lower()]
    else:
        debug = False
        log_level = logging.WARNING
        
    if args.traceur:
        traceur = True
        debug = True
        log_level = logging.DEBUG
    else:
        traceur = False
    
    # Override the module-level logger definition to root
    logger = logging.getLogger()
    # For now, log to console
    log_handler = logging.StreamHandler()
    log_handler.setLevel(log_level)
    logger.addHandler(log_handler)
    
    # Todo: add traceur argument to dump stack traces into debug log, and/or
    # use them to auto-detect deadlocks/hangs
    
    service = HypergolixService(
        host = args.host,
        threaded = False,
        debug = debug
    )
    
    # trace_start('debug/service.html')
    
    service.ws_run()
    
    # trace_stop()