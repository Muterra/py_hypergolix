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
from hypergolix.persisters import LocalhostClient
from hypergolix.ipc_hosts import WebsocketsIPC
from hypergolix.embeds import WebsocketsEmbed


class HypergolixService(WebsocketsIPC, Dispatcher, AgentBase):
    def __init__(self, host, *args, **kwargs):
        super().__init__(
            dispatcher = self, 
            dispatch = self, 
            persister = LocalhostClient(host=host, port=7770), 
            host = 'localhost', # IPC host
            port = 7772, # IPC port
            # threaded = True,
            *args, **kwargs
        )
        
    
class HypergolixLink(WebsocketsEmbed):
    def __init__(self, *args, **kwargs):
        super().__init__(
            host = 'localhost', # IPC host
            port = 7772, # IPC port
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

    args = parser.parse_args()
    
    service = HypergolixService(
        host = args.host,
        threaded = False,
    )
    service.ws_run()