'''
Scratchpad for test-based development.

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

import IPython
import unittest
import warnings
import collections
import logging
import weakref

from hypergolix.core import GolixCore
from hypergolix.core import Oracle
from hypergolix.core import GhidProxier
from hypergolix.core import _GAO

from hypergolix.privateer import Privateer

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Doorman
from hypergolix.persistence import Lawyer
from hypergolix.persistence import Enforcer
from hypergolix.persistence import Bookie
from hypergolix.persistence import MemoryLibrarian
from hypergolix.persistence import MrPostman
from hypergolix.persistence import Undertaker
from hypergolix.persistence import SalmonatorNoop
from hypergolix.persistence import _GidcLite
from hypergolix.persistence import _GeocLite
from hypergolix.persistence import _GobdLite

# This is a semi-normal import
from golix.utils import _dummy_ghid

# These are fixture imports
from golix import Ghid


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2


class MockDispatch:
    ''' Test fixture for dispatch.
    
    Oops, didn't actually need to do this. I think?
    '''
    def __init__(self):
        self.incoming = []
        self.acks = []
        self.naks = []
        
    def assemble(self, *args, **kwargs):
        # Noop
        pass
        
    def bootstrap(self, *args, **kwargs):
        # Noop
        pass
        
    def dispatch_share(self, target):
        self.incoming.append(target)
        
    def dispatch_share_ack(self, target, recipient):
        self.acks.append((target, recipient))
        
    def dispatch_share_nak(self, target, recipient):
        self.naks.append((target, recipient))
        
        
class MockRolodex:
    def request_handler(self, subs_ghid, notify_ghid):
        # Noop
        pass


# ###############################################
# Testing
# ###############################################
        
        
class PrivateerTest(unittest.TestCase):
    def test_trash(self):
        raise NotImplementedError()
        

if __name__ == "__main__":
    from _fixtures import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()