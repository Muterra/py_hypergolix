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
from hypergolix.persistence import MemoryLibrarian
from hypergolix.persistence import SalmonatorNoop
from hypergolix.persistence import _GidcLite

# This is a semi-normal import
from golix.utils import _dummy_ghid

# These are fixture imports
from golix import Ghid


# ###############################################
# Really shitty test fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2

from _fixtures.remote_exchanges import handshake2_1
from _fixtures.remote_exchanges import secret2_1
from _fixtures.remote_exchanges import cont2_1


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


# ###############################################
# Testing
# ###############################################


class GCoreTest(unittest.TestCase):
    def setUp(self):
        self.core = GolixCore()
        self.librarian = MemoryLibrarian()
        self.salmonator = SalmonatorNoop()
        
        self.core.assemble(self.librarian)
        self.librarian.assemble(self.core, self.salmonator)
        
        self.core.bootstrap(TEST_AGENT1)
        self.librarian.store(
            _GidcLite(TEST_AGENT1.ghid, TEST_AGENT1.second_party), 
            TEST_AGENT1.second_party.packed)
        self.librarian.store(
            _GidcLite(TEST_AGENT2.ghid, TEST_AGENT2.second_party), 
            TEST_AGENT2.second_party.packed)
        
    def test_trash(self):
        # Test whoami
        self.assertEqual(self.core.whoami, TEST_AGENT1.ghid)
        # Test legroom
        self.assertGreaterEqual(self.core._legroom, 2)
        
        # Note that all of these are stateless, so they don't need to be actual
        # legitimate targets or anything, just actual GHIDs
        payload = TEST_AGENT1.make_handshake(
            target = cont2_1.ghid,
            secret = secret2_1)
        self.core.open_request(handshake2_1.packed)
        self.core.make_request(
            recipient = TEST_AGENT2.ghid, 
            payload = payload)
        self.core.open_container(
            container = cont2_1, 
            secret = secret2_1)
        self.core.make_container(
            data = b'Hello world',
            secret = TEST_AGENT1.new_secret())
        self.core.make_binding_stat(target=cont2_1.ghid)
        dynamic1 = self.core.make_binding_dyn(target=cont2_1.ghid)
        dynamic2 = self.core.make_binding_dyn(
            target = cont2_1.ghid, 
            ghid = dynamic1.ghid_dynamic,
            history = [dynamic1.ghid])
        self.core.make_debinding(target=dynamic2.ghid_dynamic)
        
        
class OracleTest(unittest.TestCase):
    def test_trash(self):
        raise NotImplementedError()
        
        
class GhidproxyTest(unittest.TestCase):
    def test_trash(self):
        raise NotImplementedError()
        
        
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