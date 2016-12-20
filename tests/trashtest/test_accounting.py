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

import unittest
import logging
import concurrent.futures

from loopa.utils import await_coroutine_threadsafe
from loopa import NoopLoop

# These are normal imports
from hypergolix.accounting import Account
from hypergolix.app import HypergolixCore

from hypergolix.persistence import PersistenceCore
from hypergolix.librarian import LibrarianCore
from hypergolix.core import GolixCore
from hypergolix.core import Oracle
from hypergolix.core import GhidProxier
from hypergolix.privateer import Privateer
from hypergolix.dispatch import Dispatcher
from hypergolix.rolodex import Rolodex
from hypergolix.remotes import Salmonator

# These are abnormal imports
from golix import Ghid
from golix import ThirdParty
from golix import SecondParty

from golix._getlow import GIDC
from hypergolix.persistence import _GidcLite


# ###############################################
# Fixture imports
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_READER1
gidc = TEST_READER1.packed
gidclite1 = _GidcLite.from_golix(GIDC.unpack(TEST_READER1.packed))

logger = logging.getLogger(__name__)


# ###############################################
# Testing
# ###############################################


class AccountTest(unittest.TestCase):
    ''' Test Accounts, particularly bootstrapping.
    '''
    
    @classmethod
    def setUpClass(cls):
        ''' Set up all of the various stuff. And things.
        '''
        # Set up the nooploop
        cls.nooploop = NoopLoop(
            debug = True,
            threaded = True
        )
        cls.nooploop.start()
        
    @classmethod
    def tearDownClass(cls):
        # Kill the running loop.
        cls.nooploop.stop_threadsafe_nowait()
        
    def setUp(self):
        ''' Do any per-test fixturing.
        '''
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.librarian = LibrarianCore.__fixture__()
        
        # Minimize fixture usage to avoid hidden bugs.
        self.golcore = GolixCore(
            executor = self.executor,
            loop = self.nooploop._loop
        )
        self.ghidproxy = GhidProxier()
        self.privateer = Privateer()
        
        self.oracle = Oracle.__fixture__()
        self.rolodex = Rolodex.__fixture__()
        self.dispatch = Dispatcher.__fixture__()
        self.percore = PersistenceCore.__fixture__(librarian=self.librarian)
        self.salmonator = Salmonator.__fixture__()
        
        self.golcore.assemble(self.librarian)
        self.ghidproxy.assemble(self.librarian)
        self.privateer.assemble(self.golcore)
        
        self.hgxcore = HypergolixCore.__fixture__(
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            oracle = self.oracle,
            rolodex = self.rolodex,
            dispatch = self.dispatch,
            percore = self.percore,
            librarian = self.librarian,
            salmonator = self.salmonator
        )
        
        self.root_secret = TEST_AGENT1.new_secret()
        self.account = Account(
            user_id = TEST_AGENT1,
            root_secret = self.root_secret,
            hgxcore = self.hgxcore
        )
        
    def _get_target(self, ghid):
        ''' Figure out what is being targeted by the dynamic ghid.
        '''
        gobdlite = await_coroutine_threadsafe(
            coro = self.librarian.summarize(ghid),
            loop = self.nooploop._loop
        )
        return gobdlite.target
        
    def test_account_creation(self):
        ''' Test the zeroth bootstrap.
        '''
        await_coroutine_threadsafe(
            coro = self.account.bootstrap(),
            loop = self.nooploop._loop
        )
        
        # Make sure we have secrets for everything stored away.
        self.assertIn(
            self._get_target(self.account.rolodex_pending.ghid),
            self.privateer
        )
        self.assertIn(
            self._get_target(self.account.rolodex_outstanding.ghid),
            self.privateer
        )
        self.assertIn(
            self._get_target(self.account.dispatch_tokens.ghid),
            self.privateer
        )
        self.assertIn(
            self._get_target(self.account.dispatch_startup.ghid),
            self.privateer
        )
        self.assertIn(
            self._get_target(self.account.dispatch_private.ghid),
            self.privateer
        )
        self.assertIn(
            self._get_target(self.account.dispatch_incoming.ghid),
            self.privateer
        )
        self.assertIn(
            self._get_target(self.account.dispatch_orphan_acks.ghid),
            self.privateer
        )
        self.assertIn(
            self._get_target(self.account.dispatch_orphan_naks.ghid),
            self.privateer
        )
        
    def test_account_restoration(self):
        ''' Test restoring an account.
        '''
        await_coroutine_threadsafe(
            coro = self.account.bootstrap(),
            loop = self.nooploop._loop
        )
        
        golcore2 = GolixCore.__fixture__(
            TEST_AGENT1,
            librarian = self.librarian
        )
        ghidproxy2 = GhidProxier()
        ghidproxy2.assemble(self.librarian)
        # CANNOT BE FIXTURE! Or, reloading will not work.
        privateer2 = Privateer()
        privateer2.assemble(golcore2)
        oracle2 = Oracle.__fixture__()
        rolodex2 = Rolodex.__fixture__()
        dispatch2 = Dispatcher.__fixture__()
        percore2 = PersistenceCore.__fixture__(librarian=self.librarian)
        salmonator2 = Salmonator.__fixture__()
        
        hgxcore2 = HypergolixCore.__fixture__(
            golcore = golcore2,
            ghidproxy = ghidproxy2,
            privateer = privateer2,
            oracle = oracle2,
            rolodex = rolodex2,
            dispatch = dispatch2,
            percore = percore2,
            librarian = self.librarian,
            salmonator = salmonator2
        )
        
        account2 = Account(
            user_id = self.account._user_id,
            root_secret = self.root_secret,
            hgxcore = hgxcore2
        )
        await_coroutine_threadsafe(
            coro = account2.bootstrap(),
            loop = self.nooploop._loop
        )
        
        # Make sure we restored secrets for everything.
        self.assertIn(
            self._get_target(account2.rolodex_pending.ghid),
            privateer2
        )
        self.assertIn(
            self._get_target(account2.rolodex_outstanding.ghid),
            privateer2
        )
        self.assertIn(
            self._get_target(account2.dispatch_tokens.ghid),
            privateer2
        )
        self.assertIn(
            self._get_target(account2.dispatch_startup.ghid),
            privateer2
        )
        self.assertIn(
            self._get_target(account2.dispatch_private.ghid),
            privateer2
        )
        self.assertIn(
            self._get_target(account2.dispatch_incoming.ghid),
            privateer2
        )
        self.assertIn(
            self._get_target(account2.dispatch_orphan_acks.ghid),
            privateer2
        )
        self.assertIn(
            self._get_target(account2.dispatch_orphan_naks.ghid),
            privateer2
        )
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()
