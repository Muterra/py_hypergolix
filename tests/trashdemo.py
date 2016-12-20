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
import argparse
import unittest
import sys
import time
import statistics
import collections
import threading
import random
import pathlib
import tempfile
import shutil
import logging

from loopa.utils import await_coroutine_threadsafe

from hypergolix.utils import ApiID
from hypergolix.objproxy import Obj

from hypergolix.comms import WSConnection
from hypergolix.service import RemotePersistenceServer
from hypergolix.app import HypergolixCore
from hypergolix.accounting import Account
from hypergolix.embed import HGXLink

from golix._getlow import GIDC
from hypergolix.persistence import _GidcLite

# ###############################################
# Fixtures
# ###############################################

from trashtest._fixtures.identities import TEST_AGENT1
from trashtest._fixtures.identities import TEST_READER1
from trashtest._fixtures.identities import TEST_AGENT2
from trashtest._fixtures.identities import TEST_READER2

gidc1 = TEST_READER1.packed
gidclite1 = _GidcLite.from_golix(GIDC.unpack(TEST_READER1.packed))

gidc2 = TEST_READER2.packed
gidclite2 = _GidcLite.from_golix(GIDC.unpack(TEST_READER2.packed))

logger = logging.getLogger(__name__)
    

# ###############################################
# Testing
# ###############################################


class TestAppNoRestore(unittest.TestCase):
    ''' Test a fake application with no account restoration, just with
    a parrot between two identities.
    '''
    
    @classmethod
    def setUpClass(cls):
        ''' Make a fake application, yo.
        '''
        # Set up the SERVER
        ###########################################
        cls.server_cachedir = tempfile.mkdtemp()
        cls.server = RemotePersistenceServer(
            cache_dir = cls.server_cachedir,
            host = '127.0.0.1',
            port = 6022,
            reusable_loop = False,
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'pserver'}
        )
        
        # Set up the FIRST CLIENT
        ###########################################
        cls.hgxcore1_cachedir = tempfile.mkdtemp()
        cls.hgxcore1 = HypergolixCore(
            cache_dir = cls.hgxcore1_cachedir,
            ipc_port = 6023,
            reusable_loop = False,
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'hgxcore1'}
        )
        cls.hgxcore1.add_remote(
            connection_cls = WSConnection,
            host = '127.0.0.1',
            port = 6022,
            tls = False
        )
        cls.root_secret_1 = TEST_AGENT1.new_secret()
        cls.account1 = Account(
            user_id = TEST_AGENT1,
            root_secret = cls.root_secret_1,
            hgxcore = cls.hgxcore1
        )
        cls.hgxcore1.account = cls.account1
        cls.hgxlink1 = HGXLink(
            ipc_port = 6023,
            autostart = False,
            # debug = True,
            threaded = True,
            thread_kwargs = {'name': 'hgxlink1'}
        )
        
        # Set up the SECOND CLIENT
        ###########################################
        cls.hgxcore2_cachedir = tempfile.mkdtemp()
        cls.hgxcore2 = HypergolixCore(
            cache_dir = cls.hgxcore2_cachedir,
            ipc_port = 6024,
            reusable_loop = False,
            threaded = True,
            # debug = True,
            thread_kwargs = {'name': 'hgxcore2'}
        )
        cls.hgxcore2.add_remote(
            connection_cls = WSConnection,
            host = '127.0.0.1',
            port = 6022,
            tls = False
        )
        cls.root_secret_2 = TEST_AGENT2.new_secret()
        cls.account2 = Account(
            user_id = TEST_AGENT2,
            root_secret = cls.root_secret_2,
            hgxcore = cls.hgxcore2
        )
        cls.hgxcore2.account = cls.account2
        cls.hgxlink2 = HGXLink(
            ipc_port = 6024,
            autostart = False,
            # debug = True,
            threaded = True,
            thread_kwargs = {'name': 'hgxlink2'}
        )
        
        # START THE WHOLE SHEBANG
        ###########################################
        cls.server.start()
        cls.hgxcore1.start()
        cls.hgxcore2.start()
        cls.hgxlink1.start()
        cls.hgxlink2.start()
    
    @classmethod
    def tearDownClass(cls):
        ''' Kill errything and then remove the caches.
        '''
        try:
            cls.hgxlink2.stop_threadsafe(timeout=.5)
            cls.hgxlink1.stop_threadsafe(timeout=.5)
            cls.hgxcore2.stop_threadsafe(timeout=.5)
            cls.hgxcore1.stop_threadsafe(timeout=.5)
            cls.server.stop_threadsafe(timeout=.5)
        
        finally:
            shutil.rmtree(cls.hgxcore2_cachedir)
            shutil.rmtree(cls.hgxcore1_cachedir)
            shutil.rmtree(cls.server_cachedir)
            
    def setUp(self):
        ''' Do some housekeeping.
        '''
        self.iterations = 10
        self.timeout = 10
        
        self.request_api = ApiID(bytes(63) + b'\x01')
        self.response_api = ApiID(bytes(63) + b'\x02')
        
        self.incoming1 = collections.deque()
        self.incoming2 = collections.deque()
        self.cache2 = collections.deque()
        
        self.returnflag1 = threading.Event()
        self.updateflags = collections.deque()
        
        # Set up the timing recorder
        self.timers = collections.deque()
        
    async def roundtrip_notifier(self, mirror_obj):
        ''' This gets called when we get an update for a response.
        '''
        end_time = time.monotonic()
        ii = int.from_bytes(mirror_obj.state[:1], 'big')
        self.timers[ii].appendleft(end_time)
        self.updateflags[ii].set()
        
    def share_handler(self, ghid, origin, api_id):
        ''' This handles all shares. It's defined to be used STRICTLY in
        one direction.
        '''
        # The request handler. Requests are only received by hgxlink2.
        if api_id == self.request_api:
            # Get the object itself
            obj = self.hgxlink2.get_threadsafe(
                cls = Obj,
                ghid = ghid
            )
            # Construct a mirror object
            mirror = self.hgxlink2.new_threadsafe(
                cls = Obj,
                state = obj.state,
                api_id = self.response_api,
                dynamic = True,
                private = False
            )
            
            # Create an update callback
            async def state_mirror(source_obj, mirror_obj=mirror):
                mirror_obj.state = source_obj.state
                await mirror_obj.push()
            
            # Set the update callback and then share the mirror
            obj.callback = state_mirror
            self.incoming2.appendleft(obj)
            self.cache2.appendleft(mirror)
            mirror.share_threadsafe(origin)
            
        # The response handler. Responses are only received by hgxlink1.
        elif api_id == self.response_api:
            # Get the object itself
            mirror = self.hgxlink1.get_threadsafe(
                cls = Obj,
                ghid = ghid
            )
            mirror.callback = self.roundtrip_notifier
            self.incoming1.appendleft(mirror)
            self.returnflag1.set()
            
        else:
            raise ValueError('Bad api.')
            
    def test_whoami(self):
        ''' Super simple whoami test to make sure it's working.
        '''
        # First make sure everything is correctly started up.
        await_coroutine_threadsafe(
            coro = self.hgxcore1.await_startup(),
            loop = self.hgxcore1._loop
        )
        await_coroutine_threadsafe(
            coro = self.hgxcore2.await_startup(),
            loop = self.hgxcore2._loop
        )
        
        whoami = await_coroutine_threadsafe(
            coro = self.hgxlink1._ipc_manager.get_whoami(timeout=5),
            loop = self.hgxlink1._loop
        )
        self.assertEqual(whoami, self.hgxlink1.whoami)
        self.assertEqual(whoami, TEST_AGENT1.ghid)
        
        whoami2 = await_coroutine_threadsafe(
            coro = self.hgxlink2._ipc_manager.get_whoami(timeout=5),
            loop = self.hgxlink2._loop
        )
        self.assertEqual(whoami2, self.hgxlink2.whoami)
        self.assertEqual(whoami2, TEST_AGENT2.ghid)
            
    def test_roundtrip(self):
        ''' Bidirectional communication test.
        '''
        # First make sure everything is correctly started up.
        await_coroutine_threadsafe(
            coro = self.hgxcore1.await_startup(),
            loop = self.hgxcore1._loop
        )
        await_coroutine_threadsafe(
            coro = self.hgxcore2.await_startup(),
            loop = self.hgxcore2._loop
        )
        
        # First we need to wrap the share handler appropriately
        handler1 = self.hgxlink1.wrap_threadsafe(self.share_handler)
        handler2 = self.hgxlink2.wrap_threadsafe(self.share_handler)
        
        # Then we need to actually register it with the respective links
        self.hgxlink1.register_share_handler_threadsafe(
            self.response_api,
            handler1
        )
        self.hgxlink2.register_share_handler_threadsafe(
            self.request_api,
            handler2
        )
        
        # Now let's make the actual request, then share is
        state = bytes([random.randint(0, 255) for i in range(0, 25)])
        request = self.hgxlink1.new_threadsafe(
            cls = Obj,
            state = state,
            api_id = self.request_api,
            dynamic = True,
            private = False
        )
        request.share_threadsafe(self.hgxlink2.whoami)
        
        # Wait for a response. First make sure one comes, then that it matches
        self.assertTrue(self.returnflag1.wait(30))
        mirror = self.incoming1.pop()
        self.assertEqual(request.state, mirror.state)
        
        # Notify that we're starting the actual tests
        logger.info(
            '\n\n########################################################\n' +
            '######### Handshakes complete! Starting tests. #########\n' +
            '########################################################\n'
        )
            
        for ii in range(self.iterations):
            with self.subTest(i=ii):
                logger.info(
                    '\n' +
                    '################ Starting mirror cycle. ################'
                )
                
                # Prep the object with an update
                state = ii.to_bytes(1, 'big') + \
                    bytes([random.randint(0, 255) for i in range(0, 25)])
                request.state = state
                
                # Clear the update flag and zero out the timer
                self.updateflags.append(threading.Event())
                self.timers.append(collections.deque([0, 0], maxlen=2))
                self.timers[ii].appendleft(time.monotonic())
                
                # Call an update, wait for the response, and record the time
                request.push_threadsafe()
                success = self.updateflags[ii].wait(self.timeout)
                
                # Check for success
                self.assertTrue(success)
                self.assertEqual(mirror.state, state)
                
        times = [end - start for end, start in self.timers]
        print('Max time: ', max(times))
        print('Min time: ', min(times))
        print('Mean time:', statistics.mean(times))
        print('Med time: ', statistics.median(times))
    

# ###############################################
# Operations
# ###############################################
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=30):
    #     unittest.main()
    unittest.main()
