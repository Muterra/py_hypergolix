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

from hypergolix.comms import WSConnection
from hypergolix.service import RemotePersistenceServer
from hypergolix.app import HypergolixCore
from hypergolix.accounting import Account
from hypergolix.config import Config
from hypergolix.embed import HGXLink
from hypergolix.utils import Aengel

# from hypergolix.objproxy import ProxyBase

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


def make_fixtures(debug, hgx_root_1, hgx_root_2):
    ''' Makes fixtures for the test.
    hgx_root_# is the root app directory, used by config. It contains
    the cache directory.
    '''
    server_port = 6022
    aengel = Aengel()
    
    with Config(hgx_root_1) as config:
        config.set_remote('127.0.0.1', server_port, False)
        config.ipc_port = 6023
        
    with Config(hgx_root_2) as config:
        config.set_remote('127.0.0.1', server_port, False)
        config.ipc_port = 6024
        
    hgxserver = _hgx_server(
        host = '127.0.0.1',
        port = server_port,
        cache_dir = None,
        debug = debug,
        traceur = False,
        aengel = aengel,
    )
    # localhost:6023, no tls
    hgxraz = app_core(
        user_id = None,
        password = 'hello world',
        startup_logger = None,
        aengel = aengel,
        _scrypt_hardness = 1024,
        hgx_root = hgx_root_1,
        enable_logs = False
    )
        
    # localhost:6024, no tls
    hgxdes = app_core(
        user_id = None,
        password = 'hello world',
        startup_logger = None,
        aengel = aengel,
        _scrypt_hardness = 1024,
        hgx_root = hgx_root_2,
        enable_logs = False
    )
        
    return hgxserver, hgxraz, hgxdes, aengel
    

# ###############################################
# Testing
# ###############################################


class TestAppNoRestore(unittest.TestCase):
    ''' Test a fake application with no account restoration, just with
    a parrot between two identities.
    '''
    
    def setUp(self):
        ''' Make a fake application, yo.
        '''
        # Set up the SERVER
        ###########################################
        self.server_cachedir = tempfile.mkdtemp()
        self.server = RemotePersistenceServer(
            cache_dir = self.server_cachedir,
            host = '127.0.0.1',
            port = 6022,
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'server'}
        )
        
        # Set up the FIRST CLIENT
        ###########################################
        self.hgxcore1_cachedir = tempfile.mkdtemp()
        self.hgxcore1 = HypergolixCore(
            cache_dir = self.hgxcore1_cachedir,
            ipc_port = 6023,
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'hgxcore1'}
        )
        self.hgxcore1.add_remote(
            connection_cls = WSConnection,
            host = '127.0.0.1',
            port = 6022,
            tls = False
        )
        self.root_secret_1 = TEST_AGENT1.new_secret()
        self.account1 = Account(
            user_id = TEST_AGENT1,
            root_secret = self.root_secret_1,
            hgxcore = self.hgxcore1
        )
        self.hgxcore1.account = self.account1
        self.hgxlink1 = HGXLink(
            ipc_port = 6023,
            autostart = False,
            debug = True,
            threaded = True
        )
        
        # Set up the SECOND CLIENT
        ###########################################
        self.hgxcore2_cachedir = tempfile.mkdtemp()
        self.hgxcore2 = HypergolixCore(
            cache_dir = self.hgxcore2_cachedir,
            ipc_port = 6025,
            reusable_loop = False,
            threaded = True,
            debug = True,
            thread_kwargs = {'name': 'hgxcore2'}
        )
        self.hgxcore2.add_remote(
            connection_cls = WSConnection,
            host = '127.0.0.1',
            port = 6022,
            tls = False
        )
        self.root_secret_2 = TEST_AGENT2.new_secret()
        self.account2 = Account(
            user_id = TEST_AGENT2,
            root_secret = self.root_secret_2,
            hgxcore = self.hgxcore2
        )
        self.hgxcore2.account = self.account2
        self.hgxlink2 = HGXLink(
            ipc_port = 6025,
            autostart = False,
            debug = True,
            threaded = True
        )
        
        # START THE WHOLE SHEBANG
        ###########################################
        self.server.start()
        self.hgxcore1.start()
        self.hgxlink1.start()
        self.hgxcore2.start()
        self.hgxlink2.start()
        
    def tearDown(self):
        ''' Kill errything and then remove the caches.
        '''
        try:
            self.hgxlink2.stop_threadsafe(timeout=.5)
            self.hgxcore2.stop_threadsafe(timeout=.5)
            self.hgxlink1.stop_threadsafe(timeout=.5)
            self.hgxcore1.stop_threadsafe(timeout=.5)
            self.server.stop_threadsafe(timeout=.5)
        
        finally:
            print('Stopped. Removing cache directories.')
            shutil.rmtree(self.hgxcore2_cachedir)
            shutil.rmtree(self.hgxcore1_cachedir)
            shutil.rmtree(self.server_cachedir)
            
    def test_whoami(self):
        ''' Super simple whoami test to make sure it's working.
        '''
        whoami = await_coroutine_threadsafe(
            coro = self.hgxlink1._ipc_manager.get_whoami(timeout=1),
            loop = self.hgxlink1._loop
        )
        self.assertEqual(whoami, self.hgxlink1.whoami)
        self.assertEqual(whoami, TEST_AGENT1.ghid)
        
        whoami2 = await_coroutine_threadsafe(
            coro = self.hgxlink2._ipc_manager.get_whoami(timeout=1),
            loop = self.hgxlink2._loop
        )
        self.assertEqual(whoami2, self.hgxlink2.whoami)
        self.assertEqual(whoami2, TEST_AGENT2.ghid)


def make_tests(iterations, debug, raz, des, aengel):
    timer = collections.deque([0,0], maxlen=2)

    # Declare API
    request_api = bytes(64) + b'\x01'
    response_api = bytes(64) + b'\x02'

    # Create an object collection
    requests_outgoing = collections.deque(maxlen=10)
    requests_incoming = collections.deque(maxlen=10)
    responses_incoming = collections.deque(maxlen=10)
    responses_outgoing = collections.deque(maxlen=10)
    
    # This all handles round-trip responsing.
    roundtrip_flag = threading.Event()
    roundtrip_check = collections.deque()
    def rt_notifier(obj):
        timer.appendleft(time.monotonic())
        roundtrip_flag.set()
        roundtrip_check.append(obj._proxy_3141592)
    def rt_waiter(timeout=1.5):
        result = roundtrip_flag.wait(timeout)
        roundtrip_flag.clear()
        return result
    def rt_checker(msg):
        return roundtrip_check.pop() == msg
        
    
    class DemoReplicatorTest(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.timer = timer
            
            cls.razlink = HGXLink(
                ipc_port = 6023,
                debug = debug,
                aengel = aengel
            )
            cls.deslink = HGXLink(
                ipc_port = 6024,
                debug = debug,
                aengel = aengel
            )
            
            cls.raz = cls.razlink.whoami
            cls.des = cls.deslink.whoami
            
        def setUp(self):
            self.assertNotEqual(self.raz, self.des)
            self.razlink.get_new_token_threadsafe()
            self.deslink.get_new_token_threadsafe()
    
        # All requests go from Raz -> Des
        def make_request(self, msg):
            obj = self.razlink.new_threadsafe(
                cls = ProxyBase,
                state = msg,
                dynamic = True,
                api_id = request_api
            )
            time.sleep(.1)
            obj.hgx_share_threadsafe(self.des)
            return obj
        
        # All responses go from Des -> Raz
        def request_handler(self, obj):
            # print('Receiving request.')
            # Just to prevent GC
            requests_incoming.appendleft(obj)
            reply = self.deslink.new_threadsafe(
                cls = ProxyBase,
                state = obj,
                dynamic = True,
                api_id = response_api
            )
            
            def state_mirror(source_obj):
                # print('Mirroring state.')
                reply.hgx_state = source_obj
                reply.hgx_push_threadsafe()
            obj.hgx_register_callback_threadsafe(state_mirror)
            
            reply.hgx_share_threadsafe(recipient=self.raz)
            
            # Just to prevent GC
            responses_outgoing.appendleft(reply)
            
        # All requests go from Raz -> Des. All responses go from Des -> Raz.
        def response_handler(self, obj):
            # print('Receiving response.')
            obj.hgx_register_callback_threadsafe(rt_notifier)
            # Just to prevent GC
            responses_incoming.appendleft(obj)
            
        def test_app(self):
            ''' Yknow, pretend to make an app and shit. Basically, an
            automated version of echo-101/demo-4.py
            '''
            self.razlink.register_share_handler_threadsafe(
                response_api,
                ProxyBase,
                self.response_handler
            )
            self.deslink.register_share_handler_threadsafe(
                request_api,
                ProxyBase,
                self.request_handler
            )
            
            msg = b'hello'
            obj = self.make_request(msg)
            
            time.sleep(1.5)
            times = []
            
            logging.getLogger('').critical(
                '########## Handshakes complete! Starting tests. ##########'
            )
            
            for ii in range(iterations):
                with self.subTest(i=ii):
                    msg = ''.join(
                        [chr(random.randint(0, 255)) for i in range(0, 25)]
                    )
                    msg = msg.encode('utf-8')
                    
                    # Prep the object with an update
                    obj.hgx_state = msg
                    
                    # Zero out the timer
                    self.timer.extendleft([0, 0, time.monotonic()])
                    
                    # Call an update
                    obj.hgx_push_threadsafe()
                    # Wait for response
                    success = rt_waiter()
                    # Stop the timer
                    times.append(self.timer[0] - self.timer[1])
                    
                    # Check for success
                    self.assertTrue(success)
                    self.assertTrue(rt_checker(msg))
                    
                    # Max update frequencies can cause problems yo
                    time.sleep(.1)
                    
            print('Max time: ', max(times))
            print('Min time: ', min(times))
            print('Mean time:', statistics.mean(times))
            print('Med time: ', statistics.median(times))
    
    return DemoReplicatorTest
    

# ###############################################
# Operations
# ###############################################


def ingest_args():
    ''' Register argparser and parse (and return) args.
    '''
    parser = argparse.ArgumentParser(description='Hypergolix trashtest.')
    parser.add_argument(
        '--hgxroot',
        action = 'store',
        default = None,
        nargs = 2,
        help = 'Specify the root directories for the two configurations.'
    )
    parser.add_argument(
        '--iters',
        action = 'store',
        default = 10,
        type = int,
        help = 'How many iterations to run?',
    )
    parser.add_argument(
        '--debug',
        action = 'store_true',
        help = 'Set debug mode. Automatically sets verbosity to debug.'
    )
    parser.add_argument(
        '--logdir',
        action = 'store',
        default = None,
        type = str,
        help = 'Log to a specified directory, relative to current path.',
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
        action = 'store',
        default = None,
        type = float,
        help = 'Set traceur mode, using the passed float as a stack tracing '
               'interval for deadlock detection. Must be a positive number, '
               'or it will be ignored.'
    )
    parser.add_argument('unittest_args', nargs='*')
    args = parser.parse_args()
    
    return args
    
    
def configure_unified_logger(args):
    ''' If we want to run a single unified logger for the entire test,
    set that up here. Note that these logs will be in addition to those
    created naturally during hypergolix operation (which will live
    within each app's respective hgx_root)
    '''
    from hypergolix import logutils
    
    if args.logdir:
        logutils.autoconfig(
            tofile = True,
            logdirname = args.logdir,
            loglevel = args.verbosity
        )
    else:
        logutils.autoconfig(
            tofile = False,
            loglevel = args.verbosity
        )

                
if False:
    args = ingest_args()
    # Dammit unittest using argparse
    sys.argv[1:] = args.unittest_args
    
    # Configure logs first so any fixturing errors are handled.
    configure_unified_logger(args)
    
    # This means this run is being used to generate test vectors, and not as a
    # disposable run.
    if args.hgxroot is not None:
        cleanup_test_files = False
        hgx_root_a = args.hgxroot[0]
        hgx_root_b = args.hgxroot[1]
        
        if not pathlib.Path(hgx_root_a).exists():
            raise ValueError('Bad path for first HGX root.')
        elif not pathlib.Path(hgx_root_b).exists():
            raise ValueError('Bad path for second HGX root.')
            
    else:
        cleanup_test_files = True
        hgx_root_a = tempfile.mkdtemp()
        hgx_root_b = tempfile.mkdtemp()
    
    try:
        def do_test():
            # Okay, let's set up the tests
            server, raz, des, aengel = make_fixtures(
                args.debug,
                hgx_root_a,
                hgx_root_b
            )
            apptest = make_tests(args.iters, args.debug, raz, des, aengel)
            
            # And finally, run them
            suite = unittest.TestSuite()
            suite.addTest(apptest('test_app'))
            unittest.TextTestRunner().run(suite)
        
            logging.getLogger('').critical(
                '########## Test suite complete; closing down. ##########'
            )
        
            raz[1].wait_close_safe()
            des[1].wait_close_safe()
        
        # Clip negative numbers
        if args.traceur is not None:
            trace_interval = max([args.traceur, 0.1])
            print('Running with trace.')
            from hypergolix.utils import TraceLogger
            with TraceLogger(trace_interval):
                do_test()
        else:
            do_test()
    
    # Clean up any disposable files
    finally:
        if cleanup_test_files:
            # Wait a wee bit before attempting cleanup, so that everything can
            # exit
            time.sleep(1)
            shutil.rmtree(hgx_root_a)
            shutil.rmtree(hgx_root_b)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig(loglevel='debug')
    
    from hypergolix.utils import TraceLogger
    with TraceLogger(interval=10):
        unittest.main()
    # unittest.main()
