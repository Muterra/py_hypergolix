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
import logging
import unittest
import sys
import time
import statistics
import collections
import threading
import random
import datetime
import pathlib

import cProfile


import IPython
import warnings

from hypergolix.service import _hgx_server
from hypergolix.app import HGXService
from hypergolix import HGXLink
from hypergolix.utils import Aengel

from hypergolix.objproxy import NoopProxy

from golix import Ghid

# ###############################################
# Fixtures
# ###############################################


def make_fixtures(debug, tmpdir_a, tmpdir_b):
    aengel = Aengel()
        
    hgxserver = _hgx_server(
        host = 'localhost',
        port = 6022,
        debug = debug,
        traceur = False,
        foreground = False,
        aengel = aengel,
    )
    hgxraz = HGXService(
        host = 'localhost',
        port = 6022,
        tls = False,
        ipc_port = 6023,
        debug = debug,
        traceur = False,
        foreground = False,
        aengel = aengel,
        _scrypt_hardness = 1024,
        password = 'hello world',
        cache_dir = tmpdir_a,
        user_id = Ghid.from_str('AQaNl4v6Ng6-1_4qPL964SzJWuTn1Cg4eTPbQiHSFNag18tERtO0_IztVghvko37t8_NWchVJgvoDs5Nq-Yfe_I=')
    )
    hgxdes = HGXService(
        host = 'localhost',
        port = 6022,
        tls = False,
        ipc_port = 6024,
        debug = debug,
        traceur = False,
        foreground = False,
        aengel = aengel,
        _scrypt_hardness = 1024,
        password = 'hello world',
        cache_dir = tmpdir_b,
        user_id = Ghid.from_str('Ac7UHA_FFMndtcqqtLvzg8hyJAKd4eyJHdpx4U3N8NOWGELtRHnHJ3qGCA07szpFoLt07isXwHPQFXTNRHAECRo=')
    )
        
    return hgxserver, hgxraz, hgxdes, aengel
    

# ###############################################
# Testing
# ###############################################


def make_tests(iterations, debug, raz, des, aengel):
    inner_profiler = cProfile.Profile()
    outer_profiler = cProfile.Profile()
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
    def rt_waiter(timeout=1):
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
                cls = NoopProxy,
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
                cls = NoopProxy,
                state = obj,
                dynamic = True,
                api_id = response_api
            )
            
            def state_mirror(source_obj):
                # print('Mirroring state.')
                reply.hgx_state = source_obj
                inner_profiler.enable()
                reply.hgx_push_threadsafe()
                inner_profiler.disable()
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
                NoopProxy, 
                self.response_handler
            )
            self.deslink.register_share_handler_threadsafe(
                request_api, 
                NoopProxy,
                self.request_handler
            )
            
            msg = b'hello'
            obj = self.make_request(msg)
            
            time.sleep(1.5)
            times = []
            
            outer_profiler.enable()
            for ii in range(iterations):
                with self.subTest(i=ii):
                    msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
                    msg = msg.encode('utf-8')
                    
                    # Prep the object with an update
                    obj.hgx_state = msg
                    
                    # Zero out the timer and enable the profiler
                    inner_profiler.enable()
                    self.timer.extendleft([0,0,time.monotonic()])
                    
                    # Call an update
                    obj.hgx_push_threadsafe()
                    # Wait for response
                    success = rt_waiter()
                    # Stop the timer
                    times.append(self.timer[0] - self.timer[1])
                    # Stop the profiler
                    inner_profiler.disable()
                    
                    # Check for success
                    self.assertTrue(success)
                    self.assertTrue(rt_checker(msg))
                    
                    # Max update frequencies can cause problems yo
                    time.sleep(.1)
            outer_profiler.disable()
                    
            print('---------------------------------------------------')
            print('Max time: ', max(times))
            print('Min time: ', min(times))
            print('Mean time:', statistics.mean(times))
            print('Med time: ', statistics.median(times))
            print('\n')
            
            print('---------------------------------------------------')
            print('Cropped profile')
            inner_profiler.print_stats('cumulative')
            print('\n')
            
            print('---------------------------------------------------')
            print('Wide-angle profile')
            outer_profiler.print_stats('cumulative')
            print('\n')
    
    return DemoReplicatorTest
    

# ###############################################
# Operations
# ###############################################
                
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hypergolix trashtest.')
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
        default = False,
        type = float,
        help = 'Set traceur mode, using the passed float as a stack tracing '
                'interval for deadlock detection. Must be a positive number, '
                'or it will be ignored.'
    )
    parser.add_argument('unittest_args', nargs='*')
    args = parser.parse_args()
    
    # LOGGING CONFIGURATION! Do this first so any fixturing errors get handled
    
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
    
    # Dammit unittest using argparse
    sys.argv[1:] = args.unittest_args
    
    def do_test(cache_dir_a, cache_dir_b):
        # Okay, let's set up the tests
        server, raz, des, aengel = make_fixtures(
            args.debug, 
            cache_dir_a, 
            cache_dir_b
        )    
        
        apptest = make_tests(args.iters, args.debug, raz, des, aengel)
        
        # And finally, run them
        suite = unittest.TestSuite()
        suite.addTest(apptest('test_app'))
        unittest.TextTestRunner().run(suite)
    
    # We're going to copy all of the vectors into a temporary directory, so we
    # don't accidentally mutate them.
    import tempfile
    import pathlib
    import shutil
    
    with tempfile.TemporaryDirectory() as cache_dir_a, \
        tempfile.TemporaryDirectory() as cache_dir_b:
            save_dir_a = pathlib.Path('./trashtest/_vectors/hgx_save_a')
            save_dir_b = pathlib.Path('./trashtest/_vectors/hgx_save_b')
            
            assert cache_dir_a != cache_dir_b
            
            for fpath in save_dir_a.iterdir():
                shutil.copy(fpath.as_posix(), cache_dir_a)
            
            for fpath in save_dir_b.iterdir():
                shutil.copy(fpath.as_posix(), cache_dir_b)
        
            # Clip negative numbers
            trace_interval = max([args.traceur, 0])
            if trace_interval:
                from hypergolix.utils import TraceLogger
                with TraceLogger(trace_interval):
                    do_test(cache_dir_a, cache_dir_b)
            else:
                do_test(cache_dir_a, cache_dir_b)