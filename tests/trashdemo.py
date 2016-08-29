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


import IPython
import warnings

from hypergolix.service import _hgx_server
from hypergolix.app import HGXService
from hypergolix import HGXLink
from hypergolix.utils import Aengel

from hypergolix.objproxy import ProxyBase

from golix import Ghid

# ###############################################
# Fixtures
# ###############################################


def make_fixtures(debug):
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
        _scrypt_hardness = 1024,
        password = 'hello world',
        aengel = aengel,
        cache_dir = './trashtest/_vectors/hgx_save_a',
    )
    hgxdes = HGXService(
        host = 'localhost',
        port = 6022,
        tls = False,
        ipc_port = 6024,
        debug = debug,
        traceur = False,
        foreground = False,
        _scrypt_hardness = 1024,
        password = 'hello world',
        aengel = aengel,
        cache_dir = './trashtest/_vectors/hgx_save_b',
    )
        
    return hgxserver, hgxraz, hgxdes, aengel
    

# ###############################################
# Testing
# ###############################################


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
            # self.razlink.register_api_threadsafe(request_api, self.request_handler)
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
            # self.deslink.register_api_threadsafe(response_api, self.response_handler)
            
            msg = b'hello'
            obj = self.make_request(msg)
            
            time.sleep(1.5)
            times = []
            
            for ii in range(iterations):
                with self.subTest(i=ii):
                    msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
                    msg = msg.encode('utf-8')
                    
                    # Prep the object with an update
                    obj.hgx_state = msg
                    
                    # Zero out the timer
                    self.timer.extendleft([0,0,time.monotonic()])
                    
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
    
    def do_test():
        # Okay, let's set up the tests
        server, raz, des, aengel = make_fixtures(args.debug)    
        apptest = make_tests(args.iters, args.debug, raz, des, aengel)
        
        # And finally, run them
        suite = unittest.TestSuite()
        suite.addTest(apptest('test_app'))
        unittest.TextTestRunner().run(suite)
    
    # Clip negative numbers
    trace_interval = max([args.traceur, 0])
    if trace_interval:
        from hypergolix.utils import TraceLogger
        with TraceLogger(trace_interval):
            do_test()
    else:
        do_test()