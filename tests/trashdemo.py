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
import collections
import threading
import random


import IPython
import warnings

from hypergolix.service import _hgx_server
from hypergolix.service import main as hgxservice
from hypergolix.service import HypergolixLink

from golix import Ghid

# ###############################################
# Fixtures
# ###############################################


def make_fixtures(debug, verbosity, log_prefix):
    if log_prefix:
        fname_raz = log_prefix + '_raz.log'
        fname_des = log_prefix + '_des.log'
        fname_server = log_prefix + '_server.log'
        
    else:
        fname_raz = None
        fname_des = None
        fname_server = None
        
    hgxserver = _hgx_server(
        host = 'localhost',
        port = 6022,
        debug = debug,
        verbosity = verbosity,
        logfile = fname_server,
        traceur = False,
        foreground = False,
    )
    hgxraz = hgxservice(
        host = 'localhost',
        port = 6022,
        ipc_port = 6023,
        debug = debug,
        verbosity = verbosity,
        logfile = fname_raz,
        traceur = False,
        foreground = False,
    )
    hgxdes = hgxservice(
        host = 'localhost',
        port = 6022,
        ipc_port = 6024,
        debug = debug,
        verbosity = verbosity,
        logfile = fname_des,
        traceur = False,
        foreground = False,
    )
        
    return hgxserver, hgxraz, hgxdes
    

# ###############################################
# Testing
# ###############################################


def make_tests(iterations, debug, raz, des):

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
        roundtrip_flag.set()
        roundtrip_check.append(obj.state)
    def rt_waiter(timeout=1):
        result = roundtrip_flag.wait(timeout)
        roundtrip_flag.clear()
        return result
    def rt_checker(msg):
        return roundtrip_check.pop() == msg
        
    
    class DemoReplicatorTest(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.razlink = HypergolixLink(ipc_port=6023, debug=debug)
            cls.deslink = HypergolixLink(ipc_port=6024, debug=debug)
            
            cls.raz = cls.razlink.whoami_threadsafe()
            cls.des = cls.deslink.whoami_threadsafe()
            
        def setUp(self):
            self.razlink.new_token_threadsafe()
            self.deslink.new_token_threadsafe()
    
        # All requests go from Raz -> Des
        def make_request(self, msg):
            obj = self.razlink.new_obj_threadsafe(
                state = msg,
                dynamic = True,
                api_id = request_api
            )
            obj.share_threadsafe(self.des)
            return obj
        
        # All requests go from Raz -> Des
        def request_handler(self, obj):
            # Just to prevent GC
            requests_incoming.appendleft(obj)
            reply = self.deslink.new_obj_threadsafe(
                state = obj.state,
                dynamic = True,
                api_id = response_api
            )
            reply.share_threadsafe(recipient=self.raz)
            # Just to prevent GC
            responses_outgoing.appendleft(reply)
            
            def state_mirror(source_obj):
                reply.update_threadsafe(source_obj.state)
            obj.append_threadsafe_callback(state_mirror)
            
        # All requests go from Raz -> Des. All responses go from Des -> Raz.
        def response_handler(self, obj):
            obj.append_threadsafe_callback(rt_notifier)
            # Just to prevent GC
            responses_incoming.appendleft(obj)
            
        def test_app(self):
            ''' Yknow, pretend to make an app and shit. Basically, an 
            automated version of echo-101/demo-4.py
            '''
            self.razlink.register_api_threadsafe(request_api, self.request_handler)
            self.razlink.register_api_threadsafe(response_api, self.response_handler)
            self.deslink.register_api_threadsafe(request_api, self.request_handler)
            self.deslink.register_api_threadsafe(response_api, self.response_handler)
            
            msg = b'hello'
            obj = self.make_request(msg)
            
            time.sleep(1)
            
            for ii in range(iterations):
                with self.subTest(i=ii):
                    msg = ''.join([chr(random.randint(0,255)) for i in range(0,25)])
                    msg = msg.encode('utf-8')
                    obj.update_threadsafe(msg)
                    self.assertTrue(rt_waiter())
                    self.assertTrue(rt_checker(msg))
                    
    
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
        '--logfile', 
        action = 'store',
        default = None, 
        type = str,
        help = 'Log to a specified file, relative to current directory.',
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
    parser.add_argument('unittest_args', nargs='*')
    args = parser.parse_args()
    
    # Dammit unittest using argparse
    sys.argv[1:] = args.unittest_args
    
    # Okay, let's set up the tests
    server, raz, des = make_fixtures(args.debug, args.verbosity, args.logfile)    
    apptest = make_tests(args.iters, args.debug, raz, des)
    
    # And finally, run them
    suite = unittest.TestSuite()
    suite.addTest(apptest('test_app'))
    unittest.TextTestRunner().run(suite)