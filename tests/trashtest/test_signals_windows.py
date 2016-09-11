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

import unittest
import collections
import threading
import logging
import tempfile
import sys
import os
import time
import shutil
import pickle
import subprocess
import signal

from hypergolix._daemonize_windows import _SUPPORTED_PLATFORM
from hypergolix._daemonize_windows import IGNORE

from hypergolix._daemonize_windows import SignalHandler1
from hypergolix._daemonize_windows import send
from hypergolix._daemonize_windows import ping
from hypergolix._daemonize_windows import _sketch_raise_in_main
from hypergolix._daemonize_windows import _default_handler
from hypergolix._daemonize_windows import _noop
# No good way to test this, but it's super simple so whatever
# from hypergolix._daemonize_windows import _infinite_noop
from hypergolix._daemonize_windows import _await_signal
from hypergolix._daemonize_windows import _normalize_handler

from hypergolix.exceptions import SignalError
from hypergolix.exceptions import SIGINT
from hypergolix.exceptions import SIGTERM
from hypergolix.exceptions import SIGABRT


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


class ProcFixture:
    def __init__(self, returncode):
        self.returncode = returncode
        
    def wait(self):
        pass


# ###############################################
# Testing
# ###############################################
        
        
class Signals_test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ''' Prep for abortability.
        '''
        cls.skip_remaining = False
    
    def setUp(self):
        ''' Add a check that a test has not called for an exit, keeping
        forks from doing a bunch of nonsense.
        '''
        if self.skip_remaining:
            raise unittest.SkipTest('Internal call to skip remaining.')
    
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_raise_in_main(self):
        ''' Punch holes in the interpreter for fun and profit!
        '''
        with self.assertRaises(SignalError):
            worker = threading.Thread(
                target = _sketch_raise_in_main,
                args = (SignalError,),
                daemon = True
            )
            worker.start()
            time.sleep(.1)
    
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_default_handler(self):
        ''' Test the default signal handler.
        '''
        with self.assertRaises(SignalError):
            worker = threading.Thread(
                target = _default_handler,
                args = (-42,),
                daemon = True
            )
            worker.start()
            time.sleep(.1)
            
        with self.assertRaises(SIGABRT):
            worker = threading.Thread(
                target = _default_handler,
                args = (signal.SIGABRT,),
                daemon = True
            )
            worker.start()
            time.sleep(.1)
            
        with self.assertRaises(SIGINT):
            worker = threading.Thread(
                target = _default_handler,
                args = (signal.SIGINT,),
                daemon = True
            )
            worker.start()
            time.sleep(.1)
            
        with self.assertRaises(SIGTERM):
            worker = threading.Thread(
                target = _default_handler,
                args = (signal.SIGTERM,),
                daemon = True
            )
            worker.start()
            time.sleep(.1)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_noop(self):
        ''' Hey, it's a gimme.
        '''
        _noop(signal.SIGINT)
        _noop(signal.SIGTERM)
        _noop(signal.SIGABRT)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_signal_waiting(self):
        ''' Fixture thine self.
        '''
        proc1 = ProcFixture(signal.SIGINT)
        proc2 = ProcFixture(signal.SIGTERM)
        proc3 = ProcFixture(signal.SIGABRT)
        proc4 = ProcFixture(signal.CTRL_C_EVENT)
        proc5 = ProcFixture(signal.CTRL_BREAK_EVENT)
        
        self.assertEqual(_await_signal(proc1), signal.SIGINT)
        self.assertEqual(_await_signal(proc2), signal.SIGTERM)
        self.assertEqual(_await_signal(proc3), signal.SIGABRT)
        self.assertEqual(_await_signal(proc4), signal.SIGINT)
        self.assertEqual(_await_signal(proc5), signal.SIGINT)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_handler_normalization(self):
        ''' Convert defaults and constants to their intended targets.
        '''
        handler = lambda x: x
        
        self.assertEqual(_normalize_handler(handler), handler)
        self.assertEqual(_normalize_handler(None), _default_handler)
        self.assertEqual(_normalize_handler(IGNORE), _noop)
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()