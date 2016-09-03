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
import logging
import tempfile
import sys
import os

from hypergolix.utils import platform_specificker

from hypergolix._daemonize import daemonize
from hypergolix._daemonize import _lock_pidfile
from hypergolix._daemonize import _write_pid
from hypergolix._daemonize import _fratricidal_fork
from hypergolix._daemonize import _filial_usurpation
from hypergolix._daemonize import _make_range_tuples
from hypergolix._daemonize import _autoclose_files
from hypergolix._daemonize import _flush_stds
from hypergolix._daemonize import _redirect_stds


# ###############################################
# "Paragon of adequacy" test fixtures
# ###############################################


# Nothing to see here
# These are not the droids you are looking for
# etc


# ###############################################
# Testing
# ###############################################
        
        
class Deamonizing_test(unittest.TestCase):
    _SUPPORTED_PLATFORM = platform_specificker(
        linux_choice = True,
        win_choice = False,
        cygwin_choice = False,
        osx_choice = True,
        # Dunno if this is a good idea but might as well try
        other_choice = True
    )
        
    def test_make_ranges(self):
        ''' Test making appropriate ranges for file auto-closure. 
        Platform-independent.
        '''
        # This would be better done with hypothesis, but that can come later.
        argsets = []
        expected_results = []
        
        argsets.append(
            (0, 5, [])
        )
        expected_results.append(
            [
                (0, 5),
            ]
        )
        
        argsets.append(
            (3, 10, [1, 2])
        )
        expected_results.append(
            [
                (3, 10),
            ]
        )
        
        argsets.append(
            (3, 7, [4,])
        )
        expected_results.append(
            [
                (3, 4),
                (5, 7),
            ]
        )
        
        argsets.append(
            (3, 14, [4, 5, 10])
        )
        expected_results.append(
            [
                (3, 4),
                (6, 10),
                (11, 14),
            ]
        )
        
        argsets.append(
            (1, 3, [1, 2, 3])
        )
        expected_results.append(
            [
            ]
        )
        
        for argset, expected_result in zip(argsets, expected_results):
            with self.subTest(argset):
                actual_result = _make_range_tuples(*argset)
                self.assertEqual(actual_result, expected_result)
        
    def test_flush_stds(self):
        ''' Test flushing stds. Platform-independent.
        '''
        # Should this do any kind of verification or summat?
        _flush_stds()
        
    def test_redirect_stds(self):
        ''' Test redirecting stds. Platform-independent.
        '''
        stdin = sys.stdin
        stdout = sys.stdout
        stderr = sys.stderr
        
        # Get some file descriptors to use to cache stds
        with tempfile.NamedTemporaryFile() as stdin_tmp, \
            tempfile.NamedTemporaryFile() as stdout_tmp, \
            tempfile.NamedTemporaryFile() as stderr_tmp:
                stdin_fd = stdin_tmp.fileno()
                stdout_fd = stdout_tmp.fileno()
                stderr_fd = stderr_tmp.fileno()
                
        os.dup2(0, stdin_fd)
        os.dup2(1, stdout_fd)
        os.dup2(2, stderr_fd)
        
        # Perform the actual tests
        with tempfile.TemporaryDirectory() as dirname:
            try:
                with self.subTest('Separate streams'):
                    _redirect_stds(
                        dirname + '/stdin.txt',
                        dirname + '/stdout.txt',
                        dirname + '/stderr.txt'
                    )
                
                with self.subTest('Shared streams'):
                    _redirect_stds(
                        dirname + '/stdin2.txt',
                        dirname + '/stdshr.txt',
                        dirname + '/stdshr.txt'
                    )
                
                with self.subTest('Combined streams'):
                    _redirect_stds(
                        dirname + '/stdcomb.txt',
                        dirname + '/stdcomb.txt',
                        dirname + '/stdcomb.txt'
                    )
        
            # Restore our original stdin, stdout, stderr. Do this before dir
            # cleanup or we'll get cleanup errors.
            finally:
                os.dup2(stdin_fd, 0)
                os.dup2(stdout_fd, 1)
                os.dup2(stderr_fd, 2)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_autoclose_fs(self):
        ''' Test auto-closing files. Platform-specific.
        '''
        raise NotImplementedError()
    
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_lock_pidfile(self):
        ''' Test that locking the pidfile worked. Platform-specific.
        '''
        raise NotImplementedError()
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_write_pid(self):
        ''' Test that writing the pid to the pidfile worked. Platform-
        specific.
        '''
        raise NotImplementedError()
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_frat_fork(self):
        ''' Test "fratricidal" (okay, parricidal) forking (fork and 
        parent dies). Platform-specific.
        '''
        raise NotImplementedError()
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_filial_usurp(self):
        ''' Test decoupling child from parent environment. Platform-
        specific.
        '''
        raise NotImplementedError()
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_daemonize(self):
        ''' Test daemonization. Platform-specific.
        '''
        raise NotImplementedError()
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()