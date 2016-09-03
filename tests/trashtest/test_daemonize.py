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
        raise NotImplementedError()
        
    def test_autoclose_fs(self):
        ''' Test auto-closing files. Platform-independent.
        '''
        raise NotImplementedError()
        
    def test_flush_stds(self):
        ''' Test flushing stds. Platform-independent.
        '''
        raise NotImplementedError()
        
    def test_redirect_stds(self):
        ''' Test redirecting stds. Platform-independent.
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