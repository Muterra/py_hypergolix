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
import time
import multiprocessing

from hypergolix._daemonize import daemonize
from hypergolix._daemonize import _SUPPORTED_PLATFORM
from hypergolix._daemonize import _acquire_pidfile
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
def test_daemon(pid_file, token, response_q, ct_exit):
    ''' The test daemon quite simply daemonizes itself, does some stuff 
    to confirm its existence, waits for a signal to die, and then dies.
    '''
    # Daemonize ourselves
    daemonize(pid_file)
    # Warte mal, just because.
    time.sleep(.14)
    # Put the token into the queue.
    response_q.put(token)
    # Wait for a clear to exit
    ct_exit.wait(timeout=60)


# ###############################################
# Testing
# ###############################################
        
        
class Deamonizing_test(unittest.TestCase):
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
        
    def test_write_pid(self):
        ''' Test that writing the pid to the pidfile worked. Platform-
        specific.
        '''
        pid = str(os.getpid())
        # Test new file
        with tempfile.TemporaryFile('w+') as fp:
            _write_pid(fp)
            fp.seek(0)
            self.assertEqual(fp.read(), pid + '\n')
            
        # Test existing file
        with tempfile.TemporaryFile('w+') as fp:
            fp.write('hello world, overwrite me!')
            _write_pid(fp)
            fp.seek(0)
            self.assertEqual(fp.read(), pid + '\n')
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_autoclose_fs(self):
        ''' Test auto-closing files. Platform-specific.
        '''
        num_files = 14
        fs = []
        shielded_fs = []
        shielded_fds = []
        keepers = [0, 7, 13]
        for ii in range(num_files):
            if ii in keepers:
                thisf = tempfile.TemporaryFile()
                shielded_fs.append(thisf)
                shielded_fds.append(thisf.fileno())
            else:
                fds.append(tempfile.TemporaryFile())
            
        try:
            _autoclose_files(shielded=shielded_fds)
            
            for f in shielded_fs:
                with self.subTest('Persistent: ' + str(f)):
                    # Make sure all kept files were, in fact, kept
                    self.assertFalse(f.closed)
                    
            for f in fs:
                with self.subTest('Removed: ' + str(f)):
                    # Make sure all other files were, in fact, removed
                    self.assertTrue(f.closed)

            # Do it again with no files shielded from closure.                    
            _autoclose_files()
            for f in shielded_fs:
                with self.subTest('Cleanup: ' + str(f)):
                    self.assertTrue(f.closed)
        
        # Clean up any unsuccessful tests. Note idempotency of fd.close().
        finally:
            for f in fs:
                f.close()
            for f in shielded_fs:
                f.close()
    
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_acquire_file(self):
        ''' Test that locking the pidfile worked. Platform-specific.
        '''
        mp_ctx = multiprocessing.get_context('spawn')
        
        ctx1 = mp_ctx.Event()
        ctx2 = mp_ctx.Event()
        ct_exit = mp_ctx.Event()
        # parent_conn, child_conn = mp_ctx.Pipe()
        
        with tempfile.TemporaryDirectory() as dirname:
            fpath = dirname + '/testpid.txt'
            pid = os.fork()
            
            # Parent process execution
            if pid != 0:
                ctx1.wait(timeout=1)
                self.assertTrue(os.path.exists(fpath))
                with self.assertRaises(SystemExit):
                    _acquire_pidfile(fpath)
                ctx2.set()
                
            # Child process execution
            else:
                try:
                    pidfile = _acquire_pidfile(fpath)
                    ctx1.set()
                    ctx2.wait(timeout=60)
                    
                finally:
                    # Manually close the pidfile.
                    pidfile.close()
                    # Tell the parent it's safe to close.
                    ct_exit.set()
                    # Exit child without cleanup.
                    os._exit(0)
                
            # This will only happen in the parent process, due to os._exit call
            ct_exit.wait(timeout=60)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_frat_fork(self):
        ''' Test "fratricidal" (okay, parricidal) forking (fork and 
        parent dies). Platform-specific.
        '''
        mp_ctx = multiprocessing.get_context('spawn')
        
        pid_q = mp_ctx.Queue()
        ct_exit = mp_ctx.Event()
        inter_pid = os.fork()
        
        # This is the root parent.
        if inter_pid != 0:
            child_pid = pid_q.get(timeout=5)
            ct_exit.set()
            self.assertNotEqual(inter_pid, child_pid)
            
            # Wait to ensure shutdown of other process.
            time.sleep(.5)
            # Make sure the intermediate process is dead.
            with self.assertRaises(OSError):
                # Send it a signal to check existence (os.kill is badly named)
                os.kill(inter_pid, 0)

        # This is the intermediate.            
        else:
            try:
                # Fork again, killing the intermediate.
                _fratricidal_fork()
                my_pid = os.getpid()
                pid_q.put(my_pid)
                ct_exit.wait(timeout=5)
            finally:
                # Exit without cleanup.
                os._exit(0)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_filial_usurp(self):
        ''' Test decoupling child from parent environment. Platform-
        specific.
        '''
        mp_ctx = multiprocessing.get_context('spawn')
        
        umask = 0o027
        chdir = os.path.abspath('/')
        
        prop_q = mp_ctx.Queue()
        ct_exit = mp_ctx.Event()
        
        child_pid = os.fork()
        
        # This is the parent.
        if child_pid != 0:
            try:
                my_pid = os.getpid()
                my_sid = os.getsid(my_pid)
                child_sid = prop_q.get(timeout=5)
                child_umask = prop_q.get(timeout=5)
                child_wdir = prop_q.get(timeout=5)
                
                self.assertNotEqual(my_sid, child_sid)
                self.assertEqual(child_umask, umask)
                self.assertEqual(child_wdir, chdir)
            
            finally:
                ct_exit.set()
            
        # This is the child.
        else:
            try:
                _filial_usurpation(chdir, umask)
                # Get our session id
                my_pid = os.getpid()
                sid = os.getsid(my_pid)
                # reset umask and get our set one.
                umask = os.umask(0)
                # Get our working directory.
                wdir = os.path.abspath(os.getcwd())
                
                # Update parent
                prop_q.put(sid)
                prop_q.put(umask)
                prop_q.put(wdir)
                
                # Wait for the signal and then exit
                ct_exit.wait(timeout=5)
                
            finally:
                os._exit(0)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_daemonize(self):
        ''' Test daemonization. Platform-specific.
        '''
        mp_ctx = multiprocessing.get_context('spawn')
        
        with tempfile.TemporaryDirectory() as dirname:
            pid_file = dirname + '/testpid.pid'
            token = 2718282
            response_q = mp_ctx.Queue()
            ct_exit = mp_ctx.Event()
            
            p = mp_ctx.Process(target=test_daemon, args=(
                pid_file,
                token,
                response_q,
                ct_exit
            ))
            p.start()
            
            try:
                # Wait for the daemon to respond with the token we gave it
                parrotted_token = response_q.get(timeout=10)
                # Make sure the token matches
                self.assertEqual(parrotted_token, token)
                # Make sure the pid file exists
                self.assertTrue(os.path.exists(pid_file))
                
            finally:
                # Let the deamon die
                ct_exit.set()
                
            # Now hold off just a moment and then make sure the pid is cleaned
            # up successfully.
            time.sleep(1)
            self.assertFalse(os.path.exists(pid_file))
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    
    # from hypergolix.utils import TraceLogger
    # with TraceLogger(interval=10):
    #     unittest.main()
    unittest.main()