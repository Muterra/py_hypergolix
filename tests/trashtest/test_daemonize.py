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


MP_SPAWN = multiprocessing.get_context('spawn')


def childproc_daemon(pid_file, token, res_path):
    ''' The test daemon quite simply daemonizes itself, does some stuff 
    to confirm its existence, waits for a signal to die, and then dies.
    '''
    # Daemonize ourselves
    daemonize(pid_file)
    # Write the token to the response path
    with open(res_path, 'w') as f:
        f.write(str(token) + '\n')
        
    # Wait 1 second so that the parent can make sure our PID file exists
    time.sleep(1)
    
    
def childproc_acquire(fpath, ctx1, ctx2, ct_exit):
    ''' Child process for acquiring the pidfile.
    '''
    try:
        pidfile = _acquire_pidfile(fpath)
        ctx1.set()
        ctx2.wait(timeout=60)
        
    finally:
        # Manually close the pidfile.
        pidfile.close()
        # Tell the parent it's safe to close.
        ct_exit.set()
        
        
def childproc_fratfork(pid_q, ct_exit):
    # Fork again, killing the intermediate.
    _fratricidal_fork()
    my_pid = os.getpid()
    pid_q.put(my_pid)
    ct_exit.wait(timeout=5)

        
def childproc_filialusurp(umask, chdir, prop_q, ct_exit):
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
    def test_acquire_file(self):
        ''' Test that locking the pidfile worked. Platform-specific.
        '''
        ctx1 = MP_SPAWN.Event()
        ctx2 = MP_SPAWN.Event()
        ct_exit = MP_SPAWN.Event()
        # parent_conn, child_conn = MP_SPAWN.Pipe()
        
        with tempfile.TemporaryDirectory() as dirname:
            fpath = dirname + '/testpid.txt'
            
            # Don't use os-level forking because it screws up multiprocessing
            p = MP_SPAWN.Process(target=childproc_acquire, args=(
                fpath,
                ctx1,
                ctx2,
                ct_exit
            ))
            p.start()
            
            ctx1.wait(timeout=1)
            self.assertTrue(os.path.exists(fpath))
            with self.assertRaises(SystemExit):
                pidfile = _acquire_pidfile(fpath)
            ctx2.set()
            ct_exit.wait(timeout=60)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_filial_usurp(self):
        ''' Test decoupling child from parent environment. Platform-
        specific.
        '''
        umask = 0o027
        chdir = os.path.abspath('/')
        
        prop_q = MP_SPAWN.Queue()
        ct_exit = MP_SPAWN.Event()

        # Don't use os-level forking because it screws up multiprocessing
        p = MP_SPAWN.Process(target=childproc_filialusurp, args=(
            umask,
            chdir,
            prop_q,
            ct_exit
        ))
        p.start()
        
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
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_autoclose_fs(self):
        ''' Test auto-closing files. Platform-specific.
        '''
        all_fds = []
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
                all_fds.append(thisf.fileno())
            else:
                thisf = tempfile.TemporaryFile()
                all_fds.append(thisf.fileno())
                fs.append(thisf)
            
        try:
            _autoclose_files(shielded=shielded_fds)
            
            for f in shielded_fs:
                with self.subTest('Persistent: ' + str(f)):
                    # Make sure all kept files were, in fact, kept
                    self.assertTrue(
                        os.fstat(f.fileno())
                    )
                    
            for f in fs:
                with self.subTest('Removed: ' + str(f)):
                    # Make sure all other files were, in fact, removed
                    with self.assertRaises(OSError):
                        os.fstat(f.fileno())

            # Do it again with no files shielded from closure.                    
            _autoclose_files()
            for f in shielded_fs:
                with self.subTest('Cleanup: ' + str(f)):
                    with self.assertRaises(OSError):
                        os.fstat(f.fileno())
        
        # Clean up any unsuccessful tests. Note idempotency of fd.close().
        finally:
            for f in shielded_fs + fs:
                try:
                    fd = f.fileno()
                    f.close()
                    os.close(fd)
                except OSError:
                    pass
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_frat_fork(self):
        ''' Test "fratricidal" (okay, parricidal) forking (fork and 
        parent dies). Platform-specific.
        '''
        pid_q = MP_SPAWN.Queue()
        ct_exit = MP_SPAWN.Event()

        # Don't use os-level forking because it screws up multiprocessing
        p = MP_SPAWN.Process(target=childproc_fratfork, args=(
            pid_q,
            ct_exit
        ))
        p.start()
        
        inter_pid = p.pid
        child_pid = pid_q.get(timeout=5)
        ct_exit.set()
        self.assertNotEqual(inter_pid, child_pid)
        
        # Wait to ensure shutdown of other process.
        time.sleep(.5)
        # Make sure the intermediate process is dead.
        self.assertFalse(p.is_alive())
        # with self.assertRaises(OSError):
        #     # Send it a signal to check existence (os.kill is badly named)
        #     os.kill(inter_pid, 0)
        
    @unittest.skipIf(not _SUPPORTED_PLATFORM, 'Unsupported platform.')
    def test_daemonize(self):
        ''' Test daemonization. Platform-specific.
        '''
        with tempfile.TemporaryDirectory() as dirname:
            pid_file = dirname + '/testpid.pid'
            token = 2718282
            res_path = dirname + '/response.txt'
            
            p = MP_SPAWN.Process(target=childproc_daemon, args=(
                pid_file,
                token,
                res_path
            ))
            p.start()
            
            # Wait a moment for the daemon to show up
            time.sleep(.5)
            # This is janky, but multiprocessing hasn't been working for
            # events or queues with daemonizing, might have something to 
            # do with multiple threads and forking or something
            
            try:
                with open(res_path, 'r') as res:
                    response = res.read()
            
            except (IOError, OSError) as exc:
                raise AssertionError from exc
                
            # Make sure the token matches
            self.assertTrue(response.startswith(str(token)))
            # Make sure the pid file exists
            self.assertTrue(os.path.exists(pid_file))
                
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