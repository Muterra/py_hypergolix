'''
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

# Global dependencies
import logging
import traceback
import os
import sys
import time
import signal
import pickle
import base64
import subprocess
import multiprocessing
import shlex

# Intra-package dependencies
from .utils import platform_specificker
from .utils import _default_to

from ._daemonize import _redirect_stds
from ._daemonize import _write_pid

_SUPPORTED_PLATFORM = platform_specificker(
    linux_choice = False,
    win_choice = True,
    # Dunno if this is a good idea but might as well try
    cygwin_choice = True,
    osx_choice = False,
    other_choice = False
)

# ###############################################
# Boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Inquisitor', 
]


# ###############################################
# Library
# ###############################################


class Daemonizer:
    ''' Emulates Unix daemonization and registers all appropriate 
    cleanup functions.
    
    with Daemonizer() as (is_setup, daemonize):
        if is_setup:
            setup_code_here()
            daemonize(*args)
        else:
            *args = daemonize()
    '''
    def __init__(self, pid_file, chdir=None, stdin_goto=None, stdout_goto=None, 
                 stderr_goto=None, umask=0o027, shielded_fds=None, 
                 fd_fallback_limit=1024, success_timeout=30):
        ''' Smart stuff and stuff.
    
        umask, shielded_fds, fd_fallback_limit are unused for this 
        Windows version.
        
        success_timeout is the wait for a signal. If nothing happens 
        after timeout, we will raise a ChildProcessError.
        '''
            
        # And figure out whatever else is necessary
        self._timeout = success_timeout
        
    @staticmethod
        
    def __call__(self, *args):
        ''' Calls the daemonization function. From the parent caller, 
        this will exit the interpreter. From the child daemon, this will
        return all *args passed to the parent caller.
        '''
        # This the parent / setup pass
        if not self._daemonized:
            pass
        
        # This the child / daemon pass
        else:
            pass
        
    def __enter__(self):
        # This the parent / setup pass
        if not self._daemonized:
            pass
        
        # This the child / daemon pass
        else:
            pass
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        # This the parent / setup pass
        if not self._daemonized:
            pass
        
        # This the child / daemon pass
        else:
            pass
            
            
def _capability_check(pythonw_path, script_path):
    ''' Does a compatibility and capability check.
    '''
    if not _SUPPORTED_PLATFORM:
        raise OSError(
            'The Windows Daemonizer cannot be used on the current '
            'platform.'
        )
        
    if not os.path.exists(pythonw_path):
        raise SystemExit(
            'pythonw.exe must be available in the same directory as the '
            'current Python interpreter to support Windows daemonization.'
        )
        
    if not os.path.exists(script_path):
        raise SystemExit(
            'Daemonizer cannot locate the script to daemonize (it seems '
            'to have lost itself).'
        )
        
        
def _acquire_pidfile(pid_file, silence_lock=False):
    ''' Opens the pid_file, but unfortunately, as this is Windows, we 
    cannot really lock it. Assume existence is equivalent to locking,
    unless autoclean=True.
    '''
    try:
        if os.path.isfile(pid_file):
            if silence_lock:
                logger.warning(
                    'PID file already exists. It will be overwritten with the '
                    'new PID upon successful daemonization.'
                )
                open_pid = open(pid_file, 'r+')
            else:
                logger.critical(
                    'PID file already exists. Acquire with autoclean=True to '
                    'force cleanup of existing PID file. Traceback: \n' + 
                    ''.join(traceback.format_exc())
                )
                raise SystemExit('Unable to acquire PID file.')
                
        else:
            open_pid = open(pid_file, 'w+')
            
    except (IOError, OSError) as exc:
        logger.critical(
            'Unable to create/open the PID file w/ traceback: \n' + 
            ''.join(traceback.format_exc())
        )
        raise SystemExit('Unable to create/open PID file.') from exc
        
    return open_pid
    
    
def _filial_usurpation(chdir):
    ''' Changes our working directory, helping decouple the child 
    process from the parent. Not necessary on windows, but could help 
    standardize stuff for cross-platform apps.
    '''
    # Well this is certainly a stub.
    os.chdir(chdir)
        
        
def _clean_file(path):
    ''' Remove the file at path, if it exists, suppressing any errors.
    '''
    # Clean up the PID file.
    try:
        # This will raise if the child process had a chance to register
        # and complete its exit handler.
        os.remove(path)
        
    # So catch that error if it happens.
    except OSError:
        pass
        
        
class _NamespacePasser:
    ''' Creates a path in a secure temporary directory, such that the
    path can be used to write in a reentrant manner. Upon context exit,
    the file will be overwritten with zeros, removed, and then the temp
    directory cleaned up.
    
    We can't use the normal tempfile stuff because:
    1. it doesn't zero the file
    2. it prevents reentrant opening
    
    Using this in a context manager will return the path to the file as
    the "as" target, ie, "with _ReentrantSecureTempfile() as path:".
    '''
    
    def __init__(self):
        ''' Store args and kwargs to pass into enter and exit.
        '''
        seed = os.urandom(16)
        self._stem = base64.urlsafe_b64encode(seed).decode()
        self._tempdir = None
        self.name = None
    
    def __enter__(self):
        try:
            # Create a resident tempdir
            self._tempdir = tempfile.TemporaryDirectory()
            # Calculate the final path
            self.name = self._tempdir.name + '/' + self._stem
            # Ensure the file exists, so future cleaning calls won't error
            with open(self.name, 'wb') as f:
                pass
            
        except:
            self._tempdir.cleanup()
            raise
            
        else:
            return self.name
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        ''' Zeroes the file, removes it, and cleans up the temporary
        directory.
        '''
        try:
            # Open the existing file and overwrite it with zeros.
            with open(self.name, 'r+b') as f:
                to_erase = f.read()
                eraser = bytes(len(to_erase))
                f.seek(0)
                f.write(eraser)
                
            # Remove the file. We just accessed it, so it's guaranteed to exist
            os.remove(self.name)
            
        # Warn of any errors in the above, and then re-raise.
        except:
            logger.error(
                'Error while shredding secure temp file.\n' + 
                ''.join(traceback.format_exc())
            )
            raise
            
        finally:
            self._tempdir.cleanup()
            
            
def _fork_worker(namespace_path, child_env, pid_file, invocation, chdir, 
                 stdin_goto, stdout_goto, stderr_goto, args):
    ''' Opens a fork worker, shielding the parent from cancellation via
    signal sending. Basically, thanks Windows for being a dick about 
    signals.
    '''
    # Find out our PID so the daughter can tell us to exit
    my_pid = os.getpid()
    # Pack up all of the args that the child will need to use.
    # Prepend it to *args
    payload = (my_pid, pid_file, chdir, stdin_goto, stdout_goto, 
               stderr_goto) + args
    # Pack it up. Interestingly, I think we're shielded from pickling errors
    # already because pickle is needed for multiprocessing.
    payload = pickle.dumps(payload)
    
    # Write the payload to the namespace passer
    with open(namespace_path, 'wb') as f:
        f.write(payload)
    
    # Invoke the invocation!
    daemon = subprocess.Popen(
        invocation, 
        # This is important, because the parent _forkish is telling the child
        # to run as a daemon via env.
        env = child_env,
        # This is vital; without it, our process will be reaped at parent
        # exit.
        creationflags = subprocess.CREATE_NEW_CONSOLE,
    )
    # Busy wait until either the daemon exits, or it sends a signal to kill us.
    daemon.wait()
            
            
def forkish(pid_file, chdir=None, stdin_goto=None, stdout_goto=None, 
            stderr_goto=None, *args, success_timeout=30, strip_cmd_args=False):
    ''' Create an independent process for invocation, telling it to 
    store its "pid" in the pid_file (actually, the pid of its signal
    listener). Payload is an iterable of variables to pass the invoked
    command for returning from _respawnish.
    
    Note that a bare call to this function will result in all code 
    before the daemonize() call to be run twice.
    
    The daemon's signal-receiving pid will be recorded in pid_file.
    *args will be passed to child. Waiting for success signal will 
        timeout after success_timeout seconds.
    strip_cmd_args will ignore all additional command-line args in the
        second run.
    all other args identical to unix version of daemonize.
    '''
    ####################################################################
    # Error trap and calculate invocation
    ####################################################################
        
    # First make sure we can actually do this.
    # We need to check the path to pythonw.exe
    python_path = sys.executable
    python_path = os.path.abspath(python_path)
    python_dir = os.path.dirname(python_path)
    pythonw_path = python_dir + '/pythonw.exe'
    # We also need to check our script is known and available
    script_path = sys.argv[0]
    script_path = os.path.abspath(script_path)
    _capability_check(pythonw_path, script_path)
    
    invocation = '"' + pythonw_path + '" "' + script_path + '"'
    # Note that we don't need to worry about being too short like this; python
    # doesn't care with slicing. But, don't forget to escape the invocation.
    if not strip_cmd_args:
        for cmd_arg in sys.argv[1:]:
            invocation += ' ' + shlex.quote(cmd_arg)
    
    ####################################################################
    # Begin actual forking
    ####################################################################
            
    # Convert the pid_file to an abs path
    pid_file = os.path.abspath(pid_file)
    # Get a "lock" on the PIDfile before forking anything by opening it 
    # without silencing anything. Unless we error out while birthing, it
    # will be our daughter's job to clean up this file.
    open_pidfile = _acquire_pidfile(pid_file)
    open_pidfile.close()
        
    # Now open up a secure way to pass a namespace to the daughter process.
    with _NamespacePasser() as fpath:
        # Determine the child env
        child_env = {**os.environ, '__INVOKE_DAEMON__': fpath}
            
        # We need to shield ourselves from signals, or we'll be terminated by
        # python before running cleanup. So use a spawned worker to handle the
        # actual daemon creation.
        try:
            worker = multiprocessing.Process(
                target = _fork_worker, 
                args = (
                    fpath, # namespace_path
                    child_env,
                    pid_file,
                    invocation,
                    chdir,
                    stdin_goto,
                    stdout_goto,
                    stderr_goto,
                    args
                )
            )
            # Start the worker and wait for it to be killed by the daemon's
            # success signal.
            worker.start()
            worker.join(success_timeout)
            
        except multiprocessing.TimeoutError as exc:
            worker.terminate()
            _clean_file(pid_file)
            raise
    
    
def respawnish():
    ''' Unpacks the daemonization. Modifies the new environment as per
    the parent's forkish() call. Registers appropriate cleanup methods
    for the pid_file. Signals successful daemonization. Returns the
    *args passed to parent forkish() call.
    '''
    ####################################################################
    # Unpack and prep the arguments
    ####################################################################
    ns_passer_path = os.environ['__INVOKE_DAEMON__']
    with open(ns_passer_path, 'rb') as f:
        payload = f.read()
        
    # Unpack the namespace.
    pkg = pickle.loads(payload)
    parent, pid_file, chdir, stdin_goto, stdout_goto, stderr_goto, *args = pkg
    
    # Convert any unset std streams to go to dev null
    stdin_goto = _default_to(stdin_goto, os.devnull)
    stdout_goto = _default_to(stdout_goto, os.devnull)
    stderr_goto = _default_to(stderr_goto, os.devnull)
    
    # Convert chdir to go to current dir, and also to an abs path.
    chdir = _default_to(chdir, '.')
    chdir = os.path.abspath(chdir)
    
    ####################################################################
    # Resume actual daemonization
    ####################################################################
        
    # Get the "locked" PIDfile before forking anything by opening it 
    # without silencing anything.
    open_pidfile = _acquire_pidfile(pid_file, silence_lock=True)
    
    # Do some important housekeeping
    _write_pid(open_pidfile)
    _redirect_stds(stdin_goto, stdout_goto, stderr_goto)
    _filial_usurpation(chdir)
        
    # Define a memoized cleanup function.
    def cleanup(pid_path=pid_file, pid_lock=open_pidfile):
        try:
            pid_lock.close()
            os.remove(pid_path)
        except:
            logger.error(
                'Failed to clean up pidfile w/ traceback: \n' + 
                ''.join(traceback.format_exc())
            )
    
    # Register this as soon as possible in case something goes wrong.
    atexit.register(cleanup)
    
    # "Notify" parent of success
    os.kill(parent, signal.SIGINT)
    
    return args
    
    
def sighandler():
    ''' Signal handling system.
    '''