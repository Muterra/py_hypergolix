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

# Intra-package dependencies
from .utils import platform_specificker
from .utils import _default_to

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

# Service installation, etc
        
        

        
def daemonize(pid_file, *args, chdir=None, stdin_goto=None, stdout_goto=None, 
              stderr_goto=None, umask=0o027, shielded_fds=None, 
              fd_fallback_limit=1024):
    ''' Performs a classic unix double-fork daemonization. Registers all
    appropriate cleanup functions.
    
    fd_check_limit is a fallback value for file descriptor searching 
    while closing descriptors.
    
    umask is the eponymous unix umask. The default value:
        1. will allow owner to have any permissions.
        2. will prevent group from having write permission
        3. will prevent other from having any permission
    See https://en.wikipedia.org/wiki/Umask
    '''
    if not _SUPPORTED_PLATFORM:
        raise OSError('Daemonization is unsupported on your platform.')
    
    ####################################################################
    # Prep the arguments
    ####################################################################
    
    # Convert the pid_file to an abs path
    pid_file = os.path.abspath(pid_file)
    
    # Get the noop stream, in case Python is using something other than 
    # /dev/null
    if hasattr(os, "devnull"):
        devnull = os.devnull
    else:
        devnull = "/dev/null"
        
    # Convert any unset std streams to go to dev null
    stdin_goto = _default_to(stdin_goto, devnull)
    stdout_goto = _default_to(stdout_goto, devnull)
    stderr_goto = _default_to(stderr_goto, devnull)
    
    # Convert chdir to go to current dir, and also to an abs path.
    chdir = _default_to(chdir, '.')
    chdir = os.path.abspath(chdir)
    
    # And convert shield_fds to a set
    shielded_fds = _default_to(shielded_fds, set())
    shielded_fds = set(shielded_fds)
    
    ####################################################################
    # Begin actual daemonization
    ####################################################################
    
    # Get a lock on the PIDfile before forking anything.
    locked_pidfile = _acquire_pidfile(pid_file)
    # Make sure we don't accidentally autoclose it though.
    shielded_fds.add(locked_pidfile.fileno())
    
    # Define a memoized cleanup function.
    def cleanup(pid_path=pid_file, pid_lock=locked_pidfile):
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
    # Note that because fratricidal fork is calling os._exit(), our parents
    # will never call cleanup.
    
    # Now fork the toplevel parent, killing it.
    _fratricidal_fork()
    # We're now running from within the child. We need to detach ourself 
    # from the parent environment.
    _filial_usurpation(chdir, umask)
    # Okay, re-fork (no zombies!) and continue business as usual
    _fratricidal_fork()
    
    # Do some important housekeeping
    _write_pid(locked_pidfile)
    _autoclose_files(shielded_fds, fd_fallback_limit)
    _redirect_stds(stdin_goto, stdout_goto, stderr_goto)
    
    return *args