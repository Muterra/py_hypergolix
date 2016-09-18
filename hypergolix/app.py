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
import argparse
import sys
import getpass

from golix import Ghid

# Intra-package dependencies
from .bootstrapping import AgentBootstrap

from .utils import Aengel
from .utils import threading_autojoin
from .utils import _generate_threadnames

from .comms import Autocomms
from .comms import WSBasicClient
from .comms import WSBasicServer

from .remotes import PersisterBridgeClient

from . import logutils


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


def _make_bootstrap_chatty():
    ''' Makes the bootstrap process log info messages into STDOUT.
    '''
    bootstrap_logger = logging.getLogger('hypergolix.bootstrapping')
    bootstrap_logger.setLevel(logging.INFO)
    
    loghandler = logging.StreamHandler(sys.stdout)
    loghandler.setFormatter(
        logging.Formatter(
            'Hypergolix startup: %(message)s'
        )
    )
    
    bootstrap_logger.addHandler(loghandler)
    
    return bootstrap_logger, loghandler
    
    
def _make_bootstrap_quiet(handler):
    ''' Restores the bootstrap process logging to its previous verbosity
    and removes the handler.
    '''
    bootstrap_logger = logging.getLogger('hypergolix.bootstrapping')
    bootstrap_logger.setLevel(logging.NOTSET)
    bootstrap_logger.removeHandler(handler)
    
    
def HGXService(host, port, tls, ipc_port, debug, traceur, cache_dir, 
                foreground=True, aengel=None, user_id=None, password=None,
                _scrypt_hardness=None):
    ''' This is where all of the UX goes for the service itself. From 
    here, we build a credential, then a bootstrap, and then persisters,
    IPC, etc.
    
    Expected defaults:
    host:       'localhost'
    port:       7770
    tls:        True
    ipc_port:   7772
    debug:      False
    logfile:    None
    verbosity:  'warning'
    traceur:    False
    '''
        
    debug = bool(debug)
    # Note: this isn't currently used.
    traceur = bool(traceur)
    
    # # Override the module-level logger definition to root
    # logger = logging.getLogger()
    # # For now, log to console
    # log_handler = logging.StreamHandler()
    # log_handler.setLevel(log_level)
    # logger.addHandler(log_handler)
    
    # Todo: add traceur argument to dump stack traces into debug log, and/or
    # use them to auto-detect deadlocks/hangs
    
    if not aengel:
        aengel = Aengel()
    
    core = AgentBootstrap(aengel=aengel, debug=debug, cache_dir=cache_dir)
    core.assemble()
    
    # Do the actual bootstrap, chattily telling STDOUT what's up.
    bootstrap_logger, tmp_handler = _make_bootstrap_chatty()
    
    # Either way, we need a password.
    if password is None:
        password = getpass.getpass(
            prompt = 'Please enter your Hypergolix password. It will not be '
                    'shown while you type. Hit enter when done.'
        )
    password = password.encode('utf-8')
    
    # In this case, we have no existing user_id.
    if user_id is None:
        user_id = core.bootstrap_zero(
            password = password, 
            _scrypt_hardness = _scrypt_hardness
        )
        bootstrap_logger.info(
            'Identity created. Your user_id is ' + str(user_id) + '. '
            'Contacting upstream persisters and starting IPC server.'
        )
        
    # Hey look, we have an existing user.
    else:
        core.bootstrap(
            user_id = user_id, 
            password = password,
            _scrypt_hardness = _scrypt_hardness,
        )
        
    bootstrap_logger.info(
        'Successfully logged in to fingerprint ' + str(core.whoami)
    )
    _make_bootstrap_quiet(tmp_handler)
    del tmp_handler
    del bootstrap_logger
    
    persister = Autocomms(
        autoresponder_name = 'remrecli',
        autoresponder_class = PersisterBridgeClient,
        connector_name = 'remwscli',
        connector_class = WSBasicClient,
        connector_kwargs = {
            'host': host,
            'port': port,
            'tls': tls,
        },
        debug = debug,
        aengel = aengel,
    )
    core.salmonator.add_upstream_remote(persister)
    core.ipccore.add_ipc_server(
        'wslocal', 
        WSBasicServer, 
        host = 'localhost', 
        port = ipc_port,
        tls = False,
        debug = debug,
        aengel = aengel,
        threaded = True,
        thread_name = _generate_threadnames('ipc-ws')[0],
    )
    
    # Automatically detect if we're the main thread. If so, wait indefinitely
    # until signal caught.
    if foreground:
        threading_autojoin()
        
    return persister, core


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start the Hypergolix service.')
    parser.add_argument(
        '--host', 
        action = 'store',
        default = 'localhost', 
        type = str,
        help = 'Specify the persistence provider host [default: localhost]'
    )
    parser.add_argument(
        '--port', 
        action = 'store',
        default = 7770, 
        type = int,
        help = 'Specify the persistence provider port [default: 7770]'
    )
    parser.add_argument(
        '--notls', 
        action = 'store_true',
        help = 'Set debug mode. Automatically sets verbosity to debug.'
    )
    parser.add_argument(
        '--ipcport', 
        action = 'store',
        default = 7772, 
        type = int,
        help = 'Specify the ipc port [default: 7772]'
    )
    parser.add_argument(
        '--debug', 
        action = 'store_true',
        help = 'Set debug mode. Automatically sets verbosity to debug.'
    )
    parser.add_argument(
        '--cachedir', 
        action = 'store',
        default = './', 
        type = str,
        help = 'Specify the directory to use for on-disk caching, relative to '
                'the current path. Defaults to the current directory.'
    )
    parser.add_argument(
        '--logdir', 
        action = 'store',
        default = None, 
        type = str,
        help = 'Log to a specified director, relative to current path.',
    )
    parser.add_argument(
        '--userid', 
        action = 'store',
        default = None, 
        type = str,
        help = 'Specifies a Hypergolix login user. If omitted, creates a new '
                'account.',
    )
    parser.add_argument(
        '--verbosity', 
        action = 'store',
        default = 'warning', 
        type = str,
        help = 'Specify the logging level. \n'
                '    "extreme"  -> ultramaxx verbose, \n'
                '    "shouty"   -> abnormal most verbose, \n'
                '    "debug"    -> normal most verbose, \n'
                '    "info"     -> somewhat verbose, \n'
                '    "warning"  -> default python verbosity, \n'
                '    "error"    -> quiet.',
    )
    parser.add_argument(
        '--traceur', 
        action = 'store_true',
        help = 'Enable thorough analysis, including stack tracing. '
                'Implies verbosity of debug.'
    )

    args = parser.parse_args()
    
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
        
    if args.userid is None:
        user_id = None
    else:
        user_id = Ghid.from_str(args.userid)
    
    HGXService(
        host = args.host, 
        port = args.port, 
        tls = not args.notls,
        ipc_port = args.ipcport, 
        debug = args.debug, 
        traceur = args.traceur,
        cache_dir = args.cachedir,
        user_id = user_id
    )
