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
import traceback
from concurrent.futures import CancelledError

# Intra-package dependencies (that require explicit imports, courtesy of
# daemonization)
from hypergolix.bootstrapping import AgentBootstrap

from hypergolix.utils import Aengel
from hypergolix.utils import _generate_threadnames

from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicClient
from hypergolix.comms import WSBasicServer

from hypergolix.remotes import PersisterBridgeClient

from hypergolix.config import Config

from hypergolix import logutils


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
    
    
def app_core(user_id, password, startup_logger, aengel=None,
             _scrypt_hardness=None, hgx_root=None, enable_logs=True):
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
    if startup_logger is not None:
        # At some point, this will need to restore the module logger, but for
        # now it really doesn't make any difference whatsoever
        effective_logger = startup_logger
    else:
        effective_logger = logger
    
    with Config(hgx_root) as config:
        # Convert paths to strs
        cache_dir = str(config.cache_dir)
        log_dir = str(config.log_dir)
            
        if user_id is None:
            user_id = config.user_id
        
        debug = config.debug_mode
        verbosity = config.log_verbosity
        ipc_port = config.ipc_port
        remotes = config.remotes
        
    if enable_logs:
        logutils.autoconfig(
            tofile = True,
            logdirname = log_dir,
            loglevel = verbosity,
            logname = 'hgxapp'
        )
    
    if not aengel:
        aengel = Aengel()
    
    core = AgentBootstrap(aengel=aengel, debug=debug, cache_dir=cache_dir)
    core.assemble()
    
    # In this case, we have no existing user_id.
    if user_id is None:
        user_id = core.bootstrap_zero(
            password = password,
            _scrypt_hardness = _scrypt_hardness
        )
        effective_logger.critical(
            'Identity created. Your user ID is ' + str(user_id) + '. You ' +
            'will need your user ID to log in to Hypergolix from another ' +
            'machine, or if your Hypergolix configuration file is corrupted ' +
            'or lost.'
        )
        with Config(hgx_root) as config:
            config.fingerprint = core.whoami
            config.user_id = user_id
        
    # Hey look, we have an existing user.
    else:
        core.bootstrap(
            user_id = user_id,
            password = password,
            _scrypt_hardness = _scrypt_hardness,
        )
        effective_logger.info('Login successful.')
        
    # Add all of the remotes to a namespace preserver
    persisters = []
    for remote in remotes:
        try:
            persister = Autocomms(
                autoresponder_name = 'remrecli',
                autoresponder_class = PersisterBridgeClient,
                connector_name = 'remwscli',
                connector_class = WSBasicClient,
                connector_kwargs = {
                    'host': remote.host,
                    'port': remote.port,
                    'tls': remote.tls,
                },
                debug = debug,
                aengel = aengel,
            )
            
        except Exception:
            effective_logger.error(
                'Error while connecting to upstream remote at ' +
                remote.host + ':' + str(remote.port) + '. Connection will ' +
                'only be reattempted after restarting Hypergolix.'
            )
            logger.warning(
                'Connection error traceback:\n' +
                ''.join(traceback.format_exc())
            )
            
        else:
            core.salmonator.add_upstream_remote(persister)
            persisters.append(persister)
        
    # Finally, add the ipc system
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
        
    return persisters, core, aengel
