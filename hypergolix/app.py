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
import asyncio
import loopa
import concurrent.futures

from golix import Secret
from golix import Ghid

# Intra-package dependencies (that require explicit imports, courtesy of
# daemonization)
from hypergolix.hypothetical import API
from hypergolix.hypothetical import public_api
from hypergolix.hypothetical import fixture_noop
from hypergolix.hypothetical import fixture_api

from hypergolix import logutils
from hypergolix.accounting import Account

from hypergolix.utils import weak_property

from hypergolix.comms import BasicServer
from hypergolix.comms import WSConnection

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Doorman
from hypergolix.persistence import Enforcer
from hypergolix.persistence import Bookie

from hypergolix.lawyer import LawyerCore
from hypergolix.undertaker import Ferryman
from hypergolix.librarian import DiskLibrarian
from hypergolix.postal import MrPostman
from hypergolix.remotes import Salmonator
from hypergolix.remotes import RemotePersistenceProtocol
from hypergolix.core import GolixCore
from hypergolix.core import GhidProxier
from hypergolix.core import Oracle
from hypergolix.rolodex import Rolodex
from hypergolix.dispatch import Dispatcher
from hypergolix.ipc import IPCServerProtocol
from hypergolix.privateer import Privateer


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Inquisitor',
]


# ###############################################
# Library
# ###############################################


class HypergolixCore(loopa.TaskCommander, metaclass=API):
    ''' The core Hypergolix system.
    '''
    account = weak_property('_account')
    
    @public_api
    def __init__(self, cache_dir, ipc_port, *args, **kwargs):
        ''' Create and assemble everything, readying it for a bootstrap
        (etc).
        
        user_id may be explicitly None to create a new account.
        '''
        super().__init__(*args, **kwargs)
        # We also want to create an event so things can block on us being
        # actually, properly available
        self._ctx = asyncio.Event(loop=self._loop)
        
        # Manufacturing!
        ######################################################################
        
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=25)
        
        # Persistence stuff
        self.percore = PersistenceCore()
        self.doorman = Doorman(self.executor, self._loop)
        self.enforcer = Enforcer()
        self.bookie = Bookie()
        self.lawyer = LawyerCore()
        self.librarian = DiskLibrarian(cache_dir)
        self.postman = MrPostman()
        self.undertaker = Ferryman()
        self.salmonator = Salmonator()
        self.remote_protocol = RemotePersistenceProtocol()
        
        # Golix stuff
        self.golcore = GolixCore(self.executor, self._loop)
        self.ghidproxy = GhidProxier()
        self.oracle = Oracle()
        self.privateer = Privateer()
        
        # Application engine stuff
        self.rolodex = Rolodex()
        self.dispatch = Dispatcher()
        self.ipc_protocol = IPCServerProtocol()
        self.ipc_server = BasicServer(connection_cls=WSConnection)
        
        # Assembly!
        ######################################################################
        
        # Persistence assembly
        self.percore.assemble(
            doorman = self.doorman,
            enforcer = self.enforcer,
            lawyer = self.lawyer,
            bookie = self.bookie,
            librarian = self.librarian,
            postman = self.postman,
            undertaker = self.undertaker,
            salmonator = self.salmonator
        )
        self.doorman.assemble(librarian=self.librarian)
        self.enforcer.assemble(librarian=self.librarian)
        self.bookie.assemble(librarian=self.librarian)
        self.lawyer.assemble(librarian=self.librarian)
        self.librarian.assemble(
            enforcer = self.enforcer,
            lawyer = self.lawyer,
            percore = self.percore
        )
        self.postman.assemble(
            golcore = self.golcore,
            oracle = self.oracle,
            librarian = self.librarian,
            rolodex = self.rolodex,
            salmonator = self.salmonator
        )
        self.undertaker.assemble(
            librarian = self.librarian,
            oracle = self.oracle,
            postman = self.postman,
            privateer = self.privateer
        )
        self.salmonator.assemble(
            golcore = self.golcore,
            percore = self.percore,
            librarian = self.librarian,
            remote_protocol = self.remote_protocol
        )
        self.remote_protocol.assemble(
            percore = self.percore,
            librarian = self.librarian,
            postman = self.postman,
            salmonator = self.salmonator
        )
        
        # Golix assembly
        self.golcore.assemble(self.librarian)
        self.ghidproxy.assemble(self.librarian)
        self.oracle.assemble(
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            salmonator = self.salmonator
        )
        self.privateer.assemble(self.golcore)
        
        # App engine assembly
        self.dispatch.assemble(
            oracle = self.oracle,
            ipc_protocol = self.ipc_protocol
        )
        self.rolodex.assemble(
            golcore = self.golcore,
            ghidproxy = self.ghidproxy,
            privateer = self.privateer,
            percore = self.percore,
            librarian = self.librarian,
            salmonator = self.salmonator,
            dispatch = self.dispatch
        )
        self.ipc_protocol.assemble(
            golcore = self.golcore,
            oracle = self.oracle,
            dispatch = self.dispatch,
            rolodex = self.rolodex,
            salmonator = self.salmonator
        )
        
        # Task registration!
        ######################################################################
        
        # Note that order of these is meaningful.
        self.register_task(self.undertaker)
        self.register_task(self.postman)
        self.register_task(self.salmonator)
        self.register_task(
            self.ipc_server,
            msg_handler = self.ipc_protocol,
            host = 'localhost',
            port = ipc_port
        )
        
    @__init__.fixture
    def __init__(self, **kwargs):
        # As a fixture, just allow us to set kwargs for shit easily.
        for key, value in kwargs.items():
            setattr(self, key, value)
        
    def add_remote(self, connection_cls, *args, **kwargs):
        ''' Add an upstream remote. Connection using connection_cls; on
        instantiation, the connection will use *args and **kwargs.
        
        MUST BE CALLED BEFORE STARTING!
        '''
        self.salmonator.add_upstream_remote(
            task_commander = self,
            connection_cls = connection_cls,
            *args,
            **kwargs
        )
        
    async def setup(self):
        ''' Do all of the post-init-pre-run stuff.
        '''
        await self.librarian.restore()
        await self.account.bootstrap()
        self._ctx.set()
        
    async def teardown(self):
        ''' Do all of the post-run-pre-close stuff.
        '''
        # Hold off on this until we stop hanging on close
        # await self.account.flush()
        
    async def await_startup(self):
        ''' Wait for startup to complete.
        '''
        await self._ctx.wait()
    
    
def run(user_id, password, startup_logger, hgx_root=None, enable_logs=True):
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
