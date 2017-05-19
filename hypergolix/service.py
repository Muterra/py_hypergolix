'''
Start a hypergolix service.

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
import loopa
import concurrent.futures
import socket
import threading
import http.server
from http import HTTPStatus

import daemoniker
from daemoniker import Daemonizer
from daemoniker import SignalHandler1
from daemoniker import SIGTERM

# Intra-package dependencies (that require explicit imports, courtesy of
# daemonization)
from hypergolix import logutils
from hypergolix.comms import BasicServer
from hypergolix.comms import WSConnection

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Doorman
from hypergolix.persistence import Enforcer
from hypergolix.persistence import Bookie

from hypergolix.lawyer import LawyerCore
from hypergolix.undertaker import UndertakerCore
from hypergolix.librarian import DiskLibrarian
from hypergolix.postal import PostOffice
from hypergolix.remotes import Salmonator
from hypergolix.remotes import RemotePersistenceProtocol

from hypergolix.config import Config
from hypergolix.utils import _ensure_dir_exists
from hypergolix.utils import _default_to


# ###############################################
# Boilerplate
# ###############################################

# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
]


logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################


class _HealthHandler(http.server.BaseHTTPRequestHandler):
    ''' Handles healthcheck requests.
    '''
    
    def do_GET(self):
        # Send it a 200 with headers and GTFO
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-type', 'text/plain')
        self.send_header("Content-Length", 0)
        self.end_headers()


def _serve_healthcheck(port=7777):
    ''' Sets up an http server in a different thread to be a health
    check.
    '''
    server_address = ('', port)
    server = http.server.HTTPServer(server_address, _HealthHandler)
    worker = threading.Thread(
        # Do it in a daemon thread so that application exits are reflected as
        # unavailable, instead of persisting everything
        daemon = True,
        target = server.serve_forever,
        name = 'hlthchk'
    )
    return server, worker
    
    
def _cast_verbosity(verbosity, debug, traceur):
    ''' Returns a (potentially modified) verbosity level based on
    traceur and debug.
    '''
    if traceur:
        if verbosity != 'shouty' and verbosity != 'extreme':
            verbosity = 'debug'
        
    elif verbosity is None:
        if debug:
            verbosity = 'debug'
        else:
            verbosity = 'warning'
            
    return verbosity


def _get_local_ip():
    ''' Act like we're going to connect to Google's DNS servers and then
    use the socket to figure out our local IP address.
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 53))
    return s.getsockname()[0]
    
    
def _cast_host(host):
    ''' Checks host, defaulting to whatever.
    '''
    if host is None:
        host = '127.0.0.1'
    elif str.lower(host) == 'auto':
        host = _get_local_ip()
    elif str.lower(host) == 'any':
        host = ''
    
    # Otherwise, host stays the same
    return host


class RemotePersistenceServer(loopa.TaskCommander):
    ''' Simple persistence server.
    Expected defaults:
    host:       'localhost'
    port:       7770
    logfile:    None
    verbosity:  'warning'
    debug:      False
    traceur:    False
    '''
    
    def __init__(self, cache_dir, host, port, *args, **kwargs):
        ''' Do all of that other smart setup while we're at it.
        '''
        super().__init__(*args, **kwargs)
        
        self.executor = concurrent.futures.ThreadPoolExecutor()
        
        # Persistence stuff
        self.percore = PersistenceCore(self._loop)
        self.doorman = Doorman(self.executor, self._loop)
        self.enforcer = Enforcer()
        self.bookie = Bookie()
        self.lawyer = LawyerCore()
        self.librarian = DiskLibrarian(cache_dir, self.executor, self._loop)
        self.postman = PostOffice()
        self.undertaker = UndertakerCore()
        # I mean, this won't be used unless we set up peering, but it saves us
        # needing to do a modal switch for remote persistence servers
        self.salmonator = Salmonator.__fixture__()
        self.remote_protocol = RemotePersistenceProtocol()
        
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
            librarian = self.librarian,
            remote_protocol = self.remote_protocol
        )
        self.undertaker.assemble(
            librarian = self.librarian,
            postman = self.postman
        )
        self.remote_protocol.assemble(
            percore = self.percore,
            librarian = self.librarian,
            postman = self.postman
        )
        
        self.server = BasicServer(connection_cls=WSConnection)
        self.register_task(
            self.server,
            msg_handler = self.remote_protocol,
            host = host,
            port = port,
            tls = False
        )
        self.register_task(self.postman)
        self.register_task(self.undertaker)
        
    async def setup(self):
        ''' Once booted, restore the librarian.
        '''
        await self.librarian.restore()

    
def start(namespace=None):
    ''' Starts a Hypergolix daemon.
    '''
    # Command arg support is deprecated.
    if namespace is not None:
        # Gigantic error trap
        if ((namespace.host is not None) | (namespace.port is not None) |
            (namespace.debug is not None) | (namespace.traceur is not None) |
            (namespace.pidfile is not None) | (namespace.logdir is not None) |
            (namespace.cachedir is not None) | (namespace.chdir is not None) |
            (namespace.verbosity is not None)):
                raise RuntimeError('Server configuration through CLI is no ' +
                                   'longer supported. Edit hypergolix.yml ' +
                                   'configuration file instead.')
    
    with Daemonizer() as (is_setup, daemonizer):
        # Get our config path in setup, so that we error out before attempting
        # to daemonize (if anything is wrong).
        if is_setup:
            config = Config.find()
            config_path = config.path
            chdir = config_path.parent
            pid_file = config.server.pid_file
            
        else:
            config_path = None
            pid_file = None
            chdir = None
        
        # Daemonize.
        is_parent, config_path = daemonizer(
            str(pid_file),
            config_path,
            chdir = str(chdir),
            explicit_rescript = '-m hypergolix.service'
        )
        
        #####################
        # PARENT EXITS HERE #
        #####################
        
    config = Config.load(config_path)
    _ensure_dir_exists(config.server.ghidcache)
    _ensure_dir_exists(config.server.logdir)
    
    debug = _default_to(config.server.debug, False)
    verbosity = _default_to(config.server.verbosity, 'info')
    
    logutils.autoconfig(
        tofile = True,
        logdirname = config.server.logdir,
        logname = 'hgxserver',
        loglevel = verbosity
    )
        
    logger.debug('Parsing config...')
    host = _cast_host(config.server.host)
    rps = RemotePersistenceServer(
        config.server.ghidcache,
        host,
        config.server.port,
        reusable_loop = False,
        threaded = False,
        debug = debug
    )
    
    logger.debug('Starting health check...')
    # Start a health check
    healthcheck_server, healthcheck_thread = _serve_healthcheck()
    healthcheck_thread.start()
        
    logger.debug('Starting signal handler...')
    
    def signal_handler(signum):
        logger.info('Caught signal. Exiting.')
        healthcheck_server.shutdown()
        rps.stop_threadsafe_nowait()
        
    # Normally I'd do this within daemonization, but in this case, we need to
    # wait to have access to the handler.
    sighandler = SignalHandler1(
        str(config.server.pid_file),
        sigint = signal_handler,
        sigterm = signal_handler,
        sigabrt = signal_handler
    )
    sighandler.start()
    
    logger.info('Starting remote persistence server...')
    rps.start()
    
    
def stop(namespace=None):
    ''' Stops the Hypergolix daemon.
    '''
    if namespace.pidfile is not None:
        raise RuntimeError('Server pidfile specification through CLI is no ' +
                           'longer supported. Edit hypergolix.yml ' +
                           'configuration file instead.')
        
    config = Config.find()
    daemoniker.send(str(config.server.pid_file), SIGTERM)
    
    
if __name__ == "__main__":
    ''' This is used exclusively for reentry of the Windows daemon.
    '''
    start()
