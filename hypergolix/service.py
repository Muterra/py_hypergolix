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
import traceback
import time
import argparse
import socket
import pathlib
import threading
import http.server
from http import HTTPStatus

import daemoniker
from daemoniker import Daemonizer
from daemoniker import SignalHandler1
from daemoniker import SIGTERM
from daemoniker.exceptions import ReceivedSignal

# Intra-package dependencies (that require explicit imports, courtesy of
# daemonization)
from hypergolix import logutils
from hypergolix.utils import Aengel
from hypergolix.comms import RequestResponseProtocol as Autocomms
from hypergolix.comms import BasicServer as WSBasicServer

from hypergolix.remotes import PersisterBridgeServer
from hypergolix.remotes import SalmonatorNoop

from hypergolix.persistence import PersistenceCore
from hypergolix.persistence import Doorman
from hypergolix.persistence import PostOffice
from hypergolix.persistence import Undertaker
from hypergolix.persistence import Lawyer
from hypergolix.persistence import Enforcer
from hypergolix.persistence import Bookie
from hypergolix.persistence import DiskLibrarian
from hypergolix.persistence import MemoryLibrarian
from hypergolix.persistence import Enlitener


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
        target = server.serve_forever(),
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
    elif host == 'AUTO':
        host = _get_local_ip()
    elif host == 'ANY':
        host = ''
    
    # Otherwise, host stays the same
    return host
    
    
def _shielded_server(host, port, cache_dir, debug, traceur, aengel=None):
    ''' Wraps an _hgx_server in a run-forever while loop, catching and
    logging all RuntimeErrors but otherwise restarting immediately.
    
    Ahhhhh shit, unfortunately because everything is happening in its
    own thread this won't really work.
    '''
    while True:
        try:
            _hgx_server(host, port, cache_dir, debug, traceur, aengel)
            
        except Exception:
            logger.critical(
                'Server failed with traceback:\n' +
                ''.join(traceback.format_exc())
            )


class RemotePersistenceServer:
    ''' Simple persistence server.
    Expected defaults:
    host:       'localhost'
    port:       7770
    logfile:    None
    verbosity:  'warning'
    debug:      False
    traceur:    False
    '''
    
    def __init__(self, cache_dir=None):
        self.bridge = None
        
        self.percore = PersistenceCore()
        self.doorman = Doorman()
        self.enforcer = Enforcer()
        self.lawyer = Lawyer()
        self.bookie = Bookie()
        
        if cache_dir is None:
            self.librarian = MemoryLibrarian()
        else:
            self.librarian = DiskLibrarian(cache_dir)
            
        self.postman = PostOffice()
        self.undertaker = Undertaker()
        # I mean, this won't be used unless we set up peering, but it saves us
        # needing to do a modal switch for remote persistence servers
        self.salmonator = SalmonatorNoop()
        
    def assemble(self, bridge):
        # Now we need to link everything together.
        self.percore.assemble(self.doorman, self.enforcer,
                              self.lawyer, self.bookie,
                              self.librarian, self.postman,
                              self.undertaker, self.salmonator)
        self.doorman.assemble(self.librarian)
        self.enforcer.assemble(self.librarian)
        self.lawyer.assemble(self.librarian)
        self.bookie.assemble(self.librarian, self.lawyer, self.undertaker)
        self.librarian.assemble(self.percore)
        self.postman.assemble(self.librarian, self.bookie)
        self.undertaker.assemble(self.librarian, self.bookie, self.postman)
        # Note that this will break if we ever try to use it, because
        # golix_core isn't actually a golix_core.
        self.salmonator.assemble(self, self.percore, self.doorman,
                                 self.postman, self.librarian)
        
        # Okay, now set up the bridge, and we should be ready.
        self.bridge = bridge
        self.bridge.assemble(self.percore, self.bookie,
                             self.librarian, self.postman)


def _hgx_server(host, port, cache_dir, debug, traceur, aengel=None):
    ''' Simple remote persistence server over websockets.
    Explicitly pass None to cache_dir to use in-memory only.
    '''
    if not aengel:
        aengel = Aengel()
        
    remote = RemotePersistenceServer(cache_dir)
    server = Autocomms(
        autoresponder_name = 'reremser',
        autoresponder_class = PersisterBridgeServer,
        connector_name = 'wsremser',
        connector_class = WSBasicServer,
        connector_kwargs = {
            'host': host,
            'port': port,
            'tls': False,
            # 48 bits = 1% collisions at 2.4 e 10^6 connections
            'birthday_bits': 48,
        },
        debug = debug,
        aengel = aengel,
    )
    remote.assemble(server)
    
    return remote, server

    
def start(namespace=None):
    ''' Starts a Hypergolix daemon.
    '''
    # Command args coming in.
    if namespace is not None:
        host = namespace.host
        port = namespace.port
        debug = namespace.debug
        traceur = namespace.traceur
        chdir = namespace.chdir
        # Convert log dir to absolute if defined
        if namespace.logdir is not None:
            log_dir = str(pathlib.Path(namespace.logdir).absolute())
        else:
            log_dir = namespace.log_dir
        # Convert cache dir to absolute if defined
        if namespace.cachedir is not None:
            cache_dir = str(pathlib.Path(namespace.cachedir).absolute())
        else:
            cache_dir = namespace.cache_dir
        verbosity = namespace.verbosity
        # Convert pid path to absolute (must be defined)
        pid_path = str(pathlib.Path(namespace.pidfile).absolute())
        
    # Daemonizing, we still need these to be defined to avoid NameErrors
    else:
        host = None
        port = None
        debug = None
        traceur = None
        chdir = None
        log_dir = None
        cache_dir = None
        verbosity = None
        pid_path = None
    
    with Daemonizer() as (is_setup, daemonizer):
        # Daemonize. Don't strip cmd-line arguments, or we won't know to
        # continue with startup
        (is_parent, host, port, debug, traceur, log_dir, cache_dir, verbosity,
         pid_path) = daemonizer(
            pid_path,
            host,
            port,
            debug,
            traceur,
            log_dir,
            cache_dir,
            verbosity,
            pid_path,
            chdir = chdir,
            # Don't strip these, because otherwise we won't know to resume
            # daemonization
            strip_cmd_args = False
        )
        
        if not is_parent:
            # Do signal handling within the Daemonizer so that the parent knows
            # it was correctly init'd
            sighandler = SignalHandler1(pid_path)
            sighandler.start()
        
        #####################
        # PARENT EXITS HERE #
        #####################
        
    verbosity = _cast_verbosity(verbosity, debug, traceur)
        
    if log_dir is not None:
        logutils.autoconfig(
            tofile = True,
            logdirname = log_dir,
            logname = 'hgxremote',
            loglevel = verbosity
        )
        
    logger.debug('Starting remote persistence server...')
    host = _cast_host(host)
    remote, server = _hgx_server(host, port, cache_dir, debug, traceur)
    logger.info('Remote persistence server successfully started.')
    
    # Start a health check
    healthcheck_server, healthcheck_thread = _serve_healthcheck()

    # Wait indefinitely until signal caught.
    # TODO: literally anything smarter than this.
    try:
        while True:
            time.sleep(.5)
    except SIGTERM:
        logger.info('Caught SIGTERM. Exiting.')
        
    healthcheck_server.shutdown()
    
    del remote
    del server
    del healthcheck_thread
    del healthcheck_server
    
    
def stop(namespace=None):
    ''' Stops the Hypergolix daemon.
    '''
    daemoniker.send(namespace.pidfile, SIGTERM)


# ###############################################
# Command line stuff
# ###############################################


def _ingest_args(argv=None):
    ''' Parse and handle any command-line args.
    '''
    root_parser = argparse.ArgumentParser(
        description = 'Control the Hypergolix remote persistence service.',
        prog = 'hypergolix.service'
    )
    subparsers = root_parser.add_subparsers()

    # ###############################################
    # Start command
    # ###############################################
    
    start_parser = subparsers.add_parser(
        'start',
        help = 'Start a remote persister. Invoke "start -h" for usage.'
    )
    start_parser.set_defaults(func=start)
    
    start_parser.add_argument(
        'pidfile',
        action = 'store',
        type = str,
        help = 'The full path to the PID file we should use for the service.'
    )
    start_parser.add_argument(
        '--cachedir', '-c',
        action = 'store',
        dest = 'cachedir',
        default = None,
        type = str,
        help = 'Specify a directory to use as a persistent cache for files. ' +
               'If none is specified, will default to an in-memory-only ' +
               'cache, which is, quite obviously, rather volatile.'
    )
    start_parser.add_argument(
        '--host', '-H',
        action = 'store',
        dest = 'host',
        default = None,
        type = str,
        help = 'Specify the TCP host to use. Defaults to localhost only. ' +
               'Passing the special (case-sensitive) string "AUTO" will ' +
               'determine the current local IP address and bind to that. ' +
               'Passing the special (case-sensitive) string "ANY" will bind ' +
               'to any host at the specified port (not recommended).'
    )
    start_parser.add_argument(
        '--port', '-p',
        action = 'store',
        dest = 'port',
        default = 7770,
        type = int,
        help = 'Specify the TCP port to use. Defaults to 7770.'
    )
    start_parser.add_argument(
        '--chdir',
        action = 'store',
        default = None,
        type = str,
        help = 'Once the daemon starts, chdir it into the specified full ' +
               'directory path. By default, the daemon will remain in the ' +
               'current directory, which may create DirectoryBusy errors.'
    )
    start_parser.add_argument(
        '--logdir',
        action = 'store',
        default = None,
        type = str,
        help = 'Specify a directory to use for logs. Every service failure, ' +
               'error, message, etc will go to dev/null without this.'
    )
    start_parser.add_argument(
        '--debug',
        action = 'store_true',
        help = 'Enable debug mode. Sets verbosity to debug unless overridden.'
    )
    start_parser.add_argument(
        '--traceur',
        action = 'store_true',
        help = 'Enable thorough analysis, including stack tracing. '
               'Implies verbosity of debug.'
    )
    start_parser.add_argument(
        '--verbosity', '-V',
        action = 'store',
        dest = 'verbosity',
        type = str,
        choices = ['debug', 'info', 'warning', 'error', 'shouty', 'extreme'],
        default = None,
        help = 'Sets the log verbosity. Only applicable if --logdir is set.'
    )

    # ###############################################
    # Stop command
    # ###############################################
    
    stop_parser = subparsers.add_parser(
        'stop',
        help = 'Stop a running remote persister. Invoke "stop -h" for usage.'
    )
    stop_parser.set_defaults(func=stop)
    
    stop_parser.add_argument(
        'pidfile',
        action = 'store',
        type = str,
        help = 'The full path to the PID file we should use for the service.'
    )

    # ###############################################
    # Parse and return
    # ###############################################
    
    args = root_parser.parse_args(args=argv)
    return args.func, args
        

if __name__ == '__main__':
    cmd, namespace = _ingest_args()
    # Invoke the command
    cmd(namespace)
