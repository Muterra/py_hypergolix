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
import time
import argparse
import logging
import socket

import daemoniker
from daemoniker import Daemonizer
from daemoniker import SignalHandler1
from daemoniker import SIGTERM

# Intra-package dependencies (that require explicit imports, courtesy of
# daemonization)
from hypergolix import logutils
from hypergolix.utils import Aengel
from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicServer
from hypergolix.remotes import PersisterBridgeServer
from hypergolix.remotes import RemotePersistenceServer


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


def _get_local_ip():
    ''' Act like we're going to connect to Google's DNS servers and then
    use the socket to figure out our local IP address.
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 53))
    return s.getsockname()[0]
    
    
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
    
    
def _cast_host(host):
    ''' Checks host, defaulting to whatever.
    '''
    if host is None:
        host = '127.0.0.1'
    elif host == 'AUTO':
        host = _get_local_ip()
    
    # Otherwise, host stays the same
    return host


def _hgx_server(host, port, debug, traceur, aengel=None):
    ''' Simple remote persistence server over websockets.
    '''
    if not aengel:
        aengel = Aengel()
        
    remote = RemotePersistenceServer()
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
        log_dir = namespace.logdir
        verbosity = namespace.verbosity
        pid_path = namespace.pidfile
        
    # Daemonizing, we still need these to be defined to avoid NameErrors
    else:
        host = None
        port = None
        debug = None
        traceur = None
        chdir = None
        log_dir = None
        verbosity = None
        pid_path = None
    
    with Daemonizer() as (is_setup, daemonizer):
        # Daemonize. Don't strip cmd-line arguments, or we won't know to
        # continue with startup
        is_parent, host, port, debug, traceur, log_dir, verbosity, pid_path = \
            daemonizer(
                pid_path,
                host,
                port,
                debug,
                traceur,
                log_dir,
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
            loglevel = verbosity
        )
        
    logger.debug('Starting remote persistence server...')
    host = _cast_host(host)
    remote, server = _hgx_server(host, port, debug, traceur)
    logger.info('Remote persistence server successfully started.')

    # Wait indefinitely until signal caught.
    # TODO: literally anything smarter than this.
    try:
        while True:
            time.sleep(.5)
    except SIGTERM:
        logger.info('Caught SIGTERM. Exiting.')
    
    del remote
    del server
    
    
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
        '--host', '-H',
        action = 'store',
        dest = 'host',
        default = None,
        type = str,
        help = 'Specify the TCP host to use. Defaults to localhost only. ' +
               'Passing the special (case-sensitive) string "AUTO" will ' +
               'determine the current local IP address and bind to that.'
    )
    start_parser.add_argument(
        '--port', '-P',
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
