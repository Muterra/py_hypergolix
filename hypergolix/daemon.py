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
import socket
import multiprocessing
import time
import pathlib
import traceback

from golix import Ghid

from daemoniker import Daemonizer
from daemoniker import SignalHandler1

# Intra-package dependencies (that require explicit imports, courtesy of
# daemonization)
from hypergolix.bootstrapping import AgentBootstrap

from hypergolix.utils import Aengel
from hypergolix.utils import threading_autojoin
from hypergolix.utils import _generate_threadnames

from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicClient
from hypergolix.comms import WSBasicServer

from hypergolix.remotes import PersisterBridgeClient

from hypergolix import logutils


# ###############################################
# Boilerplate
# ###############################################


import logging
import logging.handlers
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Inquisitor',
]


# ###############################################
# Bootstrap logging comms
# ###############################################


class _BootstrapFilter(logging.Filter):
    ''' Use multiple logging levels based on the logger, all with the
    same handler.
    '''
    
    def filter(self, record):
        # Emit anything at WARNING or higher from all loggers
        if record.levelno >= logging.WARNING:
            return True
            
        # Emit everything from bootstrapping
        elif record.name == 'hypergolix.bootstrapping':
            return True
            
        # Emit nothing else
        else:
            return False
            
            
def _await_server(port, cycle_time, timeout):
    ''' Busy wait for a logserver to be available. Raises
    socket.timeout if unavailable.
    '''
    conn_timeout = cycle_time / 2
    sleep_for = conn_timeout
    cycles = int(timeout / cycle_time)
    
    log_server = ('127.0.0.1', port)
    
    # Attempt to connect until approximately hitting the timeout
    for __ in range(cycles):
        
        try:
            socket.create_connection(log_server, conn_timeout)
            
        except socket.timeout:
            # Busy wait and try again
            time.sleep(sleep_for)
            
        else:
            break
            

def _close_server(port):
    ''' Saturates the logging server's max number of connections,
    ensuring it departs its .accept() loop.
    '''
    conn_timeout = .1
    log_server = ('127.0.0.1', port)
    
    # Attempt to connect repeatedly until we error
    while True:
        try:
            socket.create_connection(log_server, conn_timeout)
            
        except OSError:
            # OSError includes socket.timeout. This implies that the parent
            # is not receiving connections and has successfully closed.
            break
        

class _StartupReporter:
    ''' Context manager for temporary reporting of startup logging.
    '''
    
    def __init__(self, port, cycle_time=.1, timeout=30):
        ''' port determines what localhost port to contact
        '''
        self.port = port
        self.handler = None
        
        self._cycle_time = cycle_time
        self._timeout = timeout
        
    def __enter__(self):
        ''' Sets up the logging reporter.
        '''
        # Wait for the server to exist first.
        logging_port = self.port
        
        try:
            _await_server(logging_port, self._cycle_time, self._timeout)

        # No connection happened, so we should revert to a stream handler
        except socket.timeout:
            logger.warning(
                'Timeout while attempting to connect to the bootstrap ' +
                'logging server.'
            )
            logging_port = None
            
        # If we have an available server to log to, use it
        if logging_port is not None:
            self.handler = logging.handlers.SocketHandler(
                host = '127.0.0.1',
                port = logging_port
            )
            
        # Otherwise, default to sys.stdout
        else:
            self.handler = logging.StreamHandler(sys.stdout)
            self.handler.setFormatter(
                logging.Formatter(
                    'Hypergolix startup: %(message)s'
                )
            )
            
        self.handler.handleError = print
            
        # Assign a filter to chill the noise
        self.handler.addFilter(_BootstrapFilter())
        
        # Enable the handler for hypergolix.bootstrapping
        bootstrap_logger = logging.getLogger('hypergolix.bootstrapping')
        self._bootstrap_revert_level = bootstrap_logger.level
        self._bootstrap_revert_propagation = bootstrap_logger.propagate
        bootstrap_logger.setLevel(logging.INFO)
        bootstrap_logger.addHandler(self.handler)
        # If we don't do this we get two messages for everything
        bootstrap_logger.propagate = False
        
        # Enable the handler for root
        root_logger = logging.getLogger('')
        # Ensure a minimum level of WARNING
        if root_logger.level < logging.WARNING:
            self._root_revert_level = root_logger.level
            root_logger.setLevel(logging.WARNING)
        else:
            self._root_revert_level = None
        # And finally, add the handler
        root_logger.addHandler(self.handler)
        
        # Return the bootstrap_logger so it can be used.
        return bootstrap_logger
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        ''' Restores the bootstrap process logging to its previous
        verbosity and removes the handler.
        '''
        try:
            root_logger = logging.getLogger('')
            bootstrap_logger = logging.getLogger('hypergolix.bootstrapping')
            
            bootstrap_logger.propagate = self._bootstrap_revert_propagation
            bootstrap_logger.setLevel(self._bootstrap_revert_level)
            if self._root_revert_level is not None:
                root_logger.setLevel(self._root_revert_level)
                
            bootstrap_logger.removeHandler(self.handler)
            root_logger.removeHandler(self.handler)
        
        finally:
            # Close the handler and, if necessary, the server
            self.handler.close()
            if isinstance(self.handler, logging.handlers.SocketHandler):
                _close_server(self.port)
        
        
def _handle_startup_connection(conn, timeout):
        try:
            # Loop forever until the connection is closed.
            while not conn.closed:
                if conn.poll(timeout):
                    try:
                        request = conn.recv()
                        print('Hypergolix startup: ' + request['msg'])
                    
                    except EOFError:
                        # Connections that ping without a body and immediately
                        # disconnect, or the end of the connection, will EOF
                        return
                        
                else:
                    # We want to break out of the parent _serve for loop.
                    raise socket.timeout(
                        'Timeout while listening to daemon startup.'
                    )
            
        finally:
            conn.close()
        
        
def _startup_listener(port, timeout):
    server_address = ('127.0.0.1', port)
    
    with multiprocessing.connection.Listener(server_address) as server:
        # Do this twice: once for the client asking "are you there?" and a
        # second time for the actual logs.
        for __ in range(2):
            with server.accept() as conn:
                _handle_startup_connection(conn, timeout)


# ###############################################
# Library
# ###############################################
    
    
def _create_password():
    ''' The typical double-prompt for password creation.
    '''
    password1 = False
    password2 = True
    first_prompt = ('Please create a password for your Hypergolix account. ' +
                    'It won\'t be shown while you type. Hit enter when done:')
    second_prompt = 'And once more to check it:'
    
    while password1 != password2:
        password1 = getpass.getpass(prompt=first_prompt)
        password2 = getpass.getpass(prompt=second_prompt)
        
        first_prompt = 'Passwords do not match! Try again please:'
        
    return password1.encode('utf-8')
    
    
def _enter_password():
    ''' Single-prompt for logging in via an existing password.
    '''
    prompt = ('Please enter your Hypergolix password. It will not be shown '
              'while you type. Hit enter when done:')
    password = getpass.getpass(prompt=prompt)
    return password.encode('utf-8')
    
    
def daemon_main(host, port, tls, ipc_port, debug, traceur, cache_dir, password,
                aengel=None, user_id=None, startup_logger=None,
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
    if startup_logger is not None:
        logger = startup_logger
        
    debug = bool(debug)
    # Note: this isn't currently used.
    traceur = bool(traceur)
    
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
        logger.info(
            'Identity created. Your user_id is ' + str(user_id) + '.'
        )
        
    # Hey look, we have an existing user.
    else:
        core.bootstrap(
            user_id = user_id,
            password = password,
            _scrypt_hardness = _scrypt_hardness,
        )
        logger.info('Login successful.')
    
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
        
    return persister, core, aengel


if __name__ == '__main__':
    # Get busy workin'
    with Daemonizer() as (is_setup, daemonizer):
        if is_setup:
            parser = argparse.ArgumentParser(
                description = 'Start the Hypergolix service.'
            )
            parser.add_argument(
                '--host',
                action = 'store',
                default = 'localhost',
                type = str,
                help = 'Specify the persistence provider host [default: ' +
                       'localhost]'
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
                help = 'Specify the directory to use for on-disk caching, ' +
                       'relative to the current path. Defaults to the ' +
                       'current directory.'
            )
            parser.add_argument(
                '--logdir',
                action = 'store',
                default = None,
                type = str,
                help = 'Log to a specified director, relative to current path.'
            )
            parser.add_argument(
                '--userid',
                action = 'store',
                default = None,
                type = str,
                help = 'Specifies a Hypergolix login user. If omitted, ' +
                       'creates a new account.',
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
                logdir = pathlib.Path(args.logdir).absolute().as_posix()
                logutils.autoconfig(
                    tofile = True,
                    logdirname = logdir,
                    loglevel = args.verbosity
                )
            else:
                logdir = None
                logutils.autoconfig(
                    tofile = False,
                    loglevel = args.verbosity
                )
                
            # Create an account
            if args.userid is None:
                user_id = None
                password = _create_password()
                
            # Log in to existing account
            else:
                user_id = Ghid.from_str(args.userid)
                password = _enter_password()
                
            startup_logging_port = 7771
            
            args = vars(args)
            
        else:
            args = {}
            args['host'] = None
            args['port'] = None
            args['notls'] = None
            args['ipcport'] = None
            args['debug'] = None
            args['traceur'] = None
            args['cachedir'] = None
            args['verbosity'] = None
            password = None
            user_id = None
            startup_logging_port = None
            logdir = None
            
        (
            is_parent, pid_file, host, port, tls, ipc_port, debug, traceur,
            cache_dir, password, user_id, startup_port, logdir, verbosity
        ) = daemonizer(
            '/ghidcache/logs/hypergolix.pid',
            '/ghidcache/logs/hypergolix.pid',
            args['host'],
            args['port'],
            not args['notls'],
            args['ipcport'],
            args['debug'],
            args['traceur'],
            args['cachedir'],
            password,
            user_id,
            startup_logging_port,
            logdir,
            args['verbosity'],
            strip_cmd_args = True
        )
         
        if is_parent:
            print('listening')
            # Set up a logging server that we can print() to the terminal
            _startup_listener(
                port = startup_logging_port,
                timeout = 60
            )
            
            
                
    # Daemonized child only from here on out.
            
    try:
        with _StartupReporter(startup_port) as startup_logger:
            try:
                if logdir is not None:
                    logutils.autoconfig(
                        tofile = True,
                        logdirname = logdir,
                        loglevel = verbosity
                    )
                else:
                    logutils.autoconfig(
                        tofile = False,
                        loglevel = verbosity
                    )
                
                persister, core, aengel = daemon_main(
                    host = host,
                    port = port,
                    tls = tls,
                    ipc_port = ipc_port,
                    debug = debug,
                    traceur = traceur,
                    cache_dir = cache_dir,
                    password = password,
                    user_id = user_id,
                    startup_logger = startup_logger
                )
            except:
                startup_logger.critical(
                    'exception: \n' + ''.join(traceback.format_exc())
                )
        
        sighandler = SignalHandler1(pid_file)
        sighandler.start()
        
        # Wait indefinitely until signal caught.
        threading_autojoin()
         
    except:
        import os
        pid = os.getpid()
        with open('/ghidcache/logs/' + str(pid) + '.txt', 'w') as f:
            f.write(''.join(traceback.format_exc()))
        raise
