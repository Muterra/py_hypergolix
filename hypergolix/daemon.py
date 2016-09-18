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
import http
import socket
import socketserver
import multiprocessing
import time
import urllib
import pickle

from golix import Ghid

from daemoniker import Daemonizer
from daemoniker import SignalHandler1

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
logging.raiseExceptions = True
logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    # 'Inquisitor',
]


# ###############################################
# Library
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
        
    @staticmethod
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
                
    @staticmethod
    def _close_server(port, timeout):
        ''' Sends the logging server a DELETE request, notifying it that
        logging has been completed.
        '''
        return True
        log_server = http.client.HTTPConnection(
            '127.0.0.1',
            port,
            timeout = timeout
        )
        
        try:
            # This will automatically connect
            log_server.request('DELETE', '/')
            
        except socket.timeout:
            logger.warning('Logging server not available to close.')
        
    def __enter__(self):
        ''' Sets up the logging reporter.
        '''
        # Wait for the server to exist first.
        logging_port = self.port
        
        try:
            self._await_server(logging_port, self._cycle_time, self._timeout)

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
                self._close_server(self.port, self._timeout)
            
            
# class _PicklingStreamHandler(socketserver.StreamRequestHandler):
#     ''' Handler for emitting received logs.
#     '''
    
#     def handle(self):
#         ''' Handle a single connection.
#         '''
#         try:
#             print('got connection.')
#             try:
#                 request = pickle.load(self.rfile)
            
#             except EOFError:
#                 logger.info('EOF error while handling logging connection.')
                
#             else:
#                 print(request)
            
#         finally:
#             self.rfile.close()
#             self.wfile.write(b'\x01')
        
        
# class _TimeoutingServer(socketserver.TCPServer):
#     ''' Server that stops serving when a timeout has occurred.
#     '''
    
#     def serve_until_timeout(self):
#         self.__timeout_stop = False
#         while not self.__timeout_stop:
#             self.handle_request()
            
#     def handle_timeout(self):
#         self.__timeout_stop = True
#         super().handle_timeout()
        
#     def force_quit(self):
#         self.__timeout_stop = True
        
#     def shutdown(self):
#         # Just in case we're serve_until_timeouting
#         self.__timeout_stop = True
#         super().shutdown()
        
        
# def _startup_listener(port, timeout):
#     server_address = ('127.0.0.1', port)
#     server = _TimeoutingServer(server_address, _PicklingStreamHandler)
#     server.timeout = timeout
    
#     try:
#         server.serve_until_timeout()
        
#     finally:
#         server.server_close()
        
        
def _handle_connection(conn, timeout):
        try:
            print('got request. polling.')
            while True:
                if conn.poll(timeout):
                    try:
                        request = conn.recv()
                        print(request)
                    
                    except EOFError:
                        print('eoferror.')
                        return
                        
                else:
                    print('timeout.')
                    return
            
        finally:
            conn.close()
            

def _serve(server, timeout):
    for __ in range(3):
        with server.accept() as conn:
            _handle_connection(conn, timeout)
        
        
def _startup_listener(port, timeout):
    server_address = ('127.0.0.1', port)
    
    # TODO: add timeout for safety
    with multiprocessing.connection.Listener(server_address) as server:
        _serve(server, timeout)
    
    
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
        
    return persister, core


if __name__ == '__main__':
    # Get busy workin'
    with Daemonizer as (is_setup, daemonizer):
        if is_setup:
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
                
            # Create an account
            if args.userid is None:
                user_id = None
                password = _create_password()
                
            # Log in to existing account
            else:
                user_id = Ghid.from_str(args.userid)
                password = _enter_password()
                
            startup_logging_port = 7771
            
        (is_parent, host, port, tls, ipc_port, debug, traceur, cache_dir,
         password, user_id, startup_port) = daemonizer(
            './hypergolix.pid',
            args.host,
            args.port,
            not args.notls,
            args.ipcport,
            args.debug,
            args.traceur,
            args.cachedir,
            password,
            user_id,
            startup_logging_port,
            strip_cmd_args = True
        )
         
        if is_parent:
            # Set up a logging server that we can print() to the terminal
            pass
    
    with _StartupReporter(startup_logging_port) as startup_logger:
        daemon_main(
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
    
    sighandler = SignalHandler1()
    sighandler.start()
    
    # Wait indefinitely until signal caught.
    threading_autojoin()
