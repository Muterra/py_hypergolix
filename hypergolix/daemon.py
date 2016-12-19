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
import traceback
import collections
import logging
import logging.handlers
import textwrap

import daemoniker
from daemoniker import Daemonizer
from daemoniker import SignalHandler1
from daemoniker import SIGTERM

from Crypto.Protocol.KDF import scrypt

from golix import FirstParty
from golix import Secret
from golix import Ghid

# Intra-package dependencies (that require explicit imports, courtesy of
# daemonization)

from hypergolix.config import Config
from hypergolix.config import get_hgx_rootdir

from hypergolix.comms import WSConnection
from hypergolix.app import HypergolixCore
from hypergolix.accounting import Account

from hypergolix import logutils

from hypergolix.exceptions import ConfigError


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)

# Control * imports.
__all__ = [
    'start',
]


# Use 2**14 for t<=100ms, 2**20 for t<=5s.
_DEFAULT_SCRYPT_HARDNESS = 2**15


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
        self._running = False
        self.port = port
        self.handler = None
        
        self._cycle_time = cycle_time
        self._timeout = timeout
        
    def start(self):
        ''' Direct call to start reporting.
        '''
        # Wait for the server to exist first.
        logging_port = self.port
        
        self._running = True
        
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
                    '+-- %(message)s'
                )
            )
            
        # Assign a filter to chill the noise
        self.handler.addFilter(_BootstrapFilter())
        
        # Enable the handler for hypergolix.bootstrapping
        bootstrap_logger = logging.getLogger('hypergolix.accounting')
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
        
    def stop(self):
        ''' Explicit call to close the logger.
        '''
        if self._running:
            try:
                root_logger = logging.getLogger('')
                bootstrap_logger = logging.getLogger('hypergolix.accounting')
                
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
                
                self._running = False
        
    def __enter__(self):
        ''' Sets up the logging reporter.
        '''
        return self.start()
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        ''' Restores the bootstrap process logging to its previous
        verbosity and removes the handler.
        '''
        try:
            root_logger = logging.getLogger('')
            
            # Well first, if we aren't cleanly exiting, report the error.
            if exc_type is not None:
                root_logger.error(
                    'Exception during startup: ' + str(exc_type) + '(' +
                    str(exc_value) + ') + \n' +
                    ''.join(traceback.format_tb(exc_tb))
                )
        
        finally:
            self.stop()
                

# Customize what bullet to use for which loglevel
def _get_bullet_from_loglevel(loglevel):
    lookup = [
        (logging.DEBUG,     '--- '),
        (logging.INFO,      '+-- '),
        (logging.WARNING,   '!-- '),
        (logging.ERROR,     '!!! ')
    ]
    
    # Return the first bullet that is at least our level
    for check_level, bullet in lookup:
        if loglevel <= check_level:
            return bullet
    # If the logging level is higher than the highest bullet level, return the
    # last (aka the highest) bullet level
    else:
        return bullet
        
        
def _handle_startup_connection(conn, timeout):
        try:
            # Loop forever until the connection is closed.
            while not conn.closed:
                if conn.poll(timeout):
                    try:
                        request = conn.recv()
                        # Add a visual indicator of new message
                        bullet = _get_bullet_from_loglevel(request['levelno'])
                        indent = len(bullet) * ' '
                        # Wrap each line to 70 (indent 4, text 66) chars
                        lines = textwrap.wrap(
                            request['msg'],
                            width = 70 - len(indent)
                        )
                        # ...and indent all following lines appropriately
                        indents = [bullet] + ([indent] * (len(lines) - 1))
                        # Interleave the indentation with the message core and
                        # condense into single str
                        msg = '\n'.join(
                            [indent + s for indent, s in zip(indents, lines)]
                        )
                        print(msg)
                    
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
# Password stuff
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
        
        
def _expand_password(salt_ghid, password, hardness=None):
    ''' Expands the author's ghid and password into a master key for
    use in generating specific keys.
    
    Hardness allows you to modify the scrypt inflation parameter. It
    defaults to something resembling a reasonable general-purpose
    value for 2016.
    '''
    # Use 2**14 for t<=100ms, 2**20 for t<=5s.
    if hardness is None:
        hardness = _DEFAULT_SCRYPT_HARDNESS
    else:
        hardness = int(hardness)
    
    # Scrypt the password. Salt against the author GHID.
    combined = scrypt(
        password = password,
        salt = bytes(salt_ghid),
        key_len = 48,
        N = hardness,
        r = 8,
        p = 1
    )
    key = combined[0:32]
    seed = combined[32:48]
    master_secret = Secret(
        cipher = 1,
        version = 'latest',
        key = key,
        seed = seed
    )
    return master_secret


# ###############################################
# Actionable intelligence
# ###############################################


class _DaemonCore(HypergolixCore):
    ''' We just want a tiny addition to HypergolixCore to stop bootup
    logging and save the config if needed. Also, because I like making
    references to historical minutiae.
    '''
    
    def __init__(self, *args, hgx_rootdir, save_cfg, boot_logger, **kwargs):
        super().__init__(*args, **kwargs)
        
        self._hgx_rootdir = hgx_rootdir
        self._save_cfg = bool(save_cfg)
        self._boot_logger = boot_logger
        
    async def setup(self):
        ''' Extend setup to also close the boot logger and, if desired,
        save the config.
        '''
        await super().setup()
        
        try:
            if self._save_cfg:
                user_id = self.account._user_id
                fingerprint = self.account._fingerprint
                
                logger.critical('Account created. Record these values in ' +
                                'case you need to log in to Hypergolix from ' +
                                'another machine, or in case your config ' +
                                'file is corrupted or lost:')
                logger.critical('User ID:\n' + user_id.as_str())
                logger.critical('Fingerprint:\n' + fingerprint.as_str())
                
                with Config(self._hgx_rootdir) as config:
                    config.fingerprint = fingerprint
                    config.user_id = user_id
                    
        finally:
            logger.critical('Hypergolix boot complete.')
            self._boot_logger.stop()


def do_setup():
    ''' Does initial setup of the daemon BEFORE daemonizing.
    '''
    hgx_rootdir = get_hgx_rootdir()
    
    with Config(hgx_rootdir) as config:
        user_id = config.user_id
        fingerprint = config.fingerprint
        root_secret = config.root_secret
        # Convert the path to a str
        pid_file = str(config.pid_file)
    
    if bool(user_id) ^ bool(fingerprint):
        raise ConfigError('Invalid config. Config must declare both ' +
                          'user_id and fingerprint, or neither.')
    
    # We have no root secret, so we need to get a password and then inflate
    # it.
    if not root_secret:
        
        # We have an existing account, so do a single prompt.
        if user_id:
            account_entity = user_id
            password = _enter_password()
        
        # We have no existing account, so do a double prompt (and then
        # generate keys) and then inflate the password.
        else:
            password = _create_password()
            print('Generating a new set of private keys. This may take ' +
                  'a while.')
            account_entity = FirstParty()
            fingerprint = account_entity.ghid
            account_entity = account_entity._serialize()
            print('Private keys generated.')
            
        print('Expanding password using scrypt. This may take a while.')
        root_secret = _expand_password(
            salt_ghid = fingerprint,
            password = password
        )
    
    # We have a root secret...
    else:
        print('Using stored secret.')
        
        # ...and an existing account
        if user_id:
            account_entity = user_id
            
        # ...but need a new account
        else:
            print('Generating a new set of private keys. This may take ' +
                  'a while.')
            account_entity = FirstParty()
            print('Private keys generated.')
            account_entity = account_entity._serialize()
        
    return hgx_rootdir, pid_file, account_entity, root_secret


def run_daemon(hgx_rootdir, pid_file, parent_port, account_entity,
               root_secret):
    ''' Start the actual Hypergolix daemon.
    '''
    # Start reporting to our parent about how stuff is going.
    parent_signaller = _StartupReporter(parent_port)
    startup_logger = parent_signaller.start()
    
    try:
        with Config(hgx_rootdir) as config:
            # Convert paths to strs
            cache_dir = str(config.cache_dir)
            log_dir = str(config.log_dir)
            debug = config.debug_mode
            verbosity = config.log_verbosity
            ipc_port = config.ipc_port
            remotes = config.remotes
            # Look to see if we have an existing user_id to determine behavior
            save_cfg = not bool(config.user_id)
        
        hgxcore = _DaemonCore(
            cache_dir = cache_dir,
            ipc_port = ipc_port,
            reusable_loop = False,
            threaded = False,
            debug = debug,
            hgx_rootdir = hgx_rootdir,
            save_cfg = save_cfg,
            boot_logger = parent_signaller
        )
        
        for remote in remotes:
            hgxcore.add_remote(
                connection_cls = WSConnection,
                host = remote.host,
                port = remote.port,
                tls = remote.tls
            )
        
        account = Account(
            user_id = account_entity,
            root_secret = root_secret,
            hgxcore = hgxcore
        )
        hgxcore.account = account
        
        logutils.autoconfig(
            tofile = True,
            logdirname = log_dir,
            loglevel = verbosity,
            logname = 'hgxapp'
        )
        
        # We need a signal handler for that.
        def signal_handler(signum):
            logger.info('Caught signal. Exiting.')
            hgxcore.stop_threadsafe_nowait()
            
        # Normally I'd do this within daemonization, but in this case, we need
        # to wait to have access to the handler.
        sighandler = SignalHandler1(
            pid_file,
            sigint = signal_handler,
            sigterm = signal_handler,
            sigabrt = signal_handler
        )
        sighandler.start()
        
        startup_logger.info('Booting Hypergolix...')
        hgxcore.start()
        
    finally:
        # This is idempotent, so no worries if we already called it
        parent_signaller.stop()

    
def start(namespace=None):
    ''' Starts a Hypergolix daemon.
    '''
    with Daemonizer() as (is_setup, daemonizer):
        parent_port = 7771
        
        if is_setup:
            print('Starting Hypergolix...')
            hgx_rootdir, pid_file, account_entity, root_secret = do_setup()
            
        else:
            # Need these so that the second time around doesn't NameError
            hgx_rootdir = None
            pid_file = None
            account_entity = None
            root_secret = None
            
        # Daemonize. Don't strip cmd-line arguments, or we won't know to
        # continue with startup
        is_parent, hgx_rootdir, pid_file, account_entity, root_secret = \
            daemonizer(pid_file, hgx_rootdir, pid_file, account_entity,
                       root_secret, chdir=str(hgx_rootdir))
         
        if is_parent:
            # Set up a logging server that we can print() to the terminal
            _startup_listener(
                port = parent_port,
                timeout = 60
            )
            #####################
            # PARENT EXITS HERE #
            #####################
            
        elif not isinstance(account_entity, Ghid):
            account_entity = FirstParty._from_serialized(account_entity)
            
    # Daemonized child only from here on out. So, run the actual daemon!
    run_daemon(hgx_rootdir, pid_file, parent_port, account_entity, root_secret)
    
    
def stop(namespace=None):
    ''' Stops the Hypergolix daemon.
    '''
    hgx_rootdir = get_hgx_rootdir()
    
    with Config(hgx_rootdir) as config:
        pid_file = str(config.pid_file)
        
    daemoniker.send(pid_file, SIGTERM)


# ###############################################
# Command line stuff
# ###############################################


COMMANDS = collections.OrderedDict((
    ('start', start),
    ('stop', stop)
))


def _ingest_args(argv=None):
    ''' Parse and handle any command-line args.
    '''
    parser = argparse.ArgumentParser(
        prog = 'hypergolix.daemon',
        description = 'Control the Hypergolix app daemon.'
    )
    parser.add_argument(
        'cmd',
        action = 'store',
        type = str,
        choices = COMMANDS,
        help = 'What should we do to the daemon? Note that stop and ' +
               'restart will only work if the daemon is already running.'
    )

    args = parser.parse_args(args=argv)
    return args
        

if __name__ == '__main__':
    namespace = _ingest_args()
    # Invoke the command
    COMMANDS[namespace.cmd.lower()](namespace)
