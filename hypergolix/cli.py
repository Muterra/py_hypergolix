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

import argparse


# ###############################################
# Command-specific entry points
# ###############################################


from hypergolix.service import start as start_server
from hypergolix.service import stop as stop_server

from hypergolix.daemon import start as start_app
from hypergolix.daemon import stop as stop_app

from hypergolix.config import handle_args as config
from hypergolix.config import NAMED_REMOTES


# ###############################################
# Root parsers
# ###############################################


root_parser = argparse.ArgumentParser(
    description = 'Hypergolix -- "programmable Dropbox" -- runs as a ' +
                  'background service for internet-connected applications. ' +
                  'Run and configure the Hypergolix daemon, or start or ' +
                  'stop a Hypergolix server, with this command.',
    prog = 'hypergolix'
)
subparsers = root_parser.add_subparsers()


start_parser = subparsers.add_parser(
    'start',
    help = 'Start a Hypergolix app or server daemon.',
    prog = 'hypergolix start'
)
start_subparsers = start_parser.add_subparsers()


stop_parser = subparsers.add_parser(
    'stop',
    help = 'Stop an existing Hypergolix app or server daemon.',
    prog = 'hypergolix stop'
)
stop_subparsers = stop_parser.add_subparsers()


config_parser = subparsers.add_parser(
    'config',
    help = 'Configure the Hypergolix app.',
    prog = 'hypergolix config'
)
# config_subparsers = config_parser.add_subparsers()


# ###############################################
# App start parser (hypergolix start app)
# ###############################################


app_start_parser = start_subparsers.add_parser(
    'app',
    help = 'Start the Hypergolix app daemon.'
)
app_start_parser.set_defaults(entry_point=start_app)


# ###############################################
# Server start parser (hypergolix start server)
# ###############################################


server_start_parser = start_subparsers.add_parser(
    'server',
    help = 'Start the Hypergolix server daemon.',
    prog = 'hypergolix start server'
)
server_start_parser.set_defaults(entry_point=start_server)
    
server_start_parser.add_argument(
    'pidfile',
    action = 'store',
    type = str,
    help = 'The full path to the PID file we should use for the service.'
)
server_start_parser.add_argument(
    '--cachedir', '-c',
    action = 'store',
    required = True,
    dest = 'cachedir',
    default = None,
    type = str,
    help = 'Specify a directory to use as a persistent cache for files. ' +
           'If none is specified, will default to an in-memory-only ' +
           'cache, which is, quite obviously, rather volatile.'
)
server_start_parser.add_argument(
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
server_start_parser.add_argument(
    '--port', '-p',
    action = 'store',
    dest = 'port',
    default = 7770,
    type = int,
    help = 'Specify the TCP port to use. Defaults to 7770.'
)
server_start_parser.add_argument(
    '--chdir',
    action = 'store',
    default = None,
    type = str,
    help = 'Once the daemon starts, chdir it into the specified full ' +
           'directory path. By default, the daemon will remain in the ' +
           'current directory, which may create DirectoryBusy errors.'
)
server_start_parser.add_argument(
    '--logdir',
    action = 'store',
    default = None,
    type = str,
    help = 'Specify a directory to use for logs. Every service failure, ' +
           'error, message, etc will go to dev/null without this.'
)
server_start_parser.add_argument(
    '--debug',
    action = 'store_true',
    help = 'Enable debug mode. Sets verbosity to debug unless overridden.'
)
server_start_parser.add_argument(
    '--traceur',
    action = 'store_true',
    help = 'Enable thorough analysis, including stack tracing. '
           'Implies verbosity of debug.'
)
server_start_parser.add_argument(
    '--verbosity', '-V',
    action = 'store',
    dest = 'verbosity',
    type = str,
    choices = ['debug', 'info', 'warning', 'error', 'shouty', 'extreme'],
    default = None,
    help = 'Sets the log verbosity. Only applicable if --logdir is set.'
)


# ###############################################
# App stop parser (hypergolix stop app)
# ###############################################


app_stop_parser = stop_subparsers.add_parser(
    'app',
    help = 'Stop the Hypergolix app daemon.',
    prog = 'hypergolix stop server'
)
app_stop_parser.set_defaults(entry_point=stop_app)


# ###############################################
# Server stop parser (hypergolix stop server)
# ###############################################


server_stop_parser = stop_subparsers.add_parser(
    'server',
    help = 'Stop the Hypergolix server daemon.'
)
server_stop_parser.set_defaults(entry_point=stop_server)
    
server_stop_parser.add_argument(
    'pidfile',
    action = 'store',
    type = str,
    help = 'The full path to the PID file we should use for the service.'
)


# ###############################################
# Config parser (hypergolix config)
# ###############################################


config_parser.set_defaults(entry_point=config)


config_parser.add_argument(
    '--root',
    action = 'store',
    type = str,
    help = 'Manually specify the Hypergolix root directory.',
    dest = 'cfg_root',
    default = None
)


# Remotes config
# -----------------------------------------------
remote_group = config_parser.add_argument_group(
    title = 'Remotes configuration',
    description = 'Specify which remote persistence servers Hypergolix ' +
                  'should use.'
)

# Exclusive autoconfig (also, clear all hosts)
remote_group.add_argument(
    '--only', '-o',
    action = 'store',
    type = str,
    help = 'Automatically configure Hypergolix to use only a single, ' +
           'named remote (or no remote). Does not affect the remainder ' +
           'of the configuration.',
    choices = ['local', *NAMED_REMOTES],
    dest = 'only_remotes',
    default = None,
)

# Auto-add
remote_group.add_argument(
    '--add', '-a',
    action = 'append',
    type = str,
    help = 'Add a named remote to the Hypergolix configuration. Cannot ' +
           'be combined with --only.',
    choices = NAMED_REMOTES,
    dest = 'add_remotes'
)

# Auto-remove
remote_group.add_argument(
    '--remove', '-r',
    action = 'append',
    type = str,
    help = 'Remove a named remote from the Hypergolix configuration. ' +
           'Cannot be combined with --only.',
    choices = NAMED_REMOTES,
    dest = 'remove_remotes'
)

# Manually add a host
remote_group.add_argument(
    '--addhost', '-ah',
    action = 'append',
    type = str,
    help = 'Add a remote host, of form "hostname port use_TLS". Example ' +
           'usage: "hypergolix.config --adhost 192.168.0.1 7770 False". ' +
           'Cannot be combined with --only.',
    nargs = 3,
    metavar = ('HOST', 'PORT', 'TLS'),
    dest = 'add_remotes'
)

# Manually remove a host
remote_group.add_argument(
    '--removehost', '-rh',
    action = 'append',
    type = str,
    help = 'Remove a remote host, of form "hostname port". Example ' +
           'usage: "hypergolix.config --removehost 192.168.0.1 7770". ' +
           'Cannot be combined with --only.',
    nargs = 2,
    metavar = ('HOST', 'PORT'),
    dest = 'remove_remotes'
)

# Set defaults for those two as well
config_parser.set_defaults(remove_remotes=[], add_remotes=[])


# Runtime config
# -----------------------------------------------

runtime_group = config_parser.add_argument_group(
    title = 'Runtime configuration',
    description = 'Specify Hypergolix runtime options.'
)

# Set debug mode.
# Make the debug parser a mutually exclusive group with flags.
debug_parser = runtime_group.add_mutually_exclusive_group(required=False)
debug_parser.add_argument(
    '--debug',
    action = 'store_true',
    dest = 'debug',
    help = 'Enables debug mode.'
)
debug_parser.add_argument(
    '--no-debug',
    action = 'store_false',
    dest = 'debug',
    help = 'Clears debug mode.'
)
config_parser.set_defaults(debug=None)

# Set verbosity
runtime_group.add_argument(
    '--verbosity', '-v',
    action = 'store',
    default = 'normal',
    type = str,
    choices = ['extreme', 'shouty', 'louder', 'loud', 'normal', 'quiet'],
    help = 'Specify the logging level.'
)

# Set verbosity
runtime_group.add_argument(
    '--ipc-port', '-ipc',
    action = 'store',
    default = None,
    type = int,
    help = 'Configure which port to use for Hypergolix IPC.',
    metavar = 'PORT'
)


# Etc
# -----------------------------------------------

etc_group = config_parser.add_argument_group(
    title = 'Miscellaneous commands'
)

# Set verbosity
etc_group.add_argument(
    '--whoami',
    action = 'store_true',
    help = 'Print the fingerprint and user ID for the current ' +
           'Hypergolix configuration.'
)

# Set verbosity
etc_group.add_argument(
    '--register',
    action = 'store_true',
    help = 'Register the current Hypergolix user, allowing them access ' +
           'to the hgx.hypergolix.com remote persister. Requires a web ' +
           'browser.'
)


# ###############################################
# Master entry point (hypergolix)
# ###############################################


def main(argv=None):
    ''' Entry point for all command line stuff.
    '''
    # This allows us to test with an explicit argstring instead of through the
    # command line only
    args = root_parser.parse_args(args=argv)
    
    try:
        # This invokes the entry point with the parsed args
        args.entry_point(args)
    
    except AttributeError:
        # Let the invoker know that no command was selected
        print('Invalid command selected. Type "hypergolix -h" for usage.')
        
    except Exception as exc:
        root_parser.error(str(exc))
    
    
if __name__ == '__main__':
    # We now return to your regularly scheduled programming
    main()
