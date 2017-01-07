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
import pathlib
import json
import collections
import copy
import webbrowser

from golix import Ghid
from golix import Secret

# Intra-package dependencies
from .utils import _BijectDict
from .exceptions import ConfigError


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
# Helper classes and encoder/decoder
# ###############################################
        
        
class _NamedListMeta(type):
    ''' Metaclass for named lists.
    '''
    
    def __new__(metacls, name, bases, clsdict, **kwargs):
        ''' Automatically add any slots declarations (except _fields_ to
        _fields, in order.
        '''
        # Enforce usage of __slots__
        if '__slots__' not in clsdict:
            raise TypeError('_NamedLists must use slots.')
            
        # Enforce non-usage of '_fields' attr. Note that this will only apply
        # to this particular subclass, but it won't matter because all of them
        # use us as a metaclass.
        elif '_fields' in clsdict['__slots__']:
            raise TypeError(
                '_NamedLists cannot define a "_fields" attribute.'
            )
            
        # Enforce not defining fields in the class definition as well
        elif '_fields' in clsdict:
            raise TypeError(
                '_NamedLists cannot define a _fields class attribute.'
            )
            
        # Now add '_fields' to the class dict separately and create the class
        clsdict['_fields'] = []
        cls = super().__new__(metacls, name, bases, clsdict, **kwargs)
        
        # Now modify cls._fields according to the MRO, adding all applicable
        # slots.
        
        # Now we need to rewrite slots, collating everything into fields.
        # Prepend '_fields' to __slots__ and convert it to a tuple
        clsdict['__slots__'] = ('_fields', *clsdict['__slots__'])
        
        # And now add all of the __slots__ to _fields, in order of their MRO
        fields = []
        # Create a version of the MRO that ignores object, which doesn't define
        # __slots__
        stub_mro = cls.__mro__[:len(cls.__mro__) - 1]
        for c in stub_mro:
            # Add any fields that are not already defined there
            fields.extend([slot for slot in c.__slots__ if slot not in fields])
        # And assign that to cls._fields
        cls._fields = tuple(fields)
        
        # Don't forget to return the finalized class!
        return cls
            
    def __len__(cls):
        ''' Use the number of _fields for the class length.
        '''
        return len(cls._fields)


# Okay, normally I'd do these as collections.namedtuples, but those are being
# interpreted by json as tuples, so no dice.
class _NamedList(metaclass=_NamedListMeta):
    ''' Some magic to simulate a named tuple in a way that doesn't
    subclass tuple, and is therefore correctly interpreted by json. As
    an implementation side effect, this is also mutable, hence being a
    _NamedList and not _NamedTuple2.
    
    This is always a fixed-length entity. Additionally, though they may
    be modified, attributes may not be added, nor deleted.
    '''
    __slots__ = []
    __hash__ = None
    
    def __init__(self, *args, **kwargs):
        ''' Pass all *args or **kwargs to _fields.
        '''
        for ii, arg in enumerate(args):
            self[ii] = arg
            
        for key, value in kwargs.items():
            # Check to see if the attr was defined by args
            if hasattr(self, key):
                raise TypeError(
                    'Got multiple values for keyword "' + key + '"'
                )
                
            else:
                setattr(self, key, value)
                
        for field in self._fields:
            if not hasattr(self, field):
                raise TypeError('Must define all attributes to a _NamedList.')
                
    def __setitem__(self, index, value):
        ''' Convert key-based (index) access to attr access.
        '''
        attrname = self._fields[index]
        setattr(self, attrname, value)
        
    def __getitem__(self, index):
        ''' Convert key-based (index) access to attr access.
        '''
        attrname = self._fields[index]
        return getattr(self, attrname)
        
    def __repr__(self):
        ''' Also add a nice repr for all of the fields.
        '''
        clsname = type(self).__name__
        
        fieldstrs = []
        for field in self._fields:
            fieldstrs.append(field)
            fieldstrs.append('=')
            fieldstrs.append(repr(getattr(self, field)))
            fieldstrs.append(', ')
        # Strip the final ', '
        fieldstrs = fieldstrs[:len(fieldstrs) - 1]
            
        return ''.join((clsname, '(', *fieldstrs, ')'))
        
    def __iter__(self):
        ''' Needed to, yknow, iterate and stuff.
        
        Note that iterating will only work if all attrs are defined.
        '''
        for field in self._fields:
            yield getattr(self, field)
            
    def __reversed__(self):
        ''' Performs the same checks as __iter__.
        '''
        for field in reversed(self._fields):
            yield getattr(self, field)
        
    def __contains__(self, value):
        # Iterate over all possible fields.
        for field in self._fields:
            # If the field is defined, and the values match, it's here.
            if value == field:
                return True
        # Gone through everything without returning? Not contained.
        else:
            return False
            
    def __len__(self):
        ''' Statically defined as the length of the _fields classattr.
        '''
        return len(self._fields)
        
    def __eq__(self, other):
        ''' The usual equality test.
        '''
        # Ensure same lengths
        if len(other) != len(self):
            return False
            
        # Compare every value and short-circuit on failure
        for mine, theirs in zip(self, other):
            if mine != theirs:
                return False
        
        # Nothing mismatched, both are same length, must be equal
        else:
            return True


class _RemoteDef(_NamedList):
    __slots__ = [
        'host',
        'port',
        'tls'
    ]


class _UserDef(_NamedList):
    __slots__ = [
        'fingerprint',
        'user_id',
        'root_secret'
    ]


class _InstrumentationDef(_NamedList):
    __slots__ = [
        'verbosity',
        'debug',
        'traceur'
    ]


class _ProcessDef(_NamedList):
    __slots__ = [
        'ipc_port'
    ]


# Using a bijective mapping allows us to do bidirectional lookup
# This might be overkill, but if you already have one in your .utils module...
_TYPEHINTS = _BijectDict({
    '__RemoteDef__': _RemoteDef,
    '__UserDef__': _UserDef,
    '__InstrumentationDef__': _InstrumentationDef,
    '__ProcessDef__': _ProcessDef
})


class _CfgDecoder(json.JSONDecoder):
    ''' Extends the default json decoder to create the relevant objects
    from the cfg file.
    '''
    
    def __init__(self):
        ''' Hard-code the super() invocation.
        '''
        super().__init__(object_hook=self._ohook)
        
    def _ohook(self, odict):
        ''' Called for every dict (json object) encountered.
        '''
        for key in odict:
            if key in _TYPEHINTS:
                # Get the class to use
                cls = _TYPEHINTS[key]
                # Pop out the key
                odict.pop(key)
                # Create an instance of the class, expanding the rest of the
                # dict to be kwargs
                return cls(**odict)
                
        else:
            return odict
    
    
class _CfgEncoder(json.JSONEncoder):
    ''' Extends the default json encoder to allow parsing the cfg
    objects into json.
    '''
    
    def __init__(self):
        ''' Hard-code in the super() invocation.
        '''
        # Make the cfg file as human-readable as possible
        super().__init__(indent=4)
        
    def default(self, obj):
        ''' Allow for encoding of our helper objects. Note that this is
        class-strict, IE subclasses must be explicitly supported.
        '''
        try:
            type_hint = _TYPEHINTS[type(obj)]
            
        # Unknown type. Pass TypeError raising to super().
        except KeyError:
            odict = super().default(obj)
            
        else:
            # Convert all attributes into dictionary keys
            odict = {key: getattr(obj, key) for key in obj._fields}
            # Add a type hint, but make sure to error if the field is already
            # defined (fail loud, fail fast)
            if type_hint in odict:
                raise ValueError(
                    'The type hint key cannot match any attribute names for ' +
                    'the object instance.'
                )
            odict[type_hint] = True
        
        return odict


# ###############################################
# Library
# ###############################################
            

_readonly_remote = collections.namedtuple(
    typename = 'Remote',
    field_names = ('host', 'port', 'tls'),
)


class Config:
    ''' Context handler for semi-atomic config updates.
    
    .hypergolix /
    
    +---logs
        +---(log file 1...)
        +---(log file 2...)
        
    +---ghidcache
        +---(ghid file 1...)
        +---(ghid file 2...)
        
    +---(hgx.pid)
    +---(hgx-cfg.json)
    '''
    
    def __init__(self, root):
        self._root = pathlib.Path(root).absolute()
        self._cfg_cache = None
        self._cfg = None
    
    def __enter__(self):
        ''' Gets a configuration for hypergolix (if one exists), and
        creates a new one (with no remote persistence servers) if none
        is available.
        '''
        try:
            # Cache the existing configuration
            self._cfg_cache = _get_hgx_config(self._root)
            # Create a copy for modifications (a second name for the mutable
            # self._cfg_cache object will always compare equal)
            self._cfg = copy.deepcopy(self._cfg_cache)
            
        except ConfigError:
            self._cfg = _make_blank_cfg()
            
        # And now allow access to self.
        return self
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        ''' Save any changes to configuration (including creation of a
        new configuration).
        '''
        # Only modify if there were no errors; never do a partial update.
        if exc_type is None:
            if self._cfg_cache != self._cfg:
                _set_hgx_config(self._root, self._cfg)
                
    @property
    def home_dir(self):
        ''' The Hypergolix home directory.
        '''
        return self._root / '.hypergolix'
        
    @property
    def cache_dir(self):
        ''' Where is the cache dir?
        '''
        return self.home_dir / 'ghidcache'
        
    @property
    def log_dir(self):
        ''' Where is the log dir?
        '''
        return self.home_dir / 'logs'
        
    @property
    def pid_file(self):
        ''' The pid file to use.
        '''
        return self.home_dir / 'hypergolix.pid'
            
    @property
    def remotes(self):
        ''' Returns a read-only copy of all current remotes.
        '''
        try:
            # Convert all of the defs to namedtuples while we're at it
            # Check out this sexy tuple comprehension
            return tuple(
                _readonly_remote(*remote) for remote in self._cfg['remotes']
            )
        
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
            
    def set_remote(self, host, port, tls=True):
        ''' Handles creation of _RemoteDef instances and insertion into
        our config. Will also update TLS configuration of existing
        remotes.
        '''
        rdef = _RemoteDef(host, port, tls)
        _set_remote(self._cfg, rdef)
        
    def remove_remote(self, host, port):
        ''' Removes an existing remote. Silently does nothing if it does
        not exist in the config.
        '''
        # TLS does not matter when removing stuff.
        rdef = _RemoteDef(host, port, False)
        _pop_remote(self._cfg, rdef)
        
    @property
    def fingerprint(self):
        ''' The fingerprint! Use this for a sharing target.
        '''
        try:
            fingerprint = self._cfg['user'].fingerprint
            
            # May be undefined, in which case return None
            if fingerprint is None:
                return fingerprint
                
            # Convert to a ghid if defined.
            else:
                return Ghid.from_str(fingerprint)
        
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
        
    @fingerprint.setter
    def fingerprint(self, fingerprint):
        ''' Set our fingerprint. Really only intended to be called by
        hypergolix itself, and not for manual manipulation of the actual
        config file.
        '''
        # Convert the ghid to a plaintext equivalent
        try:
            fingerprint = fingerprint.as_str()
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
        
        try:
            self._cfg['user'].fingerprint = fingerprint
        
        except KeyError:
            self._cfg['user'] = _UserDef(
                fingerprint = fingerprint,
                user_id = None,
                root_secret = None
            )
        
    @property
    def user_id(self):
        ''' Gets the user_id from the config. Returns a ghid.
        '''
        try:
            user_id = self._cfg['user'].user_id
            
            # May be undefined, in which case return None
            if user_id is None:
                return user_id
                
            # Convert to a ghid if defined.
            else:
                return Ghid.from_str(user_id)
        
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
            
    @user_id.setter
    def user_id(self, user_id):
        ''' Sets the user_id in the config, overwriting any existing
        user_id.
        '''
        # Convert the ghid to a plaintext equivalent
        try:
            user_id = user_id.as_str()
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
        
        try:
            self._cfg['user'].user_id = user_id
        
        except KeyError:
            self._cfg['user'] = _UserDef(
                fingerprint = None,
                user_id = user_id,
                root_secret = None
            )
            
    @property
    def root_secret(self):
        ''' Read-only property that allows people to set a root secret
        for automatic login on startup. Intended for use via sudo on
        fully-autonomous things (ex: a raspberry pi). Can only be set
        through manual manipulation of the config file. Really should
        not be used until hypergolix daemonization supports privilege
        dropping.
        '''
        try:
            cfg_str = self._cfg['user'].root_secret
            
            if cfg_str is None:
                return None
            else:
                return Secret.from_str(cfg_str)
        
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
            
    @property
    def log_verbosity(self):
        ''' Tells the log verbosity.
        '''
        try:
            return self._cfg['instrumentation'].verbosity
        
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
    
    @log_verbosity.setter
    def log_verbosity(self, verbosity):
        ''' Updates log verbosity.
        '''
        try:
            self._cfg['instrumentation'].verbosity = verbosity
        
        except KeyError:
            self._cfg['instrumentation'] = _InstrumentationDef(
                verbosity = verbosity,
                debug = False,
                traceur = False
            )
            
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
            
    @property
    def debug_mode(self):
        ''' Gets the debug mode.
        '''
        try:
            return bool(self._cfg['instrumentation'].debug)
            
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
        
    @debug_mode.setter
    def debug_mode(self, enabled):
        ''' Updates the debug mode.
        '''
        try:
            enabled = bool(enabled)
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
        
        try:
            self._cfg['instrumentation'].debug = enabled
        except KeyError:
            self._cfg['instrumentation'] = _InstrumentationDef(
                verbosity = 'warning',
                debug = enabled,
                traceur = False
            )
            
    @property
    def ipc_port(self):
        ''' Gets the IPC port.
        '''
        try:
            return int(self._cfg['process'].ipc_port)
            
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
        
    @ipc_port.setter
    def ipc_port(self, port):
        ''' Updates the debug mode.
        '''
        try:
            port = int(port)
        except Exception as exc:
            raise ConfigError('Invalid configuration.') from exc
        
        try:
            self._cfg['process'].ipc_port = port
        except KeyError:
            self._cfg['process'] = _ProcessDef(
                ipc_port = port
            )
            
            
def _make_blank_cfg():
    ''' Creates a new, blank cfg dict.
    '''
    cfg = {
        'remotes': [],
        'user': _UserDef(None, None, None),
        'instrumentation': _InstrumentationDef('warning', False, False),
        'process': _ProcessDef(ipc_port=7772)
    }
    return cfg


def get_hgx_rootdir():
    ''' Simply returns the path to the hgx home dir. Does not ensure its
    existence or perform any other checks.
    
    In the future, this will have an order of preference and search
    path, but currently it is quite naively hard-coding a subdirectory
    to the user home directory.
    '''
    # For now, simply make a subdir in the user folder.
    user_home = pathlib.Path('~/')
    user_home = user_home.expanduser()
    return user_home
    
    
def _ensure_dir(path):
    ''' Ensures the existence of a directory. Path must be to the dir,
    and not to a file therewithin.
    '''
    path = pathlib.Path(path).absolute()
    if not path.exists():
        path.mkdir(parents=True)
        
    elif not path.is_dir():
        raise FileExistsError('Path exists already and is not a directory.')
        
        
def _ensure_hgx_populated(hgx_home):
    ''' Generates the folder structure expected for an hgx homedir.
    
    The expectation:
    
    .hypergolix /
    
    +---logs
        +---(log file 1...)
        +---(log file 2...)
        
    +---ghidcache
        +---(ghid file 1...)
        +---(ghid file 2...)
        
    +---(hgx.pid)
    +---(hgx-cfg.json)
    '''
    subdirs = [
        hgx_home / 'logs',
        hgx_home / 'ghidcache'
    ]
    
    for subdir in subdirs:
        _ensure_dir(subdir)


def _ensure_hgx_homedir(root):
    ''' Gets the location of the hgx home dir and then makes sure it
    exists, including expected subdirs.
    '''
    hgx_home = root / '.hypergolix'
    # Create the home directory if it does not exist.
    _ensure_dir(hgx_home)
    # Create the remaining folder structure if it does not exist.
    _ensure_hgx_populated(hgx_home)
    
    return hgx_home
        
        
def _get_hgx_config(root):
    ''' Gets and returns the hypergolix configuration. Raises
    ConfigError if none is defined.
    '''
    hgx_home = root / '.hypergolix'
    hgx_cfg_path = hgx_home / 'hgx-cfg.json'
    
    if not hgx_cfg_path.exists():
        raise ConfigError('Hypergolix configuration has not been run.')
        
    with open(hgx_cfg_path.as_posix(), 'r') as f:
        cfg = f.read()
        
    return _CfgDecoder().decode(cfg)
    
    
def _set_hgx_config(root, cfg):
    ''' Idempotent function to update the config to the passed cfg.
    If the config does not already exist, creates it.
    '''
    hgx_home = _ensure_hgx_homedir(root)
    hgx_cfg_path = hgx_home / 'hgx-cfg.json'
    cfg = _CfgEncoder().encode(cfg)
    
    # TODO: make an atomic update system for encoding?
    # TODO: consider some kind of in-place updating system if cfg exists
    with open(hgx_cfg_path.as_posix(), 'w') as f:
        f.write(cfg)
        
        
def _set_remote(cfg, remote_def):
    ''' Adds a server to cfg. If no servers are defined, creates the
    key for them. Also ensures no duplicates. If remote already exists,
    will update in place if TLS definition changed; otherwise, silently
    does nothing.
    '''
    if 'remotes' not in cfg:
        cfg['remotes'] = [remote_def]
    
    else:
        # Get the index of the remote, which will be None if nonexistent
        index = _index_remote(cfg, remote_def)
        
        # Only add it if the server doesn't already exist in our cfg.
        if index is None:
            cfg['remotes'].append(remote_def)
        
        # Make sure the TLS definition didn't change though!
        else:
            old_rdef = cfg['remotes'][index]
            
            # TLS changed. Update in-place.
            if old_rdef != remote_def:
                cfg['remotes'][index] = remote_def
        
        
def _pop_remote(cfg, remote_def):
    ''' Removes a server from cfg. Silently does nothing if the
    server does not exist in the cfg.
    '''
    if 'remotes' not in cfg:
        raise ConfigError('Invalid configuration.')
    
    else:
        # Only remove if the server exists in cfg.
        index = _index_remote(cfg, remote_def)
        if index is None:
            return None
        else:
            return cfg['remotes'].pop(index)
        
        
def _index_remote(cfg, remote_def):
    ''' Finds the index of the remote_def in cfg. Returns None if the
    server is not contained in the cfg.
    
    Note that the index is only looking for host and port. TLS usage
    does not affect the remote index.
    '''
    # Short-circuit if servers are undefined
    if 'remotes' not in cfg:
        return None
        
    for index, server in enumerate(cfg['remotes']):
        # Short circuit and return the index if we find an equivalent server.
        # Don't worry about TLS for finding hosts -- you should never use the
        # same server over both TLS and non-TLS connections.
        if server.host == remote_def.host and server.port == remote_def.port:
            return index
            
    # No equal server found. Return None.
    else:
        return None


# ###############################################
# Argparse on command line invocation
# ###############################################


NAMED_REMOTES = {
    'hgx': _readonly_remote('hgx.hypergolix.com', 443, True)
}


def _named_remote(remote):
    ''' Converts a named remote to a host, port, TLS group.
    '''
    return NAMED_REMOTES[remote]
    
    
def _exclusive_named_remote(remote):
    ''' Converts an exclusive named remote to a list of host, port, TLS
    groups, of length one.
    '''
    # Manually set 'local' to an empty list
    if remote == 'local':
        return []
        
    # Otherwise, re-cast it as the pair.
    else:
        return [_named_remote(remote)]
    
    
def _handle_verbosity(config, verbosity):
    lookup = {
        'extreme': 'extreme',
        'shouty': 'shouty',
        'louder': 'debug',
        'loud': 'info',
        'normal': 'warning',
        'quiet': 'error'
    }
    config.log_verbosity = lookup[verbosity]
    
    
def _handle_debug(config, debug_enabled):
    ''' Only modify debug if it was specified.
    '''
    if debug_enabled is None:
        return
        
    elif debug_enabled:
        config.debug_mode = True
        
    else:
        config.debug_mode = False
    
    
def _handle_remotes(config, only_remotes, add_remotes, remove_remotes):
    ''' Manages remotes.
    '''
    # Handling an exclusive remote declaration
    if only_remotes is not None:
        # Remove all existing remotes
        for remote in config.remotes:
            config.remove_remote(remote.host, remote.port)
        
        # Do nothing for local only, but add in the named remote otherwise
        for remote in only_remotes:
            config.set_remote(
                remote.host,
                remote.port,
                remote.tls
            )
    
    # Adding and removing remotes normally.
    else:
        for remote in add_remotes:
            config.set_remote(
                remote.host,
                remote.port,
                remote.tls
            )
        for remote in remove_remotes:
            config.remove_remote(
                remote.host,
                remote.port
            )
            
            
def _typecast_remotes(args):
    ''' Performs all type checking and casting for remotes.
    '''
    # First enforce "only" actually being ONLY
    if args.only_remotes is not None and args.add_remotes:
        raise ValueError('Cannot use --only with --add or --remove.')
    elif args.only_remotes is not None and args.remove_remotes:
        raise ValueError('Cannot use --only with --add or --remove.')
        
    # Correctly defined, and we're not using an only named remote.
    elif args.only_remotes is None:
        _process_remotes(args.add_remotes)
        _process_remotes(args.remove_remotes)
        
    # We've specified a single named remote.
    elif args.only_remotes != 'local':
        args.only_remotes = [NAMED_REMOTES[args.only_remotes]]
        
    # We've specified only local.
    else:
        args.only_remotes = []
        
        
def _process_remotes(remotes):
    ''' Ensures correct definitions for all non-singular remotes, and
    type casts them appropriately.
    '''
    re_remotes = []
    for remote in remotes:
        # This is a named remote. Easy peasy.
        if isinstance(remote, str):
            re_remotes.append(NAMED_REMOTES[remote])
            
        # This is a manually-defined remote. We need to do some massaging.
        else:
            host = remote[0]
            port = int(remote[1])
            
            # Calling add_remotes specifies TLS. Use it!
            if len(remote) == 3:
                tls = _str_to_bool(
                    remote[2],
                    failure_msg = 'Failed to infer truthiness of TLS usage. ' +
                                  'Please use "true", "false", "t", "f", etc.'
                )
                
            # Calling remove_remotes omits TLS. Fake it!
            else:
                tls = True
            
            # Now make a readonly remote for the definition.
            re_remotes.append(_readonly_remote(host, port, tls))
            
    # And finally, update the original remotes in place.
    remotes.clear()
    remotes.extend(re_remotes)
    
    
def _str_to_bool(s, failure_msg='Failed to infer truthiness.'):
    ''' Attempts to convert a string to a bool.
    '''
    # Normalize case.
    s = s.lower()
    
    truisms = {'y', 'true', 't', 'yes', '1'}
    falsities = {'n', 'false', 'f', 'no', '0'}
    
    if s in truisms:
        return True
    elif s in falsities:
        return False
    else:
        raise ValueError(failure_msg)
        
        
def _handle_ipc(config, ipc):
    ''' If IPC is defined, update it.
    '''
    if ipc is not None:
        config.ipc_port = ipc
    
    
def _format_blockstr(long_line):
    ''' Wraps a urlsafe ghid.
    '''
    shortened_length = 36
    indent = '    '
    
    out = []
    for slice_start in range(0, len(long_line), shortened_length):
        out.append(
            indent + long_line[slice_start:slice_start + shortened_length]
        )
    
    return '\n'.join(out)
        
        
def _handle_whoami(config, whoami):
    ''' If whoami is True, prints out information about the current
    hypergolix user.
    '''
    if whoami:
        fingerprint = config.fingerprint
        user_id = config.user_id

        # Add newline just for format pretty
        print('')
            
        if user_id is None:
            print('Your user ID is undefined.')
        else:
            user_id = user_id.as_str()
            print('Your user ID is:\n' + _format_blockstr(user_id))
            print('You use that to log in.\n')
            
        if fingerprint is None:
            print('Your fingerprint is undefined.')
        else:
            fingerprint = fingerprint.as_str()
            print('Your fingerprint is:\n' + _format_blockstr(fingerprint))
            print(
                'Someone else can use that to share\n' +
                'Hypergolix objects with you.'
            )
        
    
def _handle_register(config, register):
    ''' Launches registration in a browser window if set.
    '''
    if register:
        fingerprint = config.fingerprint.as_str()
        reg_address = 'https://www.hypergolix.com/register.html?' + fingerprint
        
        try:
            webbrowser.open(reg_address, new=2)
            
        except Exception:
            print(
                'Failed to open web browser for registration.\n' +
                'Please navigate to this address and click "register":\n' +
                _format_blockstr(reg_address)
            )
            
        else:
            print(
                'Please complete registration in your browser window.\n' +
                'If no page was opened, navigate to this address and\n' +
                'click "register":\n' +
                _format_blockstr(reg_address)
            )
    
    
def _handle_args(args, root=None):
    ''' Performs all needed actions on the passed command args.
    '''
    if root is None:
        root = get_hgx_rootdir()
    
    with Config(root) as config:
        _handle_remotes(
            config,
            args.only_remotes,
            args.add_remotes,
            args.remove_remotes
        )
        _handle_debug(config, args.debug)
        _handle_verbosity(config, args.verbosity)
        _handle_ipc(config, args.ipc_port)
        _handle_whoami(config, args.whoami)
        _handle_register(config, args.register)
        
        
def _ingest_args(argv=None):
    ''' Makes an arg parser and then ingests commands, returning the
    arguments contained in the commands.
    '''
    parser = argparse.ArgumentParser(
        prog = 'hypergolix.config',
        description = 'Configure the Hypergolix service. You must restart ' +
                      'Hypergolix before changes are applied.',
    )
    
    ####################################################################
    # Remotes config
    ####################################################################
    
    remote_group = parser.add_argument_group(
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
    parser.set_defaults(remove_remotes=[], add_remotes=[])
    
    ####################################################################
    # Runtime config
    ####################################################################
    
    runtime_group = parser.add_argument_group(
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
    parser.set_defaults(debug=None)
    
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
    
    ####################################################################
    # Etc
    ####################################################################
    
    etc_group = parser.add_argument_group(
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
    
    # This will dispatch to sys.argv correctly if args=argv is None
    args = parser.parse_args(args=argv)
        
    # Now we need to do some manual type checking and manipulation
    try:
        _typecast_remotes(args)
        
    # If any of the handling fails, error out the parser.
    except Exception as exc:
        parser.error(str(exc))
        
    return args


if __name__ == '__main__':
    # We now return to your regularly scheduled programming
    args = _ingest_args()
    _handle_args(args)
    print('Configuration successful. Restart Hypergolix to apply any changes.')
