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

from golix import Ghid

# Intra-package dependencies
from .utils import _BijectDict


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


# TODO: make an atomic update system for encoding
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


class _PServer(_NamedList):
    __slots__ = [
        'host',
        'port',
        'tls'
    ]


class _UserDef(_NamedList):
    __slots__ = [
        'fingerprint',
        'user_id',
        'password'
    ]


class _InstrumentationDef(_NamedList):
    __slots__ = [
        'verbosity',
        'debug',
        'traceur'
    ]


# Using a bijective mapping allows us to do bidirectional lookup
# This might be overkill, but if you already have one in your .utils module...
_TYPEHINTS = _BijectDict({
    '__PServer__': _PServer,
    '__UserDef__': _UserDef,
    '__InstrumentationDef__': _InstrumentationDef
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
        
    def encode(self, o, *args, **kwargs):
        ''' Manually force any type in _TYPEHINTS to be converted to
        their custom serialization, so that namedtuples can be supported
        without any fuss (normally known types bypass self.default())
        '''
        if type(o) in _TYPEHINTS:
            o = self.default(o)
        
        return super().encode(o, *args, **kwargs)
        
    def iterencode(self, o, *args, **kwargs):
        ''' Explicitly un-support iterencode.
        '''
        if type(o) in _TYPEHINTS:
            o = self.default(o)
        
        return super().iterencode(o, *args, **kwargs)


# ###############################################
# Library
# ###############################################


def _ensure_hgx_homedir():
    ''' Gets the location of the hgx home dir and then makes sure it
    exists, including expected subdirs.
    '''
    # For now, simply make a subdir in the user folder.
    user_home = pathlib.Path('~/')
    user_home = user_home.expanduser()
    hgx_home = user_home / '.hypergolix'
    # Create the home directory if it does not exist.
    _ensure_dir(hgx_home)
    # Create the remaining folder structure if it does not exist.
    _ensure_hgx_populated(hgx_home)
    
    return hgx_home
    
    
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


if __name__ == '__main__':
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
        
    if args.userid is None:
        user_id = None
    else:
        user_id = Ghid.from_str(args.userid)
    
    HGXService(
        host = args.host, 
        port = args.port, 
        tls = not args.notls,
        ipc_port = args.ipcport, 
        debug = args.debug, 
        traceur = args.traceur,
        cache_dir = args.cachedir,
        user_id = user_id
    )