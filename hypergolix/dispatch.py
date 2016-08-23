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

Some notes:

'''

# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'Dispatcher', 
]

# Global dependencies
import collections
import weakref
import threading
import os
import abc
# import traceback
import warnings

from golix import Ghid

# Intra-package dependencies
from .core import _GAO
from .core import _GAODict
from .core import _GAOSet
from .core import Oracle

# Intra-package dependencies
from .utils import _JitSetDict
from .utils import _JitDictDict
from .utils import SetMap
from .utils import call_coroutine_threadsafe

from .exceptions import DispatchError
from .exceptions import DispatchWarning
from .exceptions import UnknownToken

# from .exceptions import HandshakeError


# ###############################################
# Logging boilerplate
# ###############################################


import logging
logger = logging.getLogger(__name__)

        
# ###############################################
# Lib
# ###############################################

        
class DispatcherBase(metaclass=abc.ABCMeta):
    ''' Base class for dispatchers. Dispatchers handle objects; they 
    translate between raw Golix payloads and application objects, as 
    well as shepherding objects appropriately to/from/between different
    applications. Dispatchers are intended to be combined with agents,
    and vice versa.
    '''
    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
        
    @abc.abstractmethod
    def dispatch_handshake(self, target):
        ''' Receives the target *object* for a handshake (note: NOT the 
        handshake itself) and dispatches it to the appropriate 
        application.
        
        handshake is a StaticObject or DynamicObject.
        Raises HandshakeError if unsuccessful.
        '''
        pass
        
    @abc.abstractmethod
    def dispatch_handshake_ack(self, ack, target):
        ''' Receives a handshake acknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymAck object.
        '''
        pass
    
    @abc.abstractmethod
    def dispatch_handshake_nak(self, nak, target):
        ''' Receives a handshake nonacknowledgement and dispatches it to
        the appropriate application.
        
        ack is a golix.AsymNak object.
        '''
        pass
            
            
_AppDef = collections.namedtuple(
    typename = '_AppDef',
    field_names = ('app_token',),
)
            
            
_DispatchableState = collections.namedtuple(
    typename = '_DispatchableState',
    field_names = ('api_id', 'state'),
)


class Dispatcher:
    ''' The Dispatcher decides which objects should be delivered where.
    This is decided through either:
    
    1. An API identifier (schema) dictating general compatibility as a
        dispatchable
    2. Declaring an object to be private, at which point the Dispatcher
        will internally (though distributedly) maintain that object to
        be exclusively available to an app token.
        
    Objects, once declared as non-private, cannot be retroactively 
    privatized. That cat has officially left the bag/building/rodeo.
    However, a private object can be made non-private at a later time,
    provided it has defined an API ID.
    
    Private objects do not track their owning app tokens internally.
    Instead, this is managed through a Dispatcher-internal GAO. As such,
    even a bug resulting in a leaked private object will not result in a
    leaked app token.
    
    Ideally, the dispatcher will eventually also enforce whatever user
    restrictions on sharing are desired BETWEEN INSTALLED APPLICATIONS.
    Sharing restrictions between external parties are within the purview
    of the Rolodex.
    
    TODO: support notification mechanism to push new objects to other
    concurrent hypergolix instances. See note in ipc.ipccore.send_object
    '''
    def __init__(self):
        ''' Yup yup yup yup yup yup yup
        '''
        # Temporarily set distributed state to None.
        # Lookup for all known tokens: set(<tokens>)
        self._all_known_tokens = None
        # Lookup (dict-like) for <app token>: <startup ghid>
        self._startup_by_token = None
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = None
        # Distributed lock for adding app tokens
        self._token_lock = None
        
    def assemble(self):
        # Huh. We've nothing to do here yet.
        pass
        
    def bootstrap(self, all_tokens, startup_objs, private_by_ghid, token_lock):
        ''' Initialize distributed state.
        '''
        # Now init distributed state.
        # All known tokens must already contain a key for b'\x00\x00\x00\x00'.
        if b'\x00\x00\x00\x00' not in all_tokens:
            all_tokens.add(b'\x00\x00\x00\x00')
            
        # Lookup for all known tokens: set(<tokens>)
        self._all_known_tokens = all_tokens
        # Lookup (set-map-like) for <app token>: set(<startup ghids>)
        self._startup_by_token = startup_objs
        # Lookup (dict-like) for <obj ghid>: <private owner>
        self._private_by_ghid = private_by_ghid
        
        # These need to be distributed but aren't yet. TODO!
        # Distributed lock for adding app tokens
        self._token_lock = token_lock
        
    def new_token(self):
        # Use a dummy api_id to force the while condition to be true initially
        token = b'\x00\x00\x00\x00'
        # Birthday paradox be damned; we can actually *enforce* uniqueness
        while token in self._all_known_tokens:
            token = os.urandom(4)
        return token
        
    def register_application(self):
        ''' Creates a new application at the dispatcher.
        
        Currently, that just means creating an app token and adding it 
        to the master list of all available app tokens. But, in the 
        future, it will also encompass any information necessary to 
        actually start the app, as Hypergolix makes the transition from 
        backround service to core OS service.
        '''
        # TODO: this lock actually needs to be a distributed lock across all
        # Hypergolix processes. There's a race condition currently. It's going
        # to be a very, very unlikely one to hit, but existant nonetheless.
        with self._token_lock:
            token = self.new_token()
            # Do this right away to prevent race condition
            self._all_known_tokens.add(token)
            
        return _AppDef(token)
        
    def start_application(self, appdef):
        ''' Ensures that an application is known to the dispatcher.
        
        Currently just checks within all known tokens. In the future, 
        this be responsible for starting the application (unless we move
        that responsibility elsewhere), and then sending all of the
        startup objects to the application.
        '''
        # This cannot be used to create new app tokens!
        if appdef[0] not in self._all_known_tokens:
            raise UnknownToken('App token unknown to dispatcher.')
            
    def register_startup(self, token, ghid):
        ''' Registers a ghid to be used as a startup object for token.
        '''
        with self._token_lock:
            if token not in self._all_known_tokens:
                raise UnknownToken()
            elif token in self._startup_by_token:
                raise ValueError(
                    'Startup object already defined for that application. '
                    'Deregister it before registering a new startup object.'
                )
            else:
                self._startup_by_token[token] = ghid
                
    def deregister_startup(self, token):
        ''' Deregisters a ghid to be used as a startup object for token.
        '''
        with self._token_lock:
            if token not in self._all_known_tokens:
                raise UnknownToken()
            elif token not in self._startup_by_token:
                raise ValueError(
                    'Startup object has not been defined for that application.'
                )
            else:
                del self._startup_by_token[token]
        
    def get_startup_obj(self, token):
        ''' Returns the ghid of the declared startup object for that 
        token, or None if none has been declared.
        ''' 
        with self._token_lock:
            if token not in self._all_known_tokens:
                raise UnknownToken()
            elif token not in self._startup_by_token:
                return None
            else:
                return self._startup_by_token[token]
        
    def register_private(self, token, ghid):
        ''' Called by oracle's new_object to register an object as 
        private. Should only be called at object creation, but this is
        only enforced through the ipccore.
        '''
        token = bytes(token)
        if len(token) != 4:
            raise ValueError('App tokens must be 4 bytes long.')
            
        with self._token_lock:
            if token not in self._all_known_tokens:
                raise UnknownToken()
            
        self._private_by_ghid[ghid] = token
        
    def make_public(self, ghid):
        ''' Makes a private object public.
        '''
        try:
            del self._private_by_ghid[ghid]
            
        except KeyError as exc:
            raise ValueError(
                'Obj w/ passed ghid is unknown or already public: ' + str(ghid)
            ) from exc
            
    def get_parent_token(self, ghid):
        ''' Returns the app_token parent for the passed ghid, if (and 
        only if) it's private. Otherwise, returns None.
        '''
        try:
            return self._private_by_ghid[ghid]
        except KeyError:
            return None
            
            
class _Dispatchable(_GAO):
    ''' A dispatchable object.
    '''
    def __init__(self, dispatch, ipc_core, api_id=None, private=False, 
                state=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Dispatch is already a weakref.proxy...
        self._dispatch = dispatch
        # But ipc_core is not.
        self._ipccore = weakref.proxy(ipc_core)
        
        self.state = state
        self.api_id = api_id
        
    @property
    def parent_token(self):
        ''' Read-only proxy to dispatch to check for a private parent.
        Returns the parent app token, or None if not a private object.
        '''
        return self._dispatch.get_parent_token(self.ghid)
        
    @property
    def private(self):
        ''' Returns true/false if the object is private.
        '''
        return bool(self._dispatch.get_parent_token(self.ghid))
        
    def pull(self, *args, **kwargs):
        ''' Refreshes self from upstream. Should NOT be called at object 
        instantiation for any existing objects. Should instead be called
        directly, or through _weak_pull for any new status.
        '''
        modified = super().pull(*args, **kwargs)
        if modified:
            logger.debug('Pull detected modifications. Sending to ipc.')
            call_coroutine_threadsafe(
                coro = self._ipccore.notify_update(self.ghid, deleted=False),
                loop = self._ipccore._loop
            )
            logger.debug('IPC completed update notifications.')
        return modified
        
    @staticmethod
    def _pack(state):
        ''' Packs state into a bytes object. May be overwritten in subs
        to pack more complex objects. Should always be a staticmethod or
        classmethod.
        '''
        version = b'\x00'
        return b'hgxd' + version + state[0] + state[1]
        
    @staticmethod
    def _unpack(packed):
        ''' Unpacks state from a bytes object. May be overwritten in 
        subs to unpack more complex objects. Should always be a 
        staticmethod or classmethod.
        '''
        magic = packed[0:4]
        version = packed[4:5]
        
        if magic != b'hgxd':
            raise DispatchError('Object does not appear to be dispatchable.')
        if version != b'\x00':
            raise DispatchError('Incompatible dispatchable version number.')
            
        api_id = packed[5:70]
        state = packed[70:]
        
        return _DispatchableState(api_id, state)
        
    def apply_state(self, state):
        ''' Apply the UNPACKED state to self.
        '''
        # TODO: make sure this doesn't accidentally change api_id or app_token
        # Maybe set the _attributes directly or something as well?
        self.api_id = state[0]
        self.state = state[1]
        
    def extract_state(self):
        ''' Extract self into a packable state.
        '''
        return _DispatchableState(self.api_id, self.state)
        
    @property
    def api_id(self):
        # Warn if both api_id and app_token are undefined
        # Is this the appropriate time to check this?
        if self._api_id == bytes(65):
            return None
        else:
            return self._api_id
        
    @api_id.setter
    def api_id(self, value):
        if value is None:
            value = bytes(65)
        else:
            value = bytes(value)
            if len(value) != 65:
                raise ValueError('API IDs must be 65 bytes long.')
            
        self._api_id = value
        
    def update(self, state):
        ''' Wrapper to apply state that reuses api_id and app_token, and
        then call push.
        '''
        if not self.dynamic:
            raise DispatchError(
                'Object is not dynamic. Cannot update.'
            )
            
        self.apply_state(
            state = (self.api_id, state)
        )
        self.push()
        
    def apply_delete(self):
        super().apply_delete()
        call_coroutine_threadsafe(
            coro = self._ipccore.notify_update(ghid, deleted=True),
            loop = self._ipccore._loop
        )