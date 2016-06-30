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

import collections
import threading

# Control * imports.
__all__ = [
    # Base class for all of the above
    'HypergolixException',
    # These are all persister errors and warnings
    'NakError',
    'UnboundContainerError',
    'DoesNotExistError',
    'PersistenceWarning',
    # These are Agent/integration errors
    'HandshakeError',
    'HandshakeWarning',
    'InaccessibleError',
    'UnknownPartyError',
    # These are dispatch errors
    'DispatchError',
    'DispatchWarning',
    # These are IPC/embed errors
    'IPCError',
    # These are comms errors
    'CommsError',
    'RequestError',
    'RequestFinished',
    'RequestUnknown',
    'SessionClosed',
]


class HypergolixException(Exception):
    ''' This is suclassed for all exceptions and warnings, so that code
    using hypergolix as an import can successfully catch all hypergolix
    exceptions with a single except.
    '''
    pass


class NakError(HypergolixException, RuntimeError):
    ''' This exception (or a subclass thereof) is raised for all failed 
    operations with persistence providers.
    '''
    pass
    
    
class UnboundContainerError(NakError):
    ''' This NakError is raised when a persistence provider has no 
    binding for the attempted container, and it was therefore passed
    immediately to garbage collection.
    '''
    pass
    
    
class DoesNotExistError(NakError):
    ''' This NakError is raised when a persistence provider has received
    a request for a ghid that does not exist in its object store.
    '''
    pass
    
    
class PersistenceWarning(HypergolixException, RuntimeWarning):
    ''' Raised when a debinding did not result in the removal of its
    target -- for example, if another binding remains on the target
    object.
    '''
    pass


class HandshakeError(HypergolixException, RuntimeError):
    ''' Raised when handshakes fail.
    '''
    pass


class InaccessibleError(HypergolixException, RuntimeError):
    ''' Raised when an Agent does not have access to an object.
    '''
    pass
    
    
class UnknownPartyError(HypergolixException, RuntimeError):
    ''' Raised when an Agent cannot find an identity definition for an
    author and therefore cannot verify anything.
    '''
    pass


class HandshakeWarning(HypergolixException, RuntimeWarning):
    ''' Raised when handshakes use an unknown app_id, but are otherwise
    legit.
    '''
    pass


class DispatchError(HypergolixException, RuntimeError):
    ''' Raised when something fails with dispatch.
    '''
    pass


class DispatchWarning(HypergolixException, RuntimeWarning):
    ''' Raised when something goes moderately wrong with dispatch.
    '''
    pass
    
    
class IPCError(HypergolixException, RuntimeError):
    ''' Raised when something goes wrong with IPC or embed (bad 
    commands, etc).
    '''
    pass
    
    
class CommsError(HypergolixException, RuntimeError):
    ''' Raised when something goes wrong with IPC (bad commands, etc).
    '''
    pass
    
    
class RequestError(CommsError):
    ''' Raised when something goes wrong with IPC (bad commands, etc).
    '''
    pass
    
    
class RequestFinished(RequestError):
    ''' Raised when something goes wrong with IPC (bad commands, etc).
    '''
    pass
    
    
class RequestUnknown(RequestError):
    ''' Raised when a request code is unknown.
    '''
    pass
    
    
class SessionClosed(CommsError):
    ''' Raised when something goes wrong with IPC (bad commands, etc).
    '''
    pass