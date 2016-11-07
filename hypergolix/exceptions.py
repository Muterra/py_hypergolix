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

# Control * imports.
__all__ = [
    # Base class for all of the above
    'HypergolixException',
    # These are local persistence errors
    'PersistenceError',
    'IntegrityError',
    'UnavailableUpstream',
    # These are remote server errors and warnings. All subclass PersistenceErr
    'RemoteNak',
    'VerificationFailure',
    'MalformedGolixPrimitive',
    'UnboundContainer',
    'InvalidIdentity',
    'AlreadyDebound',
    'InconsistentAuthor',
    'DoesNotExist',
    'InvalidTarget',
    'IllegalDynamicFrame',
    'StillBoundWarning',
    # These are Agent/integration errors
    'HandshakeError',
    'HandshakeWarning',
    'Inaccessible',
    'UnknownParty',
    # These are dispatch errors
    'DispatchError',
    'UnknownToken',
    'DispatchWarning',
    # These are IPC/embed errors
    'IPCError',
    'DeadObject',   # Also a local persistence error
    # These are comms errors
    'CommsError',
    'RequestError',
    'RequestFinished',
    'RequestUnknown',
    'ConnectionClosed',
    # These are privateer errors
    'PrivateerError',
    'RatchetError',
    # These are configuration errors
    'ConfigError',
    # These are HGXLink errors
    'HGXLinkError',
]


class HypergolixException(Exception):
    ''' This is suclassed for all exceptions and warnings, so that code
    using hypergolix as an import can successfully catch all hypergolix
    exceptions with a single except.
    '''
    pass


class PersistenceError(HypergolixException, RuntimeError):
    ''' This exception (or a subclass thereof) is raised for all issues
    related to local persistence systems.
    '''
    pass
    
    
class IntegrityError(PersistenceError):
    ''' This PersistenceError is raised when a packed Golix primitive
    appears to be corrupted in local persistence.
    '''
    pass
    
    
class UnavailableUpstream(PersistenceError):
    ''' This PersistenceError is raised when an object is unavailable or
    unacceptable from (an) upstream remote(s).
    '''
    pass


class RemoteNak(PersistenceError):
    ''' This exception (or a subclass thereof) is raised for all failed
    operations with remote persister servers.
    '''
    pass
    
    
class MalformedGolixPrimitive(RemoteNak):
    ''' This RemoteNak is raised when a packed Golix primitive appears to
    be malformed.
    '''
    pass
    
    
class VerificationFailure(RemoteNak):
    ''' This RemoteNak is raised when signature verification fails on a
    Golix object.
    '''
    pass
    
    
class InvalidIdentity(RemoteNak):
    ''' This RemoteNak is raised when a persistence provider has received
    a primitive with an unknown or invalid author or recipient.
    '''
    pass
    
    
class UnboundContainer(RemoteNak):
    ''' This RemoteNak is raised when a persistence provider has no
    binding for the attempted container, and it was therefore passed
    immediately to garbage collection.
    '''
    pass
    
    
class InvalidTarget(RemoteNak):
    ''' This RemoteNak is raised when a persistence provider has received
    a primitive targeting an inappropriate object (for example, trying
    to debind a GEOC directly).
    '''
    pass
    
    
class AlreadyDebound(RemoteNak):
    ''' This RemoteNak is raised when a persistence provider has already
    received a debinding for the primitive being published.
    '''
    pass
    
    
class InconsistentAuthor(RemoteNak):
    ''' This RemoteNak is raised when a persistence provider has received
    a primitive targeting an inappropriate object (for example, trying
    to debind a GEOC directly).
    '''
    pass
    
    
class DoesNotExist(RemoteNak, KeyError):
    ''' This RemoteNak is raised when a persistence provider has received
    a request for a ghid that does not exist in its object store.
    '''
    pass
    
    
class IllegalDynamicFrame(RemoteNak):
    ''' This RemoteNak is raised when a persistence provider has received
    an illegal dynamic frame, ie. if the existing frame is not contained
    within the new frame's history, or if the zeroth frame contains
    history.
    '''
    pass
    
    
class StillBoundWarning(HypergolixException, RuntimeWarning):
    ''' Raised when a debinding did not result in the removal of its
    target -- for example, if another binding remains on the target
    object.
    '''
    pass


class HandshakeError(HypergolixException, RuntimeError):
    ''' Raised when handshakes fail.
    '''
    pass


class Inaccessible(HypergolixException, RuntimeError):
    ''' Raised when an Agent does not have access to an object.
    '''
    pass
    
    
class UnknownParty(HypergolixException, RuntimeError):
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


class UnknownToken(DispatchError, ValueError):
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
    
    
class DeadObject(IPCError, PersistenceError, TypeError):
    ''' Raised when operations are attempted on a local object that is
    already dead.
    '''
    pass
    
    
class LocallyImmutable(IPCError, TypeError):
    ''' Raised when an object is locally immutable. That means either:
    1. the object is static
    2. the object is not "owned" by the currently-logged-in Hypergolix
        process.
    '''
    pass
    
    
class Unsharable(IPCError, TypeError):
    ''' Raised when an object cannot be shared, typically because it is
    private.
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
    
    
class ConnectionClosed(CommsError):
    ''' Raised when something goes wrong with IPC (bad commands, etc).
    '''
    pass


class PrivateerError(HypergolixException, RuntimeError):
    ''' This exception (or a subclass thereof) is raised for all failed
    operations with privateers (which keep secrets).
    '''
    pass
    
    
class RatchetError(PrivateerError):
    ''' This PrivateerError is raised when ratcheting a secret could not
    be completed successfully.
    '''
    pass
    
    
class ConflictingSecrets(PrivateerError):
    ''' This PrivateerError is raised when an existing secret does not
    match a new secret.
    '''
    pass
    
    
class SecretUnknown(PrivateerError, KeyError):
    ''' This PrivateerError is raised when a request is made for an
    unknown secret.
    '''
    pass


class ConfigError(HypergolixException, RuntimeError):
    ''' This exception (or a subclass thereof) is raised for all failed
    operations with configuration.
    '''
    pass


class HGXLinkError(HypergolixException, RuntimeError):
    ''' This exception (or a subclass thereof) is raised for errors
    originating in the HGXLink itself.
    '''
    pass
