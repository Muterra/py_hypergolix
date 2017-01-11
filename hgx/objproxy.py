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
import weakref
import traceback
import asyncio
import operator
import inspect
import json
import pickle

from loopa.utils import triplicated
from loopa.utils import Triplicate

# Internal deps
from .exceptions import DeadObject
from .exceptions import LocallyImmutable
from .exceptions import Unsharable

from .utils import run_coroutine_loopsafe
from .utils import call_coroutine_threadsafe
from .utils import ApiID
from .utils import _reap_wrapped_task

from .embed import TriplicateAPI

from .hypothetical import public_api
from .hypothetical import fixture_api


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
# Library
# ###############################################


class ObjCore(metaclass=TriplicateAPI):
    ''' Core object that exposes all Hypergolix internals as
    manually-name-mangled stuff, which can then be re-assigned by
    subclasses to support a given API.
    '''
    _hgx_HASHMIX = 3141592
    _hgx_DEFAULT_API = ApiID(bytes(63) + b'\x01')
    
    # Initialize some defaults here, just for good measure.
    __hgxlink = None
    __hgx_ipc = None
    __state = None
    __ghid = None
    __callback = None
    __binder = None
    __api_id = None
    __private = None
    __dynamic = None
    __isalive = None
    __legroom = None
    
    def __init__(self, state, api_id, dynamic, private, ghid=None, binder=None,
                 *, hgxlink, ipc_manager, _legroom, callback=None):
        ''' Allocates the object locally, but does NOT create it. You
        have to explicitly call hgx_push, hgx_push_threadsafe, or
        hgx_push_loopsafe to actually create the sync'd object and get
        a ghid.
        '''
        # Do this so we don't get circular references and can therefore support
        # our persistence declaration
        self.__hgxlink = weakref.ref(hgxlink)
        self.__hgx_ipc = weakref.ref(ipc_manager)
        # TODO: think about this
        self.__isalive = True
        
        # All others can be set via their properties
        self._hgx_state = state
        self._hgx_ghid = ghid
        self._hgx_callback = callback
        self._hgx_binder = binder
        self._hgx_api_id = api_id
        self._hgx_private = private
        self._hgx_dynamic = dynamic
        # TODO: think about this
        self._hgx_legroom = _legroom
        
    def __hash__(self):
        ''' Have a hash, if our ghid address is defined; otherwise,
        return None (which will in turn cause Python to raise a
        TypeError in the parent call).
        
        The hashmix is a random value that has been included to allow
        faster hash bucket differentiation between ghids and objproxies.
        '''
        if self.__ghid is not None:
            return hash(self.__ghid) ^ self._hgx_HASHMIX
        else:
            return None
            
    def __eq__(self, other):
        ''' Equality comparisons on ObjBase will return True if and
        only if:
        1. They both have an .hgx_ghid attribute (else, typeerror)
        2. The .hgx_ghid attribute compares equally
        3. They both have an .hgx_state attribute (else, typeerror)
        4. The .hgx_state attribute compares equally
        5. They both have an .hgx_binder attribute (else, typeerror)
        6. The .hgx_binder attribute compares equally
        '''
        try:
            # We can talk about equality until the cows come home.
            equality = (self.__ghid == other._hgx_ghid)
            # What is equality?
            equality &= (self.__binder == other._hgx_binder)
            # What is home?
            equality &= (self.__state == other._hgx_state)
            # What are cows?
            
        except AttributeError as exc:
            raise TypeError(
                'Incomparable types: ' +
                type(self).__name__ + ', ' +
                type(other).__name__
            ) from exc
            
        return equality
        
    @property
    def _loop(self):
        ''' Use the hgxlink's loop.
        '''
        return self._hgxlink._loop
        
    @property
    def _hgxlink(self):
        ''' Read-only access to the hgxlink.
        '''
        hgxlink = self.__hgxlink()
        if hgxlink is None:
            raise RuntimeError(
                'The HGXLink has been garbage collected. Cannot continue.'
            )
        else:
            return hgxlink
        
    @property
    def _hgx_ipc(self):
        ''' Read-only access to the hgxlink.
        '''
        ipc = self.__hgx_ipc()
        if ipc is None:
            raise RuntimeError(
                'The HGXLink has been garbage collected. Cannot continue.'
            )
        else:
            return ipc
            
    @property
    def _hgx_legroom(self):
        ''' Access the legroom property.
        '''
        return self.__legroom
        
    @_hgx_legroom.setter
    def _hgx_legroom(self, value):
        ''' Set the legroom property.
        '''
        self.__legroom = int(value)
        
    @property
    def _hgx_linked(self):
        ''' Dummy property until linking is supported.
        '''
        return False
        
    @property
    def _hgx_state(self):
        ''' Simple pass-through to the internal state. This is a strong
        reference, so if the state is mutable, modifications will be
        applied to the state; however, push() must still be explicitly
        called.
        '''
        if not self.__isalive:
            raise DeadObject()
        else:
            return self.__state
        
    @_hgx_state.setter
    def _hgx_state(self, value):
        ''' Allow direct overwriting of the internal state. Does not
        ensure serializability, nor does it push upstream.
        '''
        # TODO: support nesting object states, resulting in a linked object.
        if not self.__isalive:
            raise DeadObject()
        else:
            self.__state = value
        
    @property
    def _hgx_ghid(self):
        ''' This is a read-only, immutable address for the object. It is
        universal. See documentation about ghids.
        '''
        return self.__ghid
        
    @_hgx_ghid.setter
    def _hgx_ghid(self, ghid):
        ''' Prevent redefinition of object ghid.
        '''
        if self.__ghid is None:
            self.__ghid = ghid
        else:
            raise RuntimeError('Object ghid cannot be re-defined.')
        
    @property
    def _hgx_api_id(self):
        ''' An identifier for the kind of object. Used during sharing
        and delivery. Read-only.
        '''
        # Just, yknow, proxy to our internal normalization.
        return self.__api_id
        
    @_hgx_api_id.setter
    def _hgx_api_id(self, api_id):
        ''' Prevent api_id mutability.
        '''
        if self.__api_id is None:
            # Must be of type ApiID
            if isinstance(api_id, ApiID):
                self.__api_id = api_id
                
            # Or None, in which case reset to default.
            elif api_id is None:
                self.__api_id = self._hgx_DEFAULT_API
                
            # Anything else is a typeerror
            else:
                raise TypeError('API must be of type ApiID.')
        else:
            raise RuntimeError('Object API ID cannot be redefined.')
        
    @property
    def _hgx_private(self):
        ''' A private object is only accessible by this particular
        application, with this particular user. Subsequent instances of
        the application will require the same app_token to retrieve any
        of its private objects. Read-only.
        '''
        return self.__private
        
    @_hgx_private.setter
    def _hgx_private(self, privacy):
        ''' Prevent conversion of public objects to private ones. This
        is also enforced within Hypergolix itself.
        '''
        # If it's not set, allow us to set it
        if self.__private is None:
            self.__private = bool(privacy)
            
        # If it is set, only allow us to change if already private.
        elif self.__private:
            self.__private = bool(privacy)
            
        # If already set and public, cannot be changed.
        else:
            raise RuntimeError('Non-private objects cannot be made private.')
        
    @property
    def _hgx_dynamic(self):
        ''' Boolean value indicating whether or not this is a dynamic
        object. Static objects cannot be changed; any attempt to update
        upstream for a static object will cause errors.
        '''
        return self.__dynamic
        
    @_hgx_dynamic.setter
    def _hgx_dynamic(self, value):
        ''' Prevent modification of the dynamicy.
        '''
        if self.__dynamic is None:
            self.__dynamic = bool(value)
        else:
            raise RuntimeError('Object dynamicy cannot be modified.')
        
    @property
    def _hgx_binder(self):
        ''' Essentially the object's author... more or less. Sometimes
        less.
        '''
        return self.__binder
        
    @_hgx_binder.setter
    def _hgx_binder(self, ghid):
        ''' Prevents re-assignment of the binder... which is... well...
        okay this is complicated, but for now this is good enough.
        '''
        if self.__binder is None:
            self.__binder = ghid
        else:
            raise RuntimeError('Object binder cannot be modified.')
        
    @property
    def _hgx_isalive(self):
        ''' Alive objects are accessible through hypergolix. Dead ones
        are not.
        '''
        return self.__isalive
            
    @property
    def _hgx_persistence(self):
        ''' Dictates what Hypergolix should do with the object upon its
        garbage collection by the Python process.
        
        May be:
            'strong'    Object is retained until hgx_delete is
                        explicitly called, regardless of python runtime
                        behavior / garbage collection. Default.
            'weak'      Object is retained until hgx_delete is
                        explicitly called, or when python runtime
                        garbage collects the proxy, EXCEPT at python
                        exit
            'temp'      Object is retained only for the lifetime of the
                        python object. Will be retained until hgx_delete
                        is explicitly called, or when python garbage
                        collects the proxy, INCLUDING at python exit.
        '''
        raise NotImplementedError()
        # Don't forget to add this to recast() when you implement it!
        
    @_hgx_persistence.setter
    def _hgx_persistence(self, value):
        ''' Setter for hgx_persistence. Note that this attribute cannot
        be deleted.
        '''
        raise NotImplementedError()
        
    def _hgx_set_raw_callback(self, callback):
        ''' Registers a callback without wrapping.
        '''
        self.__callback = callback
        
    @property
    def _hgx_callback(self):
        ''' Get the callback.
        '''
        return self.__callback

    @_hgx_callback.setter
    def _hgx_callback(self, callback):
        ''' Register a callback to be called whenever an upstream update
        is received from the hypergolix service. There can be at most
        one callback, of any type (internal, threadsafe, loopsafe), at
        any given time.
        
        This CALLBACK will be called from within the IPC embed's
        internal event loop.
        '''
        # Add this check to help shield against accidentally-incomplete
        # loopsafe decorators, and attempts to directly set functions.
        if callback is None:
            return
        elif not inspect.iscoroutinefunction(callback):
            raise TypeError('Callback must be defined with "async def".')
        
        # Any handlers passed to us this way can already be called natively
        # from within our own event loop, so they just need to be wrapped such
        # that they never raise.
        async def wrap_callback(*args, callback=callback, **kwargs):
            try:
                await callback(*args, **kwargs)
                
            except Exception:
                logger.error(
                    'Error while running update callback. Traceback: \n' +
                    ''.join(traceback.format_exc())
                )
                
        self.__callback = wrap_callback
        
    @_hgx_callback.deleter
    def _hgx_callback(self):
        ''' Remove any declared callback.
        '''
        self.__callback = None
        
    @triplicated
    async def _hgx_recast(self, cls):
        ''' Takes the passed obj, and attempts to re-cast it as this
        class. Returns a new instance of the object, recast as cls.
        Preserves any update callbacks, even though they might break
        from the type change.
        
        NOTE THAT THIS WILL RENDER THE PREVIOUS OBJECT DEAD! The "old"
        object will also stop receiving updates from hgxlink.
        
        As examples:
            +   <PickleProxy object>.hgx_recast(ObjBase) returns the
                object recast as an ObjBase
            +   <ObjBase object>.hgx_recast(PickleProxy) returns the
                object recast as a PickleProxy
        '''
        # Re-pack the object for recasting. We always need to do this, in case
        # something got weird with serialization.
        state = await self.hgx_pack(self.__state)
        
        # Do this check afterwards to avoid a race condition.
        if not self._hgx_isalive:
            raise DeadObject('Cannot recast a dead object.')
        
        # Use the state from above to create a new copy of the object.
        recast = await self._hgxlink.get(
            cls,
            ghid = self.__ghid,
            obj_def = (
                self.__ghid,
                self.__binder,
                state,  # Note that this is the packed version from above
                False,  # is_link
                self.__api_id,
                self.__private,
                self.__dynamic,
                self.__legroom
            )
        )
        # Copy over the existing callback. This will change when we move the
        # callback wrapping into reap_anonymous_task. We have to access the
        # mangled attribute, because otherwise we will re-wrap the callback.
        recast._hgx_set_raw_callback(self.__callback)
        # Now render self (the old object) inoperable
        self.__render_inop()
        
        return recast
        
    @triplicated
    async def _hgx_push(self):
        ''' Pushes object state upstream.
        '''
        # Error traps for dead object
        if not self.__isalive:
            raise DeadObject()
            
        # The object is still alive.
        elif self.__ghid is None:
            # It's even new! Raise; this means it wasn't created through the
            # normal hgxlink.get/hgxlink.new
            raise RuntimeError(
                'Object has no ghid; cannot update. Was the object created ' +
                'through the HGXLink?'
            )
        
        # The object is not new. Is it static?
        else:
            # Error trap if the object isn't "owned" by us
            if self._hgxlink.whoami != self.__binder:
                raise LocallyImmutable('No access rights to mutate object.')
            
            # Error trap if it's static
            elif not self.__dynamic:
                raise LocallyImmutable('Cannot update a static object.')
            
            # All traps passed. Make the call.
            else:
                packed_state = await self.hgx_pack(self.__state)
                await self._hgx_ipc.update_ghid(
                    self.__ghid,
                    packed_state,
                    self.__private,
                    self.__legroom
                )

    @triplicated
    async def _hgx_sync(self):
        ''' Trivial pass-through to the hgxlink make_sync.
        '''
        if not self.__isalive:
            raise DeadObject()
        else:
            await self._hgx_ipc.sync_ghid(self.__ghid)

    @triplicated
    async def _hgx_share(self, recipient):
        ''' Trivial pass-through to the hgx make_share, plus a check for
        privacy.
        '''
        if not self.__isalive:
            raise DeadObject()
            
        elif self.__private:
            raise Unsharable('Cannot share a private object.')
            
        else:
            await self._hgx_ipc.share_ghid(self.__ghid, recipient)

    @triplicated
    async def _hgx_freeze(self):
        ''' Trivial pass-through to the hgxlink make_freeze, with type
        checking for mutability.
        '''
        if not self.__isalive:
            raise DeadObject()
        
        elif not self.__dynamic:
            raise LocallyImmutable('Cannot freeze a static object.')
        
        else:
            frozen_ghid = await self._hgx_ipc.freeze_ghid(self.__ghid)
            frozen = await self._hgxlink.get(
                cls = type(self),
                ghid = frozen_ghid
            )
            return frozen

    @triplicated
    async def _hgx_hold(self):
        ''' Trivial pass-through to the hgxlink hold.
        '''
        if not self.__isalive:
            raise DeadObject()
        else:
            await self._hgx_ipc.hold_ghid(self.__ghid)

    @triplicated
    async def _hgx_discard(self):
        ''' Does actually add some value to the hgxlink make_discard.
        '''
        if not self.__isalive:
            raise DeadObject()
        else:
            await self._hgx_ipc.discard_ghid(self.__ghid)
            self.__render_inop()

    @triplicated
    async def _hgx_delete(self):
        ''' Does actually add some value to the hgxlink make_delete.
        '''
        if not self.__isalive:
            raise DeadObject()
        else:
            await self._hgx_ipc.delete_ghid(self.__ghid)
            self.__render_inop()
    
    @staticmethod
    async def hgx_pack(state):
        ''' Packs the object into bytes. For the base proxy, treat the
        input as bytes and return immediately.
        '''
        return state
    
    @staticmethod
    async def hgx_unpack(packed):
        ''' Unpacks the object from bytes. For the base proxy, treat the
        input as bytes and return immediately.
        '''
        return packed
        
    @public_api
    async def _hgx_force_delete(self):
        ''' Does everything needed to clean up the object, after either
        an upstream or local delete.
        '''
        self.__render_inop()
        
        # If there is an update callback defined, run it concurrently.
        if self.__callback is not None:
            update_task = asyncio.ensure_future(self.__callback(self))
            update_task.add_done_callback(_reap_wrapped_task)
            
    @_hgx_force_delete.fixture
    async def _hgx_force_delete(self):
        ''' Fixture upstream deletion. By doing nothing.
        '''
    
    @public_api
    async def _hgx_force_pull(self, state):
        ''' Does everything needed to apply an upstream update to the
        object.
        '''
        state = await self.hgx_unpack(state)
        self.__state = state
        
        # If there is an update callback defined, run it concurrently.
        if self.__callback is not None:
            logger.debug(
                'Update pulled for ' + str(self.__ghid) + '. Running '
                'callback.'
            )
            pull_task = asyncio.ensure_future(self.__callback(self))
            pull_task.add_done_callback(_reap_wrapped_task)
            
        else:
            logger.debug(
                'Update pulled for ' + str(self.__ghid) + ', but it '
                'has no callback.'
            )
    
    @_hgx_force_pull.fixture
    async def _hgx_force_pull(self, state):
        ''' Fixturing this actually requires a small degree of effort.
        '''
        state = await self.hgx_unpack(state)
        self._hgx_state = state
        
    def __render_inop(self):
        ''' Renders the object locally inoperable, either through a
        delete or discard.
        '''
        self.__isalive = False
        
        
class Obj(ObjCore, metaclass=Triplicate):
    ''' Rename various internal-only methods to bring them into the
    "public" namespace for Objs.
    
    Note that, because super() doesn't use Triplicate as metaclass, and
    since subclasses don't include super() stuff in their
    metaclass.__new__ namespace, the super() methods will not have its
    methods triplicated -- only we will.
    '''
    
    state = ObjCore._hgx_state
    ghid = ObjCore._hgx_ghid
    api_id = ObjCore._hgx_api_id
    private = ObjCore._hgx_private
    dynamic = ObjCore._hgx_dynamic
    binder = ObjCore._hgx_binder
    isalive = ObjCore._hgx_isalive
    
    recast = ObjCore._hgx_recast
    callback = ObjCore._hgx_callback
    
    push = ObjCore._hgx_push
    sync = ObjCore._hgx_sync
    share = ObjCore._hgx_share
    freeze = ObjCore._hgx_freeze
    hold = ObjCore._hgx_hold
    discard = ObjCore._hgx_discard
    delete = ObjCore._hgx_delete
            
    def __repr__(self):
        return ''.join((
            '<',
            type(self).__name__,
            ' with state ',
            repr(self._hgx_state),
            ' at ',
            str(self._hgx_ghid),
            '>'
        ))


class Proxy(ObjCore, metaclass=Triplicate):
    ''' HGX proxies, partly inspired by weakref.proxies, are a mechanism
    by which normal python objects can be "dropboxed" into hypergolix.
    The proxy object, and not the original object, must be referenced.
    
    Several "magic method" / "dunder methods" are explicitly sent to the
    proxying object, instead of the proxied object:
        1.  __init__
        2.  __repr__
        3.  __hash__ (see note [1] below)
        4.  __eq__ (see note [2] below)
        5.  __dir__
    
    All other dunder methods will pass directly to the proxied object.
    If the proxied object does not support those methods, it will raise,
    but which exception vacies on a case-by-case basis.
    
    [1] Proxies are hashable if their ghids are defined, but unhashable
    otherwise. Note, however, that their hashes have nothing to do with
    their proxied objects. Also note that
        isinstance(obj, collections.Hashable)
    will always identify ObjProxies as hashable, regardless of their
    actual runtime behavior.
    
    [2] Equality comparisons, on the other hand, reference the proxy's
    state directly. So if the states compare equally, the two ObjProxies
    will compare equally, regardless of the proxy state (ghid, api_id,
    etc).
    
    Side note: as per python docs, support for magic methods ("dunder",
    or "double underscore" methods) is only reliable if declared
    directly and explicitly within the class.
    '''
    _hgx_HASHMIX = 936930316
    
    hgx_state = ObjCore._hgx_state
    hgx_ghid = ObjCore._hgx_ghid
    hgx_api_id = ObjCore._hgx_api_id
    hgx_private = ObjCore._hgx_private
    hgx_dynamic = ObjCore._hgx_dynamic
    hgx_binder = ObjCore._hgx_binder
    hgx_isalive = ObjCore._hgx_isalive
    
    hgx_recast = ObjCore._hgx_recast
    hgx_callback = ObjCore._hgx_callback
    
    hgx_push = ObjCore._hgx_push
    hgx_sync = ObjCore._hgx_sync
    hgx_share = ObjCore._hgx_share
    hgx_freeze = ObjCore._hgx_freeze
    hgx_hold = ObjCore._hgx_hold
    hgx_discard = ObjCore._hgx_discard
    hgx_delete = ObjCore._hgx_delete
    
    def __init__(self, state, *args, **kwargs):
        ''' Slight modification of super() to resolve proxies into their
        core objects (ie, making a new proxy of an existing proxy will
        always create a copy of the object, as far as hypergolix is
        concerned).
        '''
        if isinstance(state, Obj):
            state = state._hgx_state
            
        super().__init__(state, *args, **kwargs)
            
    def __repr__(self):
        return ''.join((
            '<',
            type(self).__name__,
            ' to ',
            repr(self._hgx_state),
            ' at ',
            str(self._hgx_ghid),
            '>'
        ))
            
    def __setattr__(self, name, value):
        ''' Redirect all setting of currently missing attributes to the
        proxy. This implies that setting anything for the first time
        will require
        '''
        # Try to GET the attribute with US, the actual proxy.
        try:
            super().__getattribute__(name)
        
        # We failed to get it here. Pass the setattr to the referenced object.
        except AttributeError:
            setattr(self._hgx_state, name, value)
            
        # We succeeded to get it here. Set it here.
        else:
            super().__setattr__(name, value)
        
    def __getattr__(self, name):
        ''' Redirect all missing attribute lookups to the proxy.
        Note that getattr is only called if the normal lookup fails. So,
        we don't need to check for an attributeerror locally, because
        we're guaranteed to get one.
        '''
        return getattr(self._hgx_state, name)
        
    def __delattr__(self, name):
        ''' Permanently prevent deletion of all local attributes, and
        pass any others to the referenced object.
        '''
        # Try to GET the attribute with US, the actual proxy.
        try:
            super().__getattribute__(name)
        
        # We failed to get it here. Pass the setattr to the referenced object.
        except AttributeError:
            delattr(self._hgx_state, name)
            
        # We succeeded to get it here. Delete it here (if possible, which it
        # generally won't be).
        else:
            super().__delattr__(name)
        
    def __eq__(self, other):
        ''' Pass the equality comparison straight into the state.
        '''
        # If the other instance is also an ObjBase, do a full comparison.
        try:
            return super().__eq__(other)
            
        # If not, just compare our proxy state directly to the other object.
        except TypeError:
            return self._hgx_state == other
        
    def __gt__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _hgx_state attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._hgx_state > other._hgx_state
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._hgx_state > other
        
    def __ge__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _hgx_state attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._hgx_state >= other._hgx_state
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._hgx_state >= other
        
    def __lt__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _hgx_state attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._hgx_state < other._hgx_state
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._hgx_state < other
        
    def __le__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _hgx_state attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._hgx_state <= other._hgx_state
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._hgx_state <= other
        
    def __dir__(self):
        ''' Implement a dir that attempts to only list the methods that
        will actually succeed -- ie, cut out any automatically-generated
        special/magic/dunder methods that have not also been defined at
        the referenced object.
        '''
        # Get all of our normal dirs.
        this_dir = set(super().__dir__())
        # Get all of our proxy's dirs.
        prox_dir = set(dir(self._hgx_state))
        
        # Remove any of our explicit pass-through special/magic/dunder methods
        total_dir = this_dir - self._hgx_ALL_METAD
        # Add in all of the proxy_dir
        total_dir.update(prox_dir)
        
        return total_dir
        
    # BEGIN AUTOMATICALLY-GENERATED METHODRY HERE!
    # ----------------------------------------------------
        
    _hgx_ALL_METAD = {
        '__bool__',
        '__bytes__',
        '__str__',
        '__format__',
        '__len__',
        '__length_hint__',
        '__call__',
        '__getitem__',
        '__missing__',
        '__setitem__',
        '__delitem__',
        '__iter__',
        '__reversed__',
        '__contains__',
        '__enter__',
        '__exit__',
        '__aenter__',
        '__aexit__',
        '__await__',
        '__aiter__',
        '__anext__',
        '__add__',
        '__sub__',
        '__mul__',
        '__matmul__',
        '__truediv__',
        '__floordiv__',
        '__mod__',
        '__divmod__',
        '__pow__',
        '__lshift__',
        '__rshift__',
        '__and__',
        '__xor__',
        '__or__',
        '__radd__',
        '__rsub__',
        '__rmul__',
        '__rmatmul__',
        '__rtruediv__',
        '__rfloordiv__',
        '__rmod__',
        '__rdivmod__',
        '__rpow__',
        '__rlshift__',
        '__rrshift__',
        '__rand__',
        '__rxor__',
        '__ror__',
        '__iadd__',
        '__isub__',
        '__imul__',
        '__imatmul__',
        '__itruediv__',
        '__ifloordiv__',
        '__imod__',
        '__ipow__',
        '__ilshift__',
        '__irshift__',
        '__iand__',
        '__ixor__',
        '__ior__',
        '__neg__',
        '__pos__',
        '__abs__',
        '__invert__',
        '__complex__',
        '__int__',
        '__float__',
        '__round__',
        '__index__',
    }

    def __bool__(self):
        ''' Wrap __bool__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return bool(self._hgx_state)
        
    def __bytes__(self):
        ''' Wrap __bytes__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return bytes(self._hgx_state)
        
    def __str__(self):
        ''' Wrap __str__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return str(self._hgx_state)
        
    def __format__(self, *args, **kwargs):
        ''' Wrap __format__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return format(self._hgx_state, *args, **kwargs)
        
    def __len__(self):
        ''' Wrap __len__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return len(self._hgx_state)
        
    def __length_hint__(self):
        ''' Wrap __length_hint__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return operator.length_hint(self._hgx_state)
        
    def __call__(self, *args, **kwargs):
        ''' Wrap __call__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state(*args, **kwargs)
        
    def __getitem__(self, key):
        ''' Wrap __getitem__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state[key]
        
    def __missing__(self, key):
        ''' Wrap __missing__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__missing__(key)
        
    def __setitem__(self, key, value):
        ''' Wrap __setitem__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        self._hgx_state[key] = value
        
    def __delitem__(self, key):
        ''' Wrap __delitem__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        del self._hgx_state[key]
        
    def __iter__(self):
        ''' Wrap __iter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return iter(self._hgx_state)
        
    def __reversed__(self):
        ''' Wrap __reversed__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return reversed(self._hgx_state)
        
    def __contains__(self, item):
        ''' Wrap __contains__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return item in self._hgx_state
        
    def __enter__(self):
        ''' Wrap __enter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__enter__()
        
    def __exit__(self, *args, **kwargs):
        ''' Wrap __exit__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__exit__(*args, **kwargs)
        
    def __aenter__(self):
        ''' Wrap __aenter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__aenter__()
        
    def __aexit__(self, *args, **kwargs):
        ''' Wrap __aexit__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__aexit__(*args, **kwargs)
        
    def __await__(self):
        ''' Wrap __await__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__await__()
        
    def __aiter__(self):
        ''' Wrap __aiter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__aiter__()
        
    def __anext__(self):
        ''' Wrap __anext__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return self._hgx_state.__anext__()
        
    def __add__(self, other):
        ''' Wrap __add__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state + other._hgx_state
        
        else:
            return self._hgx_state + other
            
    def __sub__(self, other):
        ''' Wrap __sub__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state - other._hgx_state
        
        else:
            return self._hgx_state - other
            
    def __mul__(self, other):
        ''' Wrap __mul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state * other._hgx_state
        
        else:
            return self._hgx_state * other
            
    def __matmul__(self, other):
        ''' Wrap __matmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state @ other._hgx_state
        
        else:
            return self._hgx_state @ other
            
    def __truediv__(self, other):
        ''' Wrap __truediv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state / other._hgx_state
        
        else:
            return self._hgx_state / other
            
    def __floordiv__(self, other):
        ''' Wrap __floordiv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state // other._hgx_state
        
        else:
            return self._hgx_state // other
            
    def __mod__(self, other):
        ''' Wrap __mod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state % other._hgx_state
        
        else:
            return self._hgx_state % other
            
    def __divmod__(self, other, *args, **kwargs):
        ''' Wrap __divmod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return divmod(
                self._hgx_state,
                other._hgx_state,
                *args,
                **kwargs
            )
        
        else:
            return divmod(
                self._hgx_state,
                other,
                *args,
                **kwargs
            )
            
    def __pow__(self, other, *args, **kwargs):
        ''' Wrap __pow__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return pow(
                self._hgx_state,
                other._hgx_state,
                *args,
                **kwargs
            )
        
        else:
            return pow(
                self._hgx_state,
                other,
                *args,
                **kwargs
            )
            
    def __lshift__(self, other):
        ''' Wrap __lshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state << other._hgx_state
        
        else:
            return self._hgx_state << other
            
    def __rshift__(self, other):
        ''' Wrap __rshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state >> other._hgx_state
        
        else:
            return self._hgx_state >> other
            
    def __and__(self, other):
        ''' Wrap __and__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state & other._hgx_state
        
        else:
            return self._hgx_state & other
            
    def __xor__(self, other):
        ''' Wrap __xor__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state ^ other._hgx_state
        
        else:
            return self._hgx_state ^ other
            
    def __or__(self, other):
        ''' Wrap __or__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            return self._hgx_state | other._hgx_state
        
        else:
            return self._hgx_state | other
            
    def __radd__(self, other):
        ''' Wrap __radd__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state + self._hgx_state
        
        else:
            return other + self._hgx_state
            
    def __rsub__(self, other):
        ''' Wrap __rsub__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state - self._hgx_state
        
        else:
            return other - self._hgx_state
            
    def __rmul__(self, other):
        ''' Wrap __rmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state * self._hgx_state
        
        else:
            return other * self._hgx_state
            
    def __rmatmul__(self, other):
        ''' Wrap __rmatmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state @ self._hgx_state
        
        else:
            return other @ self._hgx_state
            
    def __rtruediv__(self, other):
        ''' Wrap __rtruediv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state / self._hgx_state
        
        else:
            return other / self._hgx_state
            
    def __rfloordiv__(self, other):
        ''' Wrap __rfloordiv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state // self._hgx_state
        
        else:
            return other // self._hgx_state
            
    def __rmod__(self, other):
        ''' Wrap __rmod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state % self._hgx_state
        
        else:
            return other % self._hgx_state
            
    def __rdivmod__(self, other):
        ''' Wrap __rdivmod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return divmod(other._hgx_state, self._hgx_state)
        
        else:
            return divmod(other, self._hgx_state)
            
    def __rpow__(self, other):
        ''' Wrap __rpow__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return pow(other._hgx_state, self._hgx_state)
        
        else:
            return pow(other, self._hgx_state)
            
    def __rlshift__(self, other):
        ''' Wrap __rlshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state << self._hgx_state
        
        else:
            return other << self._hgx_state
            
    def __rrshift__(self, other):
        ''' Wrap __rrshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state >> self._hgx_state
        
        else:
            return other >> self._hgx_state
            
    def __rand__(self, other):
        ''' Wrap __rand__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state & self._hgx_state
        
        else:
            return other & self._hgx_state
            
    def __rxor__(self, other):
        ''' Wrap __rxor__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state ^ self._hgx_state
        
        else:
            return other ^ self._hgx_state
            
    def __ror__(self, other):
        ''' Wrap __ror__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            return other._hgx_state | self._hgx_state
        
        else:
            return other | self._hgx_state
            
    def __iadd__(self, other):
        ''' Wrap __iadd__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state += other._hgx_state
        
        else:
            self._hgx_state += other
            
        return self
            
    def __isub__(self, other):
        ''' Wrap __isub__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state -= other._hgx_state
        
        else:
            self._hgx_state -= other
            
        return self
            
    def __imul__(self, other):
        ''' Wrap __imul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state *= other._hgx_state
        
        else:
            self._hgx_state *= other
            
        return self
            
    def __imatmul__(self, other):
        ''' Wrap __imatmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state @= other._hgx_state
        
        else:
            self._hgx_state @= other
            
        return self
            
    def __itruediv__(self, other):
        ''' Wrap __itruediv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state /= other._hgx_state
        
        else:
            self._hgx_state /= other
            
        return self
            
    def __ifloordiv__(self, other):
        ''' Wrap __ifloordiv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state //= other._hgx_state
        
        else:
            self._hgx_state //= other
            
        return self
            
    def __imod__(self, other):
        ''' Wrap __imod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state %= other._hgx_state
        
        else:
            self._hgx_state %= other
            
        return self
            
    def __ipow__(self, other):
        ''' Wrap __ipow__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state **= other._hgx_state
        
        else:
            self._hgx_state **= other
            
        return self
            
    def __ilshift__(self, other):
        ''' Wrap __ilshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state <<= other._hgx_state
        
        else:
            self._hgx_state <<= other
            
        return self
            
    def __irshift__(self, other):
        ''' Wrap __irshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state >>= other._hgx_state
        
        else:
            self._hgx_state >>= other
            
        return self
            
    def __iand__(self, other):
        ''' Wrap __iand__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state &= other._hgx_state
        
        else:
            self._hgx_state &= other
            
        return self
            
    def __ixor__(self, other):
        ''' Wrap __ixor__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state ^= other._hgx_state
        
        else:
            self._hgx_state ^= other
            
        return self
            
    def __ior__(self, other):
        ''' Wrap __ior__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* ObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a
        # much safer bet.
        if isinstance(other, Proxy):
            # Other proxies are very likely to fail, since the reveresed call
            # would normally have already been called -- but try them anyways.
            self._hgx_state |= other._hgx_state
        
        else:
            self._hgx_state |= other
            
        return self
            
    def __neg__(self):
        ''' Wrap __neg__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return -(self._hgx_state)
            
    def __pos__(self):
        ''' Wrap __pos__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return +(self._hgx_state)
            
    def __abs__(self):
        ''' Wrap __abs__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return abs(self._hgx_state)
            
    def __invert__(self):
        ''' Wrap __invert__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return ~(self._hgx_state)
            
    def __complex__(self):
        ''' Wrap __complex__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return complex(self._hgx_state)
            
    def __int__(self):
        ''' Wrap __int__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return int(self._hgx_state)
            
    def __float__(self):
        ''' Wrap __float__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return float(self._hgx_state)
            
    def __round__(self):
        ''' Wrap __round__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return round(self._hgx_state)
            
    def __index__(self):
        ''' Wrap __index__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a
        purpose-built script.
        '''
        return operator.index(self._hgx_state)
            

class PickleSerializer:
    ''' An ObjProxy that uses Pickle for serialization. DO NOT, UNDER
    ANY CIRCUMSTANCE, LOAD A PICKLEPROXY FROM AN UNTRUSTED SOURCE. As
    pickled objects can control their own pickling process, and python
    can execute arbitrary shell commands, PickleProxies can be trivially
    used as a rootkit (within the privilege confines of the current
    python process).
    '''
    _hgx_DEFAULT_API = ApiID(bytes(63) + b'\x02')
    
    @staticmethod
    async def hgx_pack(state):
        ''' Packs the object into bytes. For the base proxy, treat the
        input as bytes and return immediately.
        '''
        try:
            return pickle.dumps(state, protocol=4)
            
        except:
            logger.error(
                'Failed to pickle the object w/ traceback: \n' +
                ''.join(traceback.format_exc())
            )
            raise
    
    @staticmethod
    async def hgx_unpack(packed):
        ''' Unpacks the object from bytes. For the base proxy, treat the
        input as bytes and return immediately.
        '''
        try:
            return pickle.loads(packed)
            
        except:
            logger.error(
                'Failed to unpickle the object w/ traceback: \n' +
                ''.join(traceback.format_exc())
            )
            raise
            
            
class PickleObj(PickleSerializer, Obj):
    ''' Use pickle serialization for object.
    '''
    pass
        
        
class PickleProxy(PickleSerializer, Proxy):
    ''' Make a proxy object that serializes with pickle.
    '''
    pass


class JsonSerializer:
    ''' An ObjProxy that uses json for serialization.
    '''
    _hgx_DEFAULT_API = ApiID(bytes(63) + b'\x03')
    
    @staticmethod
    async def hgx_pack(state):
        ''' Packs the object into bytes. For the base proxy, treat the
        input as bytes and return immediately.
        '''
        try:
            # Use the most compact json possible.
            return json.dumps(state, separators=(',', ':')).encode('utf-8')
            
        except:
            logger.error(
                'Failed to pickle the object w/ traceback: \n' +
                ''.join(traceback.format_exc())
            )
            raise
    
    @staticmethod
    async def hgx_unpack(packed):
        ''' Unpacks the object from bytes. For the base proxy, treat the
        input as bytes and return immediately.
        '''
        try:
            return json.loads(packed.decode('utf-8'))
            
        except:
            logger.error(
                'Failed to unpickle the object w/ traceback: \n' +
                ''.join(traceback.format_exc())
            )
            raise
        
        
class JsonObj(JsonSerializer, Obj):
    ''' Make a proxy object that serializes with json.
    '''
    pass
        
        
class JsonProxy(JsonSerializer, Proxy):
    ''' Make a proxy object that serializes with json.
    '''
    pass
