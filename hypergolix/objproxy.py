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
import threading
import asyncio
import operator

from golix import Ghid

# Local dependencies
from .exceptions import DeadObject
from .exceptions import LocallyImmutable
from .exceptions import Unsharable

from .utils import run_coroutine_loopsafe
from .utils import call_coroutine_threadsafe


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
        
# These are all the names in a plain 'ole object()
_OBJECT_NAMESPACE = {
    '__class__', 
    '__delattr__', 
    '__dir__', 
    '__doc__', 
    '__eq__', 
    '__format__', 
    '__ge__', 
    '__getattribute__', 
    '__gt__', 
    '__hash__', 
    '__init__', 
    '__le__', 
    '__lt__', 
    '__ne__', 
    '__new__', 
    '__reduce__', 
    '__reduce_ex__', 
    '__repr__', 
    '__setattr__', 
    '__sizeof__', 
    '__str__', 
    '__subclasshook__'
}

# These are all of the names in a user-defined class object, as best I can tell
_USER_NAMESPACE = {
    '__class__', 
    '__delattr__', 
    '__dict__', 
    '__dir__', 
    '__doc__', 
    '__eq__', 
    '__format__', 
    '__ge__', 
    '__getattr__', 
    '__getattribute__', 
    '__gt__', 
    '__hash__', 
    '__init__', 
    '__le__', 
    '__lt__', 
    '__module__', 
    '__ne__', 
    '__new__', 
    '__reduce__', 
    '__reduce_ex__', 
    '__repr__', 
    '__setattr__', 
    '__sizeof__', 
    '__str__', 
    '__subclasshook__', 
    '__weakref__', 
}
_USER_NAMESPACE_2 = {
    '__init__', 
    '__doc__', 
    '__module__', 
    '__gt__', 
    '__subclasshook__', 
    '__dir__', 
    '__eq__', 
    '__le__', 
    '__dict__', 
    '__class__', 
    '__ge__', 
    '__format__', 
    '__hash__', 
    '__repr__', 
    '__lt__', 
    '__setattr__', 
    '__weakref__', 
    '__delattr__', 
    '__getattribute__', 
    '__reduce_ex__', 
    '__sizeof__', 
    '__ne__', 
    '__reduce__', 
    '__str__', 
    '__new__'
}


class HGXObjBase:
    ''' This is a base object to make an object hypergolix-aware.
    '''
    _HASHMIX_3141592 = 3141592
    _hgx_DEFAULT_API_ID = bytes(64)
    
    def __init__(self, hgxlink, state, api_id, dynamic, private, ghid=None, 
                binder=None):
        ''' Allocates the object locally, but does NOT create it. You
        have to explicitly call hgx_push, hgx_push_threadsafe, or
        hgx_push_loopsafe to actually create the sync'd object and get
        a ghid.
        '''
        # Do this so we don't get circular references and can therefore support
        # our persistence declaration
        # Recasting will result in this being passed a weakref.proxy.
        if isinstance(hgxlink, weakref.ProxyType):
            self._hgxlink_3141592 = hgxlink
        else:
            self._hgxlink_3141592 = weakref.proxy(hgxlink)
        
        self._proxy_3141592 = state
        self._callback_3141592 = None
        self._ghid_3141592 = ghid
        self._binder_3141592 = binder
        self._api_id_3141592 = api_id
        self._private_3141592 = bool(private)
        self._dynamic_3141592 = bool(dynamic)
        self._isalive_3141592 = True
        
    @classmethod
    async def _hgx_recast(cls, obj):
        ''' Takes the passed obj, and attempts to re-cast it as this
        class. Only works for up/down classing; you cannot directly, for
        example, convert between a JsonProxy and a PickleProxy, because
        they have divergent inheritance. Returns a new instance of the
        object, recast as the class you're calling from. Preserves any
        update callbacks, even though they might break from the type
        change.
        
        NOTE THAT THIS WILL RENDER THE PREVIOUS OBJECT DEAD! The "old"
        object will also stop receiving updates from hgxlink.
        
        As examples:
            + HGXObjBase.hgx_recast(<PickleProxy object>) returns the object 
                recast as an HGXObjBase
            + PickleProxy.hgx_recast(<HGXObjBase object>) returns the object
                recast as a PickleProxy
            + PickleProxy.hgx_recast(<JsonProxy object>) raises TypeError
        '''
        # We are going from child to parent
        if issubclass(type(obj), cls):
            # Re-pack the object, and then unpack it.
            state = await obj._hgx_pack(obj._proxy_3141592)
            state = await cls._hgx_unpack(state)
        
        # We are going from parent to child
        elif issubclass(cls, type(obj)):
            # Simply recreate the object with an unpacked state
            state = await cls._hgx_unpack(obj._proxy_3141592)
            
        # We have divergent heritage
        else:
            raise TypeError(
                'HGX objects can only be recast into ancestor or descendant '
                'classes. They cannot be recast into classes with divergent '
                'heritage.'
            )
            
        # Use the state from above to create a new copy of the object.
        recast = cls(
            hgxlink = obj._hgxlink_3141592,
            state = state,
            api_id = obj._api_id_3141592,
            dynamic = obj._dynamic_3141592,
            private = obj._private_3141592,
            ghid = obj._ghid_3141592,
            binder = obj._binder_3141592,
        )
        # Copy over the existing isalive and callback.
        recast._callback_3141592 = obj._callback_3141592
        recast._isalive_3141592 = obj._isalive_3141592
        # Now transfer the subscription to the new object and render the old 
        # inoperable
        obj._hgxlink_3141592.subscribe_to_updates(recast)
        obj._render_inop_3141592()
        
        return recast
        
    @classmethod
    def hgx_recast_threadsafe(cls, obj):
        ''' Takes the passed obj, and attempts to re-cast it as this
        class. Only works for up/down classing; you cannot directly, for
        example, convert between a JsonProxy and a PickleProxy, because
        they have divergent inheritance. Returns a new instance of the
        object, recast as the class you're calling from. Preserves any
        update callbacks, even though they might break from the type
        change.
        
        NOTE THAT THIS WILL RENDER THE PREVIOUS OBJECT DEAD! The "old"
        object will also stop receiving updates from hgxlink.
        
        As examples:
            + HGXObjBase.hgx_recast(<PickleProxy object>) returns the object 
                recast as an HGXObjBase
            + PickleProxy.hgx_recast(<HGXObjBase object>) returns the object
                recast as a PickleProxy
            + PickleProxy.hgx_recast(<JsonProxy object>) raises TypeError
        '''
        return call_coroutine_threadsafe(
            coro = cls._hgx_recast(obj),
            loop = obj._hgxlink_3141592._loop
        )
        
    async def hgx_recast_loopsafe(cls, obj):
        ''' Takes the passed obj, and attempts to re-cast it as this
        class. Only works for up/down classing; you cannot directly, for
        example, convert between a JsonProxy and a PickleProxy, because
        they have divergent inheritance. Returns a new instance of the
        object, recast as the class you're calling from. Preserves any
        update callbacks, even though they might break from the type
        change.
        
        NOTE THAT THIS WILL RENDER THE PREVIOUS OBJECT DEAD! The "old"
        object will also stop receiving updates from hgxlink.
        
        As examples:
            + HGXObjBase.hgx_recast(<PickleProxy object>) returns the object 
                recast as an HGXObjBase
            + PickleProxy.hgx_recast(<HGXObjBase object>) returns the object
                recast as a PickleProxy
            + PickleProxy.hgx_recast(<JsonProxy object>) raises TypeError
        '''
        return (await run_coroutine_loopsafe(
            coro = cls._hgx_recast(obj),
            target_loop = obj._hgxlink_3141592._loop
        ))
        
    @property
    def hgx_state(self):
        ''' Simple pass-through to the internal state. This is a strong
        reference, so if the state is mutable, modifications will be
        applied to the state; however, push() must still be explicitly
        called.
        '''
        return self._proxy_3141592
        
    @hgx_state.setter
    def hgx_state(self, value):
        ''' Allow direct overwriting of the internal state. Does not 
        ensure serializability, nor does it push upstream.
        '''
        self._proxy_3141592 = value
        
    @property
    def hgx_ghid(self):
        ''' This is a read-only, immutable address for the object. It is
        universal. See documentation about ghids.
        '''
        return self._ghid_3141592
        
    @property
    def hgx_api_id(self):
        ''' An identifier for the kind of object. Used during sharing 
        and delivery. Read-only.
        '''
        # Just, yknow, proxy to our internal normalization.
        if self._api_id_3141592 is None:
            return None
        else:
            return self._renormalize_api_id_3141592(self._api_id_3141592)
        
    @property
    def hgx_private(self):
        ''' A private object is only accessible by this particular 
        application, with this particular user. Subsequent instances of
        the application will require the same app_token to retrieve any
        of its private objects. Read-only.
        '''
        return self._private_3141592
        
    @property
    def hgx_dynamic(self):
        ''' Boolean value indicating whether or not this is a dynamic
        object. Static objects cannot be changed; any attempt to update
        upstream for a static object will cause errors.
        '''
        return self._dynamic_3141592
        
    @property
    def hgx_binder(self):
        ''' Essentially the object's author... more or less. Sometimes 
        less.
        '''
        return self._binder_3141592
        
    @property
    def hgx_isalive(self):
        ''' Alive objects are accessible through hypergolix. Dead ones
        are not.
        '''
        return self._isalive_3141592
            
    @property
    def hgx_persistence(self):
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
        
    @hgx_persistence.setter
    def hgx_persistence(self, value):
        ''' Setter for hgx_persistence. Note that this attribute cannot
        be deleted.
        '''
        raise NotImplementedError()

    def _hgx_register_callback(self, callback):
        ''' Register a callback to be called whenever an upstream update
        is received from the hypergolix service. There can be at most
        one callback, of any type (internal, threadsafe, loopsafe), at
        any given time.
        
        This CALLBACK will be called from within the IPC embed's 
        internal event loop.
        
        This METHOD may be called anywhere.
        '''
        # Any handlers passed to us this way can already be called natively 
        # from withinour own event loop, so they just need to be wrapped such 
        # that they never raise.
        async def wrap_callback(*args, callback=callback, **kwargs):
            try:
                await callback(*args, **kwargs)
                
            except:
                logger.error(
                    'Error while running update callback. Traceback: \n' +
                    ''.join(traceback.format_exc())
                )
                
        self._callback_3141592 = wrap_callback

    def hgx_register_callback_threadsafe(self, callback):
        ''' Register a callback to be called whenever an upstream update
        is received from the hypergolix service. There can be at most
        one callback, of any type (internal, threadsafe, loopsafe), at
        any given time.
        
        This CALLBACK will be called from within a single-use, dedicated
        thread.
        
        This METHOD may be called anywhere except from within the 
        internal event loop.
        '''
        # For simplicity, wrap the handler, so that any shares can be called
        # normally from our own event loop.
        async def wrapped_callback(*args, func=callback):
            ''' Wrap the handler in run_in_executor.
            '''
            await self._hgxlink_3141592._loop.run_in_executor(
                self._hgxlink_3141592._executor,
                func,
                *args
            )
        self._hgx_register_callback(wrapped_callback)

    def hgx_register_callback_loopsafe(self, callback, target_loop):
        ''' Register a callback to be called whenever an upstream update
        is received from the hypergolix service. There can be at most
        one callback, of any type (internal, threadsafe, loopsafe), at
        any given time.
        
        This CALLBACK will be called within the specified event loop,
        also implying the specified event loop context (typically, that
        loop's thread).
        
        This METHOD may be called anywhere.
        '''
        async def wrapped_callback(*args, loop=target_loop, coro=callback):
            ''' Wrap the handler in run_in_executor.
            '''
            await run_coroutine_loopsafe(
                coro = coro(*args),
                target_loop = loop
            )
        self._hgx_register_callback(wrapped_callback)

    def hgx_clear_callback(self):
        ''' Clears any registered callback.
        '''
        self._callback_3141592 = None
        
    async def _hgx_push(self):
        ''' Pushes object state upstream.
        '''
        # Error traps for dead object
        if not self._isalive_3141592:
            raise DeadObject()
            
        # The object is still alive.
        if self._ghid_3141592 is None:
            # It's even new!
            ghid, binder = await self._hgxlink_3141592._make_new(obj=self)
            self._ghid_3141592 = ghid
            self._binder_3141592 = binder
        
        # The object is not new. Is it static?
        else:
            # Error trap if the object isn't "owned" by us
            if self._hgxlink_3141592.whoami != self.hgx_binder:
                raise LocallyImmutable('No access rights to mutate object.')
            
            # Error trap if it's static
            elif not self._dynamic_3141592:
                raise LocallyImmutable('Cannot update a static object.')
            
            # All traps passed. Make the call.
            else:
                await self._hgxlink_3141592._make_update(obj=self)

    def hgx_push_threadsafe(self):
        '''
        '''
        call_coroutine_threadsafe(
            coro = self._hgx_push(),
            loop = self._hgxlink_3141592._loop
        )

    async def hgx_push_loopsafe(self):
        '''
        '''
        await run_coroutine_loopsafe(
            coro = self._hgx_push(),
            target_loop = self._hgxlink_3141592._loop
        )

    async def _hgx_sync(self):
        ''' Trivial pass-through to the hgxlink make_sync.
        '''
        if not self._isalive_3141592:
            raise DeadObject()
        else:
            await self._hgxlink_3141592._make_sync(obj=self)

    def hgx_sync_threadsafe(self):
        '''
        '''
        call_coroutine_threadsafe(
            coro = self._hgx_sync(),
            loop = self._hgxlink_3141592._loop
        )

    async def hgx_sync_loopsafe(self):
        '''
        '''
        await run_coroutine_loopsafe(
            coro = self._hgx_sync(),
            target_loop = self._hgxlink_3141592._loop
        )

    async def _hgx_share(self, recipient):
        ''' Trivial pass-through to the hgx make_share, plus a check for
        privacy.
        '''
        if not self._isalive_3141592:
            raise DeadObject()
        elif self.hgx_private:
            raise Unsharable('Cannot share a private object.')
        else:
            await self._hgxlink_3141592._make_share(
                obj = self, 
                recipient = recipient
            )

    def hgx_share_threadsafe(self, recipient):
        '''
        '''
        call_coroutine_threadsafe(
            coro = self._hgx_share(recipient),
            loop = self._hgxlink_3141592._loop
        )

    async def hgx_share_loopsafe(self, recipient):
        '''
        '''
        await run_coroutine_loopsafe(
            coro = self._hgx_share(recipient),
            target_loop = self._hgxlink_3141592._loop
        )

    async def _hgx_freeze(self):
        ''' Trivial pass-through to the hgxlink make_freeze, with type
        checking for mutability.
        '''
        if not self._isalive_3141592:
            raise DeadObject()
        elif not self.hgx_dynamic:
            raise LocallyImmutable('Cannot freeze a static object.')
        else:    
            frozen = await self._hgxlink_3141592._make_freeze(obj=self)
            return frozen

    def hgx_freeze_threadsafe(self):
        '''
        '''
        return call_coroutine_threadsafe(
            coro = self._hgx_freeze(),
            loop = self._hgxlink_3141592._loop
        )

    async def hgx_freeze_loopsafe(self):
        '''
        '''
        return (await run_coroutine_loopsafe(
            coro = self._hgx_freeze(),
            target_loop = self._hgxlink_3141592._loop
        ))

    async def _hgx_hold(self):
        ''' Trivial pass-through to the hgxlink hold.
        '''
        if not self._isalive_3141592:
            raise DeadObject()
        else:
            await self._hgxlink_3141592._make_hold(obj=self)

    def hgx_hold_threadsafe(self):
        '''
        '''
        call_coroutine_threadsafe(
            coro = self._hgx_hold(),
            loop = self._hgxlink_3141592._loop
        )

    async def hgx_hold_loopsafe(self):
        '''
        '''
        await run_coroutine_loopsafe(
            coro = self._hgx_hold(),
            target_loop = self._hgxlink_3141592._loop
        )

    async def _hgx_discard(self):
        ''' Does actually add some value to the hgxlink make_discard.
        '''
        if not self._isalive_3141592:
            raise DeadObject()
        else:
            await self._hgxlink_3141592._make_discard(obj=self)
            self._render_inop_3141592()

    def hgx_discard_threadsafe(self):
        '''
        '''
        call_coroutine_threadsafe(
            coro = self._hgx_discard(),
            loop = self._hgxlink_3141592._loop
        )

    async def hgx_discard_loopsafe(self):
        '''
        '''
        await run_coroutine_loopsafe(
            coro = self._hgx_discard(),
            target_loop = self._hgxlink_3141592._loop
        )

    async def _hgx_delete(self):
        ''' Does actually add some value to the hgxlink make_delete.
        '''
        if not self._isalive_3141592:
            raise DeadObject()
        else:
            await self._hgxlink_3141592._make_delete(obj=self)
            self._render_inop_3141592()

    def hgx_delete_threadsafe(self):
        '''
        '''
        call_coroutine_threadsafe(
            coro = self._hgx_delete(),
            loop = self._hgxlink_3141592._loop
        )

    async def hgx_delete_loopsafe(self):
        '''
        '''
        await run_coroutine_loopsafe(
            coro = self._hgx_delete(),
            target_loop = self._hgxlink_3141592._loop
        )
    
    @staticmethod
    async def _hgx_pack(data):
        ''' Packs the object into bytes. For the base proxy, treat the 
        input as bytes and return immediately.
        '''
        return data
    
    @staticmethod
    async def _hgx_unpack(packed):
        ''' Unpacks the object from bytes. For the base proxy, treat the 
        input as bytes and return immediately.
        '''
        return packed
        
    @staticmethod
    def _renormalize_api_id_3141592(api_id):
        ''' Makes sure that our api_id is symmetric with other contexts.
        In other words, standardize the developer-facing version of the
        api_id, even though the internal one contains an extra reserved
        byte.
        '''
        if len(api_id) == 65:
            return api_id[1:65]
        elif len(api_id) == 64:
            return api_id
        else:
            raise ValueError('Illegal api_id.')
        
    def _render_inop_3141592(self):
        ''' Renders the object locally inoperable, either through a 
        delete or discard.
        '''
        self._isalive_3141592 = False
        self._proxy_3141592 = None
        
    async def _force_delete_3141592(self):
        ''' Does everything needed to clean up the object, after either
        an upstream or local delete.
        '''
        self._render_inop_3141592()
        
        # If there is an update callback defined, run it concurrently.
        if self._callback_3141592 is not None:
            asyncio.ensure_future(self._callback_3141592(self))
        
    async def _force_pull_3141592(self, state):
        ''' Does everything needed to apply an upstream update to the
        object.
        '''
        state = await self._hgx_unpack(state)
        self._proxy_3141592 = state
        
        # If there is an update callback defined, run it concurrently.
        if self._callback_3141592 is not None:
            logger.debug(
                'Update pulled for ' + str(self._ghid_3141592) + '. Running '
                'callback.'
            )
            asyncio.ensure_future(self._callback_3141592(self))
        else:
            logger.debug(
                'Update pulled for ' + str(self._ghid_3141592) + ', but it '
                'has no callback.'
            )
            
    def __repr__(self):
        classname = type(self).__name__
        return (
            '<' + classname + ' at ' + str(self.hgx_ghid) + ' with state ' + 
            repr(self._proxy_3141592) + '>'
        )
        
    def __hash__(self):
        ''' Have a hash, if our ghid address is defined; otherwise, 
        return None (which will in turn cause Python to raise a 
        TypeError in the parent call).
        
        The hashmix is a random value that has been included to allow 
        faster hash bucket differentiation between ghids and objproxies.
        '''
        if self.hgx_ghid is not None:
            return hash(self.hgx_ghid) ^ self._HASHMIX_3141592
        else:
            return None
            
    def __eq__(self, other):
        ''' Equality comparisons on HGXObjBase will return True if and
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
            equality = (self.hgx_ghid == other.hgx_ghid)
            # What is equality?
            equality &= (self.hgx_binder == other.hgx_binder)
            # What is home?
            equality &= (self.hgx_state == other.hgx_state)
            # What are cows?
            
        except AttributeError as exc:
            raise TypeError(
                'Incomparable types: ' + 
                type(self).__name__ + ', ' + 
                type(other).__name__
            ) from exc
            
        return equality


class NoopProxy(HGXObjBase):
    ''' HGX proxies, partly inspired by weakref.proxies, are a mechanism
    by which normal python objects can be "dropboxed" into hypergolix.
    The proxy object, and not the original object, must be referenced.
    
    Several "magic method" / "dunder methods" are explicitly sent to the
    proxy object. If the proxy object does not support those methods,
    they will raise... something or other (it's a little hard to tell, 
    and varies on a case-by-case basis). These are:
        1. __str__
        2. __format__
    
    Proxies pass through all attribute access to their proxied objects,
    with the exception of:
        1.  __init__
        2.  __repr__
        3.  __hash__ (see note [1] below)
        4.  __eq__ (see note [2] below)
        5.  hgx_ghid
        6.  hgx_api_id
        7.  hgx_private
        8.  hgx_dynamic
        9.  hgx_binder
        10. hgx_persistence
        11. hgx_isalive
        12. hgx_update
        12. _hgx_push
        13. hgx_push_threadsafe
        14. hgx_push_loopsafe
        15. _hgx_register_callback
        16. hgx_register_callback_threadsafe
        17. hgx_register_callback_loopsafe
        18. hgx_clear_callback
        19. _hgx_sync
        20. hgx_sync_threadsafe
        21. hgx_sync_loopsafe
        22. _hgx_share
        23. hgx_share_threadsafe
        24. hgx_share_loopsafe
        25. _hgx_freeze
        26. hgx_freeze_threadsafe
        27. hgx_freeze_loopsafe
        28. _hgx_hold
        29. hgx_hold_threadsafe
        30. hgx_hold_loopsafe
        31. _hgx_discard
        32. hgx_discard_threadsafe
        33. hgx_discard_loopsafe
        34. _hgx_delete
        35. hgx_delete_threadsafe
        36. hgx_delete_loopsafe
        39. _hgx_pack
        40. _hgx_unpack
        41. _hgx_DEFAULT_API_ID
    (as well as some name-mangled internal attributes; see note [3] 
    below).
    
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
    
    [3] The primary concern here is NOT enforcing access restrictions,
    which you cannot do in python anyways (we're all consenting adults!)
    but rather to prevent name conflicts, particularly since we're 
    passing through attribute access to arbitrary proxy objets. As such,
    instead of manually enumerating all of the possible implementation
    detail methods, we're name mangling them by postpending '_3141592' 
    to the method name. We're doing this instead of the default python
    name mangling, because we'd like them to be trivially available to
    subclasses (if necessary).
    
    Side note: as per python docs, support for magic methods ("dunder",
    or "double underscore" methods) is only reliable if declared 
    directly and explicitly within the class.
    
    TODO: fix __dir__
    '''
    _hgx_DEFAULT_API_ID = bytes(63) + b'\x01'
    _HASHMIX_3141592 = 936930316
    
    # Declare a static namespace, so that all of these attributes will 
    # be accessible HERE using getattr/setattr. Because the proxy lookup
    # for setattr (in particular) first checks to see if we can find it
    # locally, by setting the namespace like this for the class itself,
    # we can trick the lookup into succeeding.
    _hgxlink_3141592 = None
    _proxy_3141592 = None
    _ghid_3141592 = None
    _binder_3141592 = None
    _api_id_3141592 = None
    _private_3141592 = None
    _dynamic_3141592 = None
    _isalive_3141592 = None
    _callback_3141592 = None
    
    def __init__(self, hgxlink, state, api_id, dynamic, private, ghid=None, 
                binder=None):
        ''' Allocates the object locally, but does NOT create it. You
        have to explicitly call hgx_push, hgx_push_threadsafe, or
        hgx_push_loopsafe to actually create the sync'd object and get
        a ghid.
        '''
        # For now, do this to extract the state from any proxy objects, instead
        # of nesting them. Even though nesting them would be better long-term.
        if isinstance(state, HGXObjBase):
            state = state._proxy_3141592
            
        super().__init__(hgxlink, state, api_id, dynamic, private, ghid, binder)
        
    @property
    def hgx_state(self):
        ''' Simple pass-through to the internal state. This is a strong
        reference, so if the state is mutable, modifications will be
        applied to the state; however, push() must still be explicitly
        called.
        '''
        return self._proxy_3141592
        
    @hgx_state.setter
    def hgx_state(self, value):
        ''' Allow direct overwriting of the internal state. Does not 
        ensure serializability, nor does it push upstream.
        '''
        # For now, do this to extract the state from any proxy objects, instead
        # of nesting them. Even though nesting them would be better long-term.
        if isinstance(value, HGXObjBase):
            value = value._proxy_3141592
        
        self._proxy_3141592 = value
            
    def __repr__(self):
        classname = type(self).__name__
        return (
            '<' + classname + ' to ' + repr(self._proxy_3141592) + 
            ' at ' + str(self.hgx_ghid) + '>'
        )
            
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
            setattr(self._proxy_3141592, name, value)
            
        # We succeeded to get it here. Set it here.
        else:
            super().__setattr__(name, value)
        
    def __getattr__(self, name):
        ''' Redirect all missing attribute lookups to the proxy.
        Note that getattr is only called if the normal lookup fails. So, 
        we don't need to check for an attributeerror locally, because 
        we're guaranteed to get one.
        '''
        return getattr(self._proxy_3141592, name)
        
    def __delattr__(self, name):
        ''' Permanently prevent deletion of all local attributes, and 
        pass any others to the referenced object.
        '''
        # Try to GET the attribute with US, the actual proxy.
        try:
            super().__getattribute__(name)
        
        # We failed to get it here. Pass the setattr to the referenced object.
        except AttributeError:
            delattr(self._proxy_3141592, name)
            
        # We succeeded to get it here. Set it here.
        else:
            raise AttributeError('Cannot delete proxy-internal attributes.')
        
    def __eq__(self, other):
        ''' Pass the equality comparison straight into the state.
        '''
        # If the other instance is also an HGXObjBase, do a full comparison.
        try:
            return super().__eq__(other)
            
        # If not, just compare our proxy state directly to the other object.
        except TypeError:
            return self._proxy_3141592 == other
        
    def __gt__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _proxy_3141592 attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._proxy_3141592 > other._proxy_3141592
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._proxy_3141592 > other
        
    def __ge__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _proxy_3141592 attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._proxy_3141592 >= other._proxy_3141592
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._proxy_3141592 >= other
        
    def __lt__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _proxy_3141592 attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._proxy_3141592 < other._proxy_3141592
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._proxy_3141592 < other
        
    def __le__(self, other):
        ''' Pass the comparison straight into the state.
        '''
        # If the other instance also has an _proxy_3141592 attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare
        try:
            return self._proxy_3141592 <= other._proxy_3141592
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._proxy_3141592 <= other
        
    def __dir__(self):
        ''' Implement a dir that attempts to only list the methods that
        will actually succeed -- ie, cut out any automatically-generated
        special/magic/dunder methods that have not also been defined at
        the referenced object.
        '''
        # Get all of our normal dirs.
        this_dir = set(super().__dir__())
        # Get all of our proxy's dirs.
        prox_dir = set(dir(self._proxy_3141592))
        
        # Remove any of our explicit pass-through special/magic/dunder methods
        total_dir = this_dir - self._ALL_METAD_3141592
        # Add in all of the proxy_dir
        total_dir.update(prox_dir)
        
        return total_dir
        
    # BEGIN AUTOMATICALLY-GENERATED METHODRY HERE!
    # ----------------------------------------------------
        
    _ALL_METAD_3141592 = {
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
        return bool(self._proxy_3141592)
        
    def __bytes__(self):
        ''' Wrap __bytes__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return bytes(self._proxy_3141592)
        
    def __str__(self):
        ''' Wrap __str__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return str(self._proxy_3141592)
        
    def __format__(self, *args, **kwargs):
        ''' Wrap __format__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return format(self._proxy_3141592, *args, **kwargs)
        
    def __len__(self):
        ''' Wrap __len__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return len(self._proxy_3141592)
        
    def __length_hint__(self):
        ''' Wrap __length_hint__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return operator.length_hint(self._proxy_3141592)
        
    def __call__(self, *args, **kwargs):
        ''' Wrap __call__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592(*args, **kwargs)
        
    def __getitem__(self, key):
        ''' Wrap __getitem__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592[key]
        
    def __missing__(self, key):
        ''' Wrap __missing__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__missing__(key)
        
    def __setitem__(self, key, value):
        ''' Wrap __setitem__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        self._proxy_3141592[key] = value
        
    def __delitem__(self, key):
        ''' Wrap __delitem__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        del self._proxy_3141592[key]
        
    def __iter__(self):
        ''' Wrap __iter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return iter(self._proxy_3141592)
        
    def __reversed__(self):
        ''' Wrap __reversed__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return reversed(self._proxy_3141592)
        
    def __contains__(self, item):
        ''' Wrap __contains__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return item in self._proxy_3141592
        
    def __enter__(self):
        ''' Wrap __enter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__enter__()
        
    def __exit__(self, *args, **kwargs):
        ''' Wrap __exit__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__exit__(*args, **kwargs)
        
    def __aenter__(self):
        ''' Wrap __aenter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__aenter__()
        
    def __aexit__(self, *args, **kwargs):
        ''' Wrap __aexit__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__aexit__(*args, **kwargs)
        
    def __await__(self):
        ''' Wrap __await__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__await__()
        
    def __aiter__(self):
        ''' Wrap __aiter__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__aiter__()
        
    def __anext__(self):
        ''' Wrap __anext__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return self._proxy_3141592.__anext__()
        
    def __add__(self, other):
        ''' Wrap __add__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 + other._proxy_3141592
        
        else:
            return self._proxy_3141592 + other
            
    def __sub__(self, other):
        ''' Wrap __sub__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 - other._proxy_3141592
        
        else:
            return self._proxy_3141592 - other
            
    def __mul__(self, other):
        ''' Wrap __mul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 * other._proxy_3141592
        
        else:
            return self._proxy_3141592 * other
            
    def __matmul__(self, other):
        ''' Wrap __matmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 @ other._proxy_3141592
        
        else:
            return self._proxy_3141592 @ other
            
    def __truediv__(self, other):
        ''' Wrap __truediv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 / other._proxy_3141592
        
        else:
            return self._proxy_3141592 / other
            
    def __floordiv__(self, other):
        ''' Wrap __floordiv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 // other._proxy_3141592
        
        else:
            return self._proxy_3141592 // other
            
    def __mod__(self, other):
        ''' Wrap __mod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 % other._proxy_3141592
        
        else:
            return self._proxy_3141592 % other
            
    def __divmod__(self, other, *args, **kwargs):
        ''' Wrap __divmod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return divmod(
                self._proxy_3141592, 
                other._proxy_3141592,
                *args, 
                **kwargs
            )
        
        else:
            return divmod(
                self._proxy_3141592, 
                other,
                *args, 
                **kwargs
            )
            
    def __pow__(self, other, *args, **kwargs):
        ''' Wrap __pow__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return pow(
                self._proxy_3141592, 
                other._proxy_3141592,
                *args, 
                **kwargs
            )
        
        else:
            return pow(
                self._proxy_3141592, 
                other,
                *args, 
                **kwargs
            )
            
    def __lshift__(self, other):
        ''' Wrap __lshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 << other._proxy_3141592
        
        else:
            return self._proxy_3141592 << other
            
    def __rshift__(self, other):
        ''' Wrap __rshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 >> other._proxy_3141592
        
        else:
            return self._proxy_3141592 >> other
            
    def __and__(self, other):
        ''' Wrap __and__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 & other._proxy_3141592
        
        else:
            return self._proxy_3141592 & other
            
    def __xor__(self, other):
        ''' Wrap __xor__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 ^ other._proxy_3141592
        
        else:
            return self._proxy_3141592 ^ other
            
    def __or__(self, other):
        ''' Wrap __or__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 | other._proxy_3141592
        
        else:
            return self._proxy_3141592 | other
            
    def __radd__(self, other):
        ''' Wrap __radd__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 + self._proxy_3141592
        
        else:
            return other + self._proxy_3141592
            
    def __rsub__(self, other):
        ''' Wrap __rsub__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 - self._proxy_3141592
        
        else:
            return other - self._proxy_3141592
            
    def __rmul__(self, other):
        ''' Wrap __rmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 * self._proxy_3141592
        
        else:
            return other * self._proxy_3141592
            
    def __rmatmul__(self, other):
        ''' Wrap __rmatmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 @ self._proxy_3141592
        
        else:
            return other @ self._proxy_3141592
            
    def __rtruediv__(self, other):
        ''' Wrap __rtruediv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 / self._proxy_3141592
        
        else:
            return other / self._proxy_3141592
            
    def __rfloordiv__(self, other):
        ''' Wrap __rfloordiv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 // self._proxy_3141592
        
        else:
            return other // self._proxy_3141592
            
    def __rmod__(self, other):
        ''' Wrap __rmod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 % self._proxy_3141592
        
        else:
            return other % self._proxy_3141592
            
    def __rdivmod__(self, other):
        ''' Wrap __rdivmod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return divmod(other._proxy_3141592, self._proxy_3141592)
        
        else:
            return divmod(other, self._proxy_3141592)
            
    def __rpow__(self, other):
        ''' Wrap __rpow__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return pow(other._proxy_3141592, self._proxy_3141592)
        
        else:
            return pow(other, self._proxy_3141592)
            
    def __rlshift__(self, other):
        ''' Wrap __rlshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 << self._proxy_3141592
        
        else:
            return other << self._proxy_3141592
            
    def __rrshift__(self, other):
        ''' Wrap __rrshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 >> self._proxy_3141592
        
        else:
            return other >> self._proxy_3141592
            
    def __rand__(self, other):
        ''' Wrap __rand__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 & self._proxy_3141592
        
        else:
            return other & self._proxy_3141592
            
    def __rxor__(self, other):
        ''' Wrap __rxor__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 ^ self._proxy_3141592
        
        else:
            return other ^ self._proxy_3141592
            
    def __ror__(self, other):
        ''' Wrap __ror__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 | self._proxy_3141592
        
        else:
            return other | self._proxy_3141592
            
    def __iadd__(self, other):
        ''' Wrap __iadd__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 += other._proxy_3141592
        
        else:
            self._proxy_3141592 += other
            
        return self
            
    def __isub__(self, other):
        ''' Wrap __isub__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 -= other._proxy_3141592
        
        else:
            self._proxy_3141592 -= other
            
        return self
            
    def __imul__(self, other):
        ''' Wrap __imul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 *= other._proxy_3141592
        
        else:
            self._proxy_3141592 *= other
            
        return self
            
    def __imatmul__(self, other):
        ''' Wrap __imatmul__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 @= other._proxy_3141592
        
        else:
            self._proxy_3141592 @= other
            
        return self
            
    def __itruediv__(self, other):
        ''' Wrap __itruediv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 /= other._proxy_3141592
        
        else:
            self._proxy_3141592 /= other
            
        return self
            
    def __ifloordiv__(self, other):
        ''' Wrap __ifloordiv__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 //= other._proxy_3141592
        
        else:
            self._proxy_3141592 //= other
            
        return self
            
    def __imod__(self, other):
        ''' Wrap __imod__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 %= other._proxy_3141592
        
        else:
            self._proxy_3141592 %= other
            
        return self
            
    def __ipow__(self, other):
        ''' Wrap __ipow__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 **= other._proxy_3141592
        
        else:
            self._proxy_3141592 **= other
            
        return self
            
    def __ilshift__(self, other):
        ''' Wrap __ilshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 <<= other._proxy_3141592
        
        else:
            self._proxy_3141592 <<= other
            
        return self
            
    def __irshift__(self, other):
        ''' Wrap __irshift__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 >>= other._proxy_3141592
        
        else:
            self._proxy_3141592 >>= other
            
        return self
            
    def __iand__(self, other):
        ''' Wrap __iand__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 &= other._proxy_3141592
        
        else:
            self._proxy_3141592 &= other
            
        return self
            
    def __ixor__(self, other):
        ''' Wrap __ixor__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 ^= other._proxy_3141592
        
        else:
            self._proxy_3141592 ^= other
            
        return self
            
    def __ior__(self, other):
        ''' Wrap __ior__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 |= other._proxy_3141592
        
        else:
            self._proxy_3141592 |= other
            
        return self
            
    def __neg__(self):
        ''' Wrap __neg__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return -(self._proxy_3141592)
            
    def __pos__(self):
        ''' Wrap __pos__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return +(self._proxy_3141592)
            
    def __abs__(self):
        ''' Wrap __abs__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return abs(self._proxy_3141592)
            
    def __invert__(self):
        ''' Wrap __invert__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return ~(self._proxy_3141592)
            
    def __complex__(self):
        ''' Wrap __complex__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return complex(self._proxy_3141592)
            
    def __int__(self):
        ''' Wrap __int__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return int(self._proxy_3141592)
            
    def __float__(self):
        ''' Wrap __float__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return float(self._proxy_3141592)
            
    def __round__(self):
        ''' Wrap __round__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return round(self._proxy_3141592)
            
    def __index__(self):
        ''' Wrap __index__ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return operator.index(self._proxy_3141592)
            

class HGXPickleObj(HGXObjBase):
    ''' An ObjProxy that uses Pickle for serialization. DO NOT, UNDER 
    ANY CIRCUMSTANCE, LOAD A PICKLEPROXY FROM AN UNTRUSTED SOURCE. As
    pickled objects can control their own pickling process, and python 
    can execute arbitrary shell commands, PickleProxies can be trivially
    used as a rootkit (within the privilege confines of the current 
    python process).
    '''
        
        
class PickleProxy(HGXPickleObj, NoopProxy):
    ''' Make a proxy object that serializes with pickle.
    '''
    pass


class HGXJsonObj(HGXObjBase):
    ''' An ObjProxy that uses json for serialization.
    '''
        
        
class JsonProxy(HGXJsonObj, NoopProxy):
    ''' Make a proxy object that serializes with json.
    '''
    pass