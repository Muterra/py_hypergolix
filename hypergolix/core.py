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

# External deps
import logging
import weakref
import threading
import traceback

from golix import FirstParty
from golix import SecondParty
from golix import Ghid

# Internal deps
from .hypothetical import API
from .hypothetical import public_api
from .hypothetical import fixture_api
from .hypothetical import fixture_noop

from .utils import NoContext
from .utils import weak_property
from .utils import readonly_property

from .persistence import _GeocLite

from .exceptions import UnknownParty


# ###############################################
# Boilerplate
# ###############################################


logger = logging.getLogger(__name__)


# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'GolixCore',
]

        
# ###############################################
# Lib
# ###############################################
            
            
class GolixCore(metaclass=API):
    ''' Wrapper around Golix library that automates much of the state
    management, holds the Agent's identity, etc etc.
    '''
    _librarian = weak_property('__librarian')
    DEFAULT_LEGROOM = 7
    
    # Async stuff
    _executor = readonly_property('__executor')
    _loop = readonly_property('__loop')
    
    @public_api
    def __init__(self, executor, loop, *args, **kwargs):
        ''' Create a new agent. Persister should subclass _PersisterBase
        (eventually this requirement may be changed).
        
        persister isinstance _PersisterBase
        dispatcher isinstance DispatcherBase
        _identity isinstance golix.FirstParty
        '''
        super().__init__(*args, **kwargs)
        self._mutex_request = threading.Lock()
        self._mutex_container = threading.Lock()
        self._mutex_sbinding = threading.Lock()
        self._mutex_dbinding = threading.Lock()
        self._mutex_xbinding = threading.Lock()
        
        # Added during bootstrap
        self.__identity = None
        
        # Async-specific stuff
        setattr(self, '__executor', executor)
        setattr(self, '__loop', loop)
        
    @__init__.fixture
    def __init__(self, test_agent, *args, librarian=None, **kwargs):
        ''' Just, yknow, throw in the towel. Err, agent. Whatever.
        '''
        super(GolixCore.__fixture__, self).__init__(
            executor = None,
            loop = None,
            *args,
            **kwargs
        )
        self._identity = test_agent
        
        if librarian is not None:
            self._librarian = librarian
        
    def assemble(self, librarian):
        # Chicken, meet egg.
        self._librarian = librarian
        
    def bootstrap(self, account):
        # This must be done ASAGDFP. Must be absolute first thing to bootstrap.
        self._identity = account._identity
        
    @property
    def _identity(self):
        ''' Wrap our identity in such a way that, if it's not defined,
        we can fall back on the FirstParty class.
        '''
        if self.__identity is None:
            return FirstParty
        else:
            return self.__identity
            
    @_identity.setter
    def _identity(self, identity):
        ''' Set the identity.
        '''
        self.__identity = identity
        
    @property
    @public_api
    def whoami(self):
        ''' Return the Agent's Ghid.
        '''
        return self._identity.ghid
    
    @public_api
    async def unpack_request(self, request):
        ''' Just like it says on the label...
        Note that the request is PACKED, not unpacked.
        '''
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._unpack_request,
            request
        ))
        
    @unpack_request.fixture
    async def unpack_request(self, request):
        ''' Bypass executor for the fixture.
        '''
        return self._unpack_request(request)
        
    def _unpack_request(self, request):
        ''' Just like it says on the label...
        Note that the request is PACKED, not unpacked.
        '''
        with self._mutex_request:
            unpacked = self._identity.unpack_request(request)
        return unpacked
        
    @public_api
    async def open_request(self, unpacked):
        ''' Just like it says on the label...
        Note that the request is UNPACKED, not packed.
        '''
        try:
            requestor = SecondParty.from_packed(
                await self._librarian.retrieve(unpacked.author)
            )
            
        except KeyError as exc:
            raise UnknownParty(
                'Request author unknown: ' + str(unpacked.author)
            ) from exc
        
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._open_request,
            unpacked,
            requestor
        ))
        
    @open_request.fixture
    async def open_request(self, unpacked):
        ''' Also bypass executor here.
        '''
        requestor = SecondParty.from_packed(
            await self._librarian.retrieve(unpacked.author)
        )
        return self._open_request(unpacked, requestor)
        
    def _open_request(self, unpacked, requestor):
        ''' Just like it says on the label...
        Note that the request is UNPACKED, not packed.
        '''
        return self._identity.receive_request(requestor, unpacked)
    
    @public_api
    async def make_request(self, recipient, payload):
        # Just like it says on the label...
        try:
            recipient = SecondParty.from_packed(
                await self._librarian.retrieve(recipient)
            )
        except KeyError as exc:
            raise UnknownParty(
                'Request author unknown: ' + str(recipient)
            ) from exc
        
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._make_request,
            recipient,
            payload
        ))
        
    @make_request.fixture
    async def make_request(self, recipient, payload):
        ''' Bypass the goddamn executor. Ffs.
        '''
        recipient = SecondParty.from_packed(
            await self._librarian.retrieve(recipient)
        )
        return self._make_request(recipient, payload)
        
    def _make_request(self, recipient, payload):
        # Just like it says on the label...
        with self._mutex_request:
            return self._identity.make_request(
                recipient = recipient,
                request = payload,
            )
    
    @public_api
    async def open_container(self, container, secret):
        author = SecondParty.from_packed(
            await self._librarian.retrieve(container.author)
        )
        
        # Wrapper around golix.FirstParty.receive_container.
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._open_container,
            container,
            secret,
            author
        ))
        
    @open_container.fixture
    async def open_container(self, container, secret):
        ''' Bypass executor for fixture.
        '''
        author = SecondParty.from_packed(
            await self._librarian.retrieve(container.author)
        )
        
        return self._open_container(container, secret, author)
        
    def _open_container(self, container, secret, author):
        # Wrapper around golix.FirstParty.receive_container.
        with self._mutex_container:
            return self._identity.receive_container(
                author = author,
                secret = secret,
                container = container
            )
    
    @public_api
    async def make_container(self, data, secret):
        # Simple wrapper around golix.FirstParty.make_container
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._make_container,
            data,
            secret
        ))
        
    @make_container.fixture
    async def make_container(self, data, secret):
        ''' Bypass executor.
        '''
        return self._make_container(data, secret)
        
    def _make_container(self, data, secret):
        # Simple wrapper around golix.FirstParty.make_container
        with self._mutex_container:
            return self._identity.make_container(
                secret = secret,
                plaintext = data
            )

    @public_api
    async def make_binding_stat(self, target):
        # Note that this requires no open() method, as bindings are verified by
        # the local persister.
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._make_binding_stat,
            target
        ))
        
    @make_binding_stat.fixture
    async def make_binding_stat(self, target):
        ''' Skip the executor for the fixture.
        '''
        return self._make_binding_stat(target)

    def _make_binding_stat(self, target):
        # Note that this requires no open() method, as bindings are verified by
        # the local persister.
        with self._mutex_sbinding:
            return self._identity.make_bind_static(target)
    
    @public_api
    async def make_binding_dyn(self, target, ghid=None, history=None):
        ''' Make a new dynamic binding frame.
        If supplied, ghid is the dynamic address, and history is an
        ordered iterable of the previous frame ghids.
        '''
        # Either ghid AND history must be defined, XOR ghid AND history must be
        # undefined.
        if bool(ghid) ^ bool(history):
            raise ValueError('Mixed def of ghid/history while dyn binding.')
        
        if ghid:
            current_binding = await self._librarian.summarize(ghid)
            counter = current_binding.counter + 1
        else:
            counter = 0
            
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._make_binding_dyn,
            ghid,
            counter,
            target,
            history
        ))
        
    @make_binding_dyn.fixture
    async def make_binding_dyn(self, target, ghid=None, history=None):
        ''' Yep, just bypass the executor
        '''
        if ghid:
            current_binding = await self._librarian.summarize(ghid)
            counter = current_binding.counter + 1
        else:
            counter = 0
        
        return self._make_binding_dyn(ghid, counter, target, history)
        
    def _make_binding_dyn(self, ghid, counter, target, history):
        ''' Make a new dynamic binding frame.
        If supplied, ghid is the dynamic address, and history is an
        ordered iterable of the previous frame ghids.
        '''
        if history is not None:
            target_vector = [target, *history]
        else:
            target_vector = [target]
        
        with self._mutex_dbinding:
            return self._identity.make_bind_dynamic(
                counter = counter,
                target_vector = target_vector,
                ghid_dynamic = ghid
            )
    
    @public_api
    async def make_debinding(self, target):
        # Simple wrapper around golix.FirstParty.make_debind
        # Run the actual function in the executor
        return (await self._loop.run_in_executor(
            self._executor,
            self._make_debinding,
            target
        ))
        
    @make_debinding.fixture
    async def make_debinding(self, target):
        ''' Bypass executor for the fixture.
        '''
        return self._make_debinding(target)
        
    def _make_debinding(self, target):
        # Simple wrapper around golix.FirstParty.make_debind
        with self._mutex_xbinding:
            return self._identity.make_debind(target)


class GhidProxier(metaclass=API):
    ''' Resolve the base container GHID from any associated ghid. Uses
    all weak references, so should not interfere with GCing objects.
    '''
    # Note that we can't really cache aliases, because their proxies will
    # not update when we change things unless the proxy is also removed
    # from the cache. Since the objects may (or may not) exist locally in
    # memory anyways, we should just take advantage of that, and allow our
    # inquisitor to more easily manage memory consumption as well.
    _librarian = weak_property('__librarian')
    
    @fixture_api
    def __init__(self, *args, **kwargs):
        ''' Add in a dict to store resolutions.
        '''
        super(GhidProxier.__fixture__, self).__init__(*args, **kwargs)
        self.lookup = {}
        
    def assemble(self, librarian):
        # Chicken, meet egg.
        self._librarian = librarian
        
    def __mklink(self, proxy, target):
        ''' Set, or update, a ghid proxy.
        
        Ghids must only ever have a single proxy. Calling chain on an 
        existing proxy will update the target.
        '''
        raise NotImplementedError('Explicit link creation has been removed.')
        
        if not isinstance(proxy, Ghid):
            raise TypeError('Proxy must be Ghid.')
            
        if not isinstance(target, Ghid):
            raise TypeError('Target must be ghid.')
        
        with self._modlock:
            self._refs[proxy] = target
    
    @public_api
    async def resolve(self, ghid):
        ''' Protect the entry point with a global lock, but don't leave
        the recursive bit alone.
        
        TODO: make this guarantee, through using the persister's
        librarian, that the resolved ghid IS, in fact, a container.
        
        TODO: consider adding a depth limit to resolution.
        '''
        if not isinstance(ghid, Ghid):
            raise TypeError('Can only resolve a ghid.')
            
        return (await self._resolve(ghid))
        
    async def _resolve(self, ghid):
        ''' Recursively resolves the container ghid for a proxy (or a
        container).
        '''
        try:
            obj = await self._librarian.summarize(ghid)
        
        # TODO: make this an error?
        except KeyError:
            logger.warning(''.join((
                'GAO ',
                str(ghid),
                ' address resolver failed to verify: missing at librarian.\n',
                traceback.format_exc()
            )))
            
            result = ghid
        
        else:
            if isinstance(obj, _GeocLite):
                result = ghid
                
            else:
                result = await self._resolve(obj.target)
                
        return result
        
    @resolve.fixture
    async def resolve(self, ghid):
        ''' Ehhhh, okay. So we're going to fixture this, mostly for
        privateer, in a way that just returns the ghid immediately.
        '''
        if ghid in self.lookup:
            return self.lookup[ghid]
        else:
            return ghid
        
        
class Oracle(metaclass=API):
    ''' Source for total internal truth and state tracking of objects.
    
    Maintains <ghid>: <obj> lookup. Used by dispatchers to track obj
    state. Might eventually be used by AgentBase. Just a quick way to
    store and retrieve any objects based on an associated ghid.
    '''
    # These are actually used by the oracle itself
    _salmonator = weak_property('__salmonator')
    
    # These are only here to pass along to GAOs
    _golcore = weak_property('__golcore')
    _ghidproxy = weak_property('__ghidproxy')
    _privateer = weak_property('__privateer')
    _percore = weak_property('__percore')
    _librarian = weak_property('__librarian')
    
    def __init__(self, *args, **kwargs):
        ''' Sets up internal tracking.
        '''
        super().__init__(*args, **kwargs)
        self._lookup = {}
        
    @fixture_api
    def RESET(self):
        ''' Simply re-call init.
        '''
        self._lookup.clear()
        
    def assemble(self, golcore, ghidproxy, privateer, percore, librarian,
                 salmonator):
        # Chicken, meet egg.
        self._golcore = golcore
        self._ghidproxy = ghidproxy
        self._privateer = privateer
        self._percore = percore
        self._librarian = librarian
        self._salmonator = salmonator
        
    @fixture_api
    def add_object(self, ghid, obj):
        ''' Add an object to the fixture.
        '''
        self._lookup[ghid] = obj
            
    @public_api
    async def get_object(self, gaoclass, ghid, *args, **kwargs):
        ''' Get an object.
        '''
        if ghid in self._lookup:
            obj = self._lookup[ghid]
            logger.debug(''.join((
                'GAO ',
                str(ghid),
                ' already exists in Oracle memory.'
            )))
            
            if not isinstance(obj, gaoclass):
                raise TypeError(
                    'Object has already been resolved, and is not the '
                    'correct GAO class.'
                )
        
        else:
            logger.info(''.join((
                'GAO ',
                str(ghid),
                ' not currently in Oracle memory. Attempting load.'
            )))
            
            # First create the actual GAO. We do not need to have the ghid
            # downloaded to do this -- object creation is just making a Python
            # object locally.
            obj = gaoclass(
                ghid,
                None,   # dynamic
                None,   # author
                7,      # legroom (will be overwritten by pull)
                *args,
                golcore = self._golcore,
                ghidproxy = self._ghidproxy,
                privateer = self._privateer,
                percore = self._percore,
                librarian = self._librarian,
                **kwargs
            )
            
            # Now immediately subscribe to the object upstream, so that there
            # is no race condition getting updates
            await self._salmonator.register(obj)
            # Explicitly pull the object from salmonator to ensure we have the
            # newest version, and that it is available locally in librarian if
            # also available anywhere else. Note that salmonator handles modal
            # switching for dynamic/static. It will also (by default, with
            # skip_refresh=False) pull in any updates that have accumulated in
            # the meantime.
            await self._salmonator.attempt_pull(ghid, quiet=True)
            
            # Now actually fetch the object. This may KeyError if the ghid is
            # still unknown.
            await obj._pull()
            
            # Always do this to make sure we have the most recent version
            self._lookup[ghid] = obj
            
        return obj
        
    @get_object.fixture
    async def get_object(self, gaoclass, ghid, *args, **kwargs):
        ''' Do the easy thing and just pull it out of lookup.
        '''
        return self._lookup[ghid]
        
    @public_api
    async def new_object(self, gaoclass, dynamic, legroom, *args, **kwargs):
        ''' Creates a new object and returns it. Passes all *kwargs to
        the declared gao_class. Requires a zeroth state, and calls push
        internally.
        '''
        obj = gaoclass(
            None,                   # ghid
            dynamic,
            self._golcore.whoami,   # author
            legroom,
            *args,
            golcore = self._golcore,
            ghidproxy = self._ghidproxy,
            privateer = self._privateer,
            percore = self._percore,
            librarian = self._librarian,
            **kwargs
        )
        await obj._push()
        
        # Do this before registering with salmonator, in case the latter errors
        self._lookup[obj.ghid] = obj
        
        # Finally, register to receive any concurrent updates from other
        # simultaneous sessions, and then return the object
        await self._salmonator.register(obj)
        return obj
            
    @new_object.fixture
    async def new_object(self, *args, **kwargs):
        ''' Relies upon add_object, but otherwise just pops something
        from the lookup.
        '''
        ghid, obj = self._lookup.popitem()
        self._lookup[ghid] = obj
        return obj
        
    def forget(self, ghid):
        ''' Removes the object from the cache. Next time an application
        wants it, it will need to be acquired from persisters.
        
        Indempotent; will not raise KeyError if called more than once.
        '''
        try:
            del self._lookup[ghid]
        except KeyError:
            logger.debug(str(ghid) + ' unknown to oracle.')
            
    def __contains__(self, ghid):
        ''' Checks for the ghid in cache (but does not check for global
        availability; that would require checking the persister for its
        existence and the privateer for access).
        '''
        return ghid in self._lookup
