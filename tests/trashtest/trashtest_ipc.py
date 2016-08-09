'''
Scratchpad for test-based development.

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

import IPython
import unittest
import warnings
import collections
import threading
import time
import asyncio
import random
import traceback
import logging

from hypergolix.core import GolixCore
from hypergolix.core import Oracle
from hypergolix.core import GhidProxier
from hypergolix.dispatch import _Dispatchable
from hypergolix.dispatch import Dispatcher
from hypergolix.privateer import Privateer

from hypergolix.remotes import MemoryPersister

from hypergolix.utils import Aengel
from hypergolix.utils import SetMap
from hypergolix.comms import Autocomms
from hypergolix.comms import WSBasicServer
from hypergolix.comms import WSBasicClient
from hypergolix.ipc import IPCHost
from hypergolix.ipc import IPCEmbed

from golix import Ghid

# from hypergolix.embeds import WebsocketsEmbed


# ###############################################
# Testing fixtures
# ###############################################


from _fixtures.identities import TEST_AGENT1
from _fixtures.identities import TEST_AGENT2


class MockDispatchable:
    def __init__(self, author, dynamic, api_id):
        self.author = author
        self.dynamic = dynamic
        self.api_id = api_id
        
        
class MockGolcore:
    def __init__(self, whoami):
        self.whoami = whoami.ghid
        

class MockDispatch:
    ''' Test fixture for dispatch.
    
    Oops, didn't actually need to do this. I think?
    '''
    def __init__(self, app_token, whoami):
        self.incoming = []
        self.acks = []
        self.naks = []
        
        self._golcore = MockGolcore(whoami)
        self.app_token = app_token
        
        self.endpoint_registered = False
        
        self.next_obj_dyn = False
        self.next_obj_api = bytes(65)
        self.next_obj_auth = bytes(65)
        
    def assemble(self, *args, **kwargs):
        # Noop
        pass
        
    def bootstrap(self, *args, **kwargs):
        # Noop
        pass
        
    def dispatch_share(self, target):
        self.incoming.append(target)
        
    def dispatch_share_ack(self, target, recipient):
        self.acks.append((target, recipient))
        
    def dispatch_share_nak(self, target, recipient):
        self.naks.append((target, recipient))
        
    def get_object(self, asking_token, ghid):
        ''' Gets an object by ghid for a specific endpoint. Currently 
        only works for non-private objects.
        '''
        return MockDispatchable(
            self.next_obj_auth, 
            self.next_obj_dyn, 
            self.next_obj_api
        )
        
    def new_object(self, asking_token, *args, **kwargs):
        ''' Creates a new object with the upstream golix provider.
        asking_token is the app_token requesting the object.
        *args and **kwargs are passed to Oracle.new_object(), which are 
            in turn passed to the _GAO_Class (probably _Dispatchable)
        ''' 
        return MockDispatchable(
            self.next_obj_auth,
            self.next_obj_dyn,
            self.next_obj_api
        )
        
        return Ghid.from_bytes(b'\x01' + bytes(64))
        
    def update_object(self, asking_token, ghid, state):
        ''' Initiates an update of an object. Must be tied to a specific
        endpoint, to prevent issuing that endpoint a notification in 
        return.
        '''
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            dispatch = self, 
            ghid = ghid,
        )
                
        # Try updating golix before local.
        # Temporarily silence updates from persister about the ghid we're 
        # in the process of updating
        try:
            # This is now handled internally by _GAO in the _Dispatchable
            # self._ignore_subs_because_updating.add(ghid)
            obj.update(state)
        except:
            # Note: because a push() failure restores previous state, we should 
            # probably distribute it INCLUDING TO THE ASKING TOKEN if there's a 
            # push failure. TODO: think about this more.
            raise
        else:
            self.distribute_to_endpoints(ghid, skip_token=asking_token)
        # See above re: no longer needed (handled by _GAO)
        # finally:
        #     self._ignore_subs_because_updating.remove(ghid)
        
    def freeze_object(self, asking_token, ghid):
        ''' Converts a dynamic object to a static object, returning the
        static ghid.
        '''
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            dispatch = self, 
            ghid = ghid
        )
        
        if not obj.dynamic:
            raise DispatchError('Cannot freeze a static object.')
            
        static_address = self._golcore.freeze_dynamic(
            ghid_dynamic = ghid
        )
        
        # We're going to avoid a race condition by pulling the freezed object
        # post-facto, instead of using a cache.
        self._oracle.get_object(
            gaoclass = _Dispatchable,
            dispatch = self, 
            ghid = static_address
        )
        
        return static_address
        
    def hold_object(self, asking_token, ghid):
        ''' Binds to an address, preventing its deletion. Note that this
        will publicly identify you as associated with the address and
        preventing its deletion to any connected persistence providers.
        '''
        # TODO: add some kind of proofing here? 
        binding = self._golcore.make_binding_stat(ghid)
        self._persister.ingest_gobs(binding)
        
    def delete_object(self, asking_token, ghid):
        ''' Debinds an object, attempting to delete it. This operation
        will succeed if the persistence provider accepts the deletion,
        but that doesn't necessarily mean the object was removed. A
        warning may be issued if the object was successfully debound, 
        but other bindings are preventing its removal.
        
        NOTE THAT THIS DELETES ALL COPIES OF THE OBJECT! It will become
        subsequently unavailable to other applications using it.
        '''
        # First we need to cache the object so we can call updates.
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            dispatch = self, 
            ghid = ghid
        )
        
        try:
            obj.halt_updates()
            # self._ignore_subs_because_updating.add(ghid)
            self._golcore.delete_ghid(ghid)
        except:
            # Why is it a syntax error to have else without except?
            raise
        else:
            self._oracle.forget(ghid)
            
            # If this is a private object, remove it from startup object record
            if obj.app_token != bytes(4):
                self._startup_by_token.discard(obj.app_token, obj.ghid)
                
        # This is now handled by obj.silence(), except because the object is
        # being deleted, we don't need to unsilence it.
        # finally:
        #     self._ignore_subs_because_updating.remove(ghid)
        
        # Todo: check to see if this actually results in deleting the object
        # upstream.
        
        # There's only a race condition here if the object wasn't actually 
        # removed upstream.
        self.distribute_to_endpoints(
            ghid, 
            skip_token = asking_token, 
            deleted = obj
        )
        
    def discard_object(self, asking_token, ghid):
        ''' Removes the object from *only the asking application*. The
        asking_token will no longer receive updates about the object.
        However, the object itself will persist, and remain available to
        other applications. This is the preferred way to remove objects.
        '''
        # This is sorta an accidental check that we're actually tracking the
        # object. Could make it explicit I suppose.
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            dispatch = self, 
            ghid = ghid
        )
            
        api_id = obj.api_id
        
        # Completely discard/deregister anything we don't care about anymore.
        interested_tokens = set()
        
        if obj.app_token == bytes(4):
            interested_tokens.update(self._api_ids[api_id])
        else:
            interested_tokens.add(obj.app_token)
            
        interested_tokens.update(self._requestors_by_ghid[ghid])
        interested_tokens.difference_update(self._discarders_by_ghid[ghid])
        interested_tokens.discard(asking_token)
        
        # Now perform actual updates
        if len(interested_tokens) == 0:
            # Delete? GC? Not clear what we should do here.
            # For now, just tell the oracle to ignore it.
            self._oracle.forget(ghid)
            
        else:
            self._requestors_by_ghid[ghid].discard(asking_token)
            self._discarders_by_ghid[ghid].add(asking_token)
        
    def share_object(self, asking_token, ghid, recipient):
        ''' Do the whole super thing, and then record which application
        initiated the request, and who the recipient was.
        '''
        obj = self._oracle.get_object(
            gaoclass = _Dispatchable,
            dispatch = self, 
            ghid = ghid
        )
            
        if obj.app_token != bytes(4):
            raise DispatchError('Cannot share a private object.')
        
        sharepair = _SharePair(ghid, recipient)
        
        try:
            self._outstanding_shares.add(sharepair, asking_token)
            
            # Currently, just perform a handshake. In the future, move this 
            # to a dedicated exchange system.
            request = self._golcore.hand_ghid(ghid, recipient)
            
        except:
            self._outstanding_shares.discard(sharepair, asking_token)
            raise
    
    def dispatch_share(self, target):
        ''' Receives the target *object* from a rolodex share, and 
        dispatches it as appropriate.
        '''        
        # Go ahead and distribute it to the appropriate endpoints.
        self.distribute_to_endpoints(target)
    
    def dispatch_share_ack(self, target, recipient):
        ''' Receives a share ack from the rolodex and passes it on to 
        the application that requested the share.
        '''
        sharepair = _SharePair(target, recipient)
        
        # TODO: make this work distributedly.
        requesting_tokens = self._outstanding_shares.pop_any(sharepair)
        
        # Now notify just the requesting app of the successful share. Note that
        # this will also handle any missing endpoints.
        for app_token in requesting_tokens:
            self._attempt_contact_endpoint(
                app_token, 
                'notify_share_success',
                target, 
                recipient = recipient
            )
    
    def dispatch_share_nak(self, target, recipient):
        ''' Receives a share nak from the rolodex and passes it on to 
        the application that requested the share.
        '''
        sharepair = _SharePair(target, recipient)
        
        # TODO: make this work distributedly.
        requesting_tokens = self._outstanding_shares.pop_any(sharepair)
        
        # Now notify just the requesting app of the successful share. Note that
        # this will also handle any missing endpoints.
        for app_token in requesting_tokens:
            self._attempt_contact_endpoint(
                app_token, 
                'notify_share_failure',
                target, 
                recipient = recipient
            )
                    
    def distribute_to_endpoints(self, ghid, skip_token=None, deleted=False):
        ''' Passes the object to all endpoints supporting its api via 
        command.
        
        If tokens to skip is defined, they will be skipped.
        tokens_to_skip isinstance iter(app_tokens)
        
        Should suppressing notifications for original creator be handled
        by the endpoint instead?
        '''
        # Create a temporary set
        callsheet = set()
        
        # If deleted, we passed the object itself.
        if deleted:
            obj = deleted
        else:
            # Not deleted? Grab the object.
            obj = self._oracle.get_object(
                gaoclass = _Dispatchable,
                dispatch = self, 
                ghid = ghid
            )
            
        # The app token is defined, so contact that endpoint (and only that 
        # endpoint) directly
        # Bypass if someone is sending us an app token we don't know about
        if (obj.app_token != bytes(4) and 
            obj.app_token in self._all_known_tokens):
                callsheet.add(obj.app_token)
            
        # It's not defined, so get everyone that uses that api_id
        else:
            callsheet.update(self._api_ids[obj.api_id])
            
        # Now add anyone explicitly tracking that object
        callsheet.update(self._requestors_by_ghid[ghid])
        
        # And finally, remove the skip token if present, as well as any apps
        # that have discarded the object
        callsheet.discard(skip_token)
        callsheet.difference_update(self._discarders_by_ghid[ghid])
            
        if len(callsheet) == 0:
            logger.warning('Agent lacks application to handle app id.')
            self._orphan_shares_incoming.add(ghid)
        else:
            for token in callsheet:
                if deleted:
                    # It's mildly dangerous to do this -- what if we throw an 
                    # error in _attempt_contact_endpoint?
                    self._attempt_contact_endpoint(
                        token, 
                        'send_delete', 
                        ghid
                    )
                else:
                    # It's mildly dangerous to do this -- what if we throw an 
                    # error in _attempt_contact_endpoint?
                    self._attempt_contact_endpoint(
                        token, 
                        'notify_object', 
                        ghid, 
                        state = obj.state
                )
    
    def register_endpoint(self, endpoint):
        ''' Registers an endpoint and all of its appdefs / APIs. If the
        endpoint has already been registered, updates it.
        
        Note: this cannot be used to create new app tokens! The token 
        must already be known to the dispatcher.
        '''
        self.endpoint_registered = endpoint
            
    def close_endpoint(self, endpoint):
        ''' Closes this client's connection to the endpoint, meaning it
        will not be able to handle any more requests. However, does not
        (and should not) clean tokens from the api_id dict.
        '''
        self.endpoint_registered = False
        
    def get_tokens(self, api_id):
        ''' Gets the local app tokens registered as capable of handling
        the passed API id.
        '''
        if api_id in self._api_ids:
            return frozenset(self._api_ids[api_id])
        else:
            raise KeyError(
                'Dispatcher does not have a token for passed api_id.'
            )
        
    def new_token(self):
        return self.app_token


# ###############################################
# Testing
# ###############################################
        
        
class WebsocketsIPCTrashTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._notifier_1 = threading.Event()
        self._notifier_2 = threading.Event()
        self._timeout = 1
        
    def _objhandler_1(self, obj):
        # Quick and dirty.
        self._notifier_11.set()
        
    def notification_checker_1(self):
        result = self._notifier_1.wait(self._timeout)
        self._notifier_1.clear()
        return result
        
    def _objhandler_2(self, obj):
        # Quick and dirty.
        self._notifier_2.set()
        
    def notification_checker_2(self):
        result = self._notifier_2.wait(self._timeout)
        self._notifier_2.clear()
        return result
    
    @classmethod
    def setUpClass(cls):
        cls.persister = MemoryPersister()
        cls.aengel = Aengel()
        
        cls.alice_core = MockCore(
            persister = cls.persister, 
            identity = TEST_AGENT1
        )
        cls.alice = Autocomms(
            autoresponder_class = IPCHost,
            autoresponder_kwargs = {'dispatch': cls.alice_core._dispatch},
            connector_class = WSBasicServer,
            connector_kwargs = {
                'host': 'localhost',
                'port': 4628,
            },
            debug = True,
            aengel = cls.aengel,
        )
        
        cls.bob_core = MockCore(
            persister = cls.persister,
            identity = TEST_AGENT2
        )
        cls.bob = Autocomms(
            autoresponder_class = IPCHost,
            autoresponder_kwargs = {'dispatch': cls.bob_core._dispatch},
            connector_class = WSBasicServer,
            connector_kwargs = {
                'host': 'localhost',
                'port': 4629,
            },
            debug = True,
            aengel = cls.aengel,
        )
        
        cls.app1 = Autocomms(
            autoresponder_class = IPCEmbed,
            connector_class = WSBasicClient,
            connector_kwargs = {
                'host': 'localhost',
                'port': 4628,
            },
            debug = True,
            aengel = cls.aengel,
        )
        cls.app1endpoint = cls.alice.any_session
        
        cls.app2 = Autocomms(
            autoresponder_class = IPCEmbed,
            connector_class = WSBasicClient,
            connector_kwargs = {
                'host': 'localhost',
                'port': 4628,
            },
            debug = True,
            aengel = cls.aengel,
        )
        endpoints = set(cls.alice.sessions)
        cls.app2endpoint = list(endpoints - {cls.app1endpoint})[0]
        
        cls.__api_id = bytes(64) + b'1'
        
        # Should these be moved into a dedicated test? Probably.
        cls.app1.new_token_threadsafe()
        cls.app2.new_token_threadsafe()
        
    def test_client1(self):
        pt0 = b'I am a sexy stagnant beast.'
        pt1 = b'Hello, world?'
        pt2 = b'Hiyaback!'
        pt3 = b'Listening...'
        pt4 = b'All ears!'
        
        # time.sleep(1)
        # Make sure we have an app token.
        self.assertTrue(isinstance(self.app1.app_token, bytes))
        
        # Test whoami
        whoami = self.app1.whoami_threadsafe()
        self.assertTrue(isinstance(whoami, Ghid))
        
        # Test registering an api_id
        self.app1.register_api_threadsafe(self.__api_id, self._objhandler_1)
        self.app2.register_api_threadsafe(self.__api_id, self._objhandler_2)
        self.assertIn(self.__api_id, self.app1endpoint.apis)
        
        obj1 = self.app1.new_obj_threadsafe(
            state = pt0,
            api_id = self.__api_id,
            dynamic = False
        )
        self.assertIn(obj1.address, self.persister.librarian)
        self.assertTrue(self.notification_checker_2())
        
        obj2 = self.app1.new_obj_threadsafe(
            state = pt1,
            api_id = self.__api_id,
            dynamic = True
        )
        self.assertIn(obj2.address, self.persister.librarian)
        self.assertTrue(self.notification_checker_2())
        
        joint1 = self.app2.get_obj_threadsafe(obj1.address)
        self.assertEqual(obj1, joint1)
        
        joint2 = self.app2.get_obj_threadsafe(obj2.address)
        self.assertEqual(obj2, joint2)
        
        obj3 = self.app1.new_obj_threadsafe(
            state = pt1,
            api_id = self.__api_id,
            dynamic = True
        )
        self.assertIn(obj3.address, self.persister.librarian)
        self.assertTrue(self.notification_checker_2())
        
        self.app1.update_obj_threadsafe(obj3, pt2)
        joint3 = self.app2.get_obj_threadsafe(obj3.address)
        self.assertEqual(obj3, joint3)
        
        # Note that this is calling bob's DISPATCH whoami, NOT an app whoami.
        self.app1.share_obj_threadsafe(obj3, self.bob_core.whoami)
        self.assertIn(obj3.address, self.bob_core._dispatch._orphan_shares_incoming)
        
        frozen3 = self.app1.freeze_obj_threadsafe(obj3)
        self.assertEqual(frozen3.state, obj3.state)
        
        self.app2.hold_obj_threadsafe(joint3)
        self.assertIn(obj3.address, self.alice_core._holdings)
        
        self.app2.discard_obj_threadsafe(joint3)
        self.assertIn(self.app2endpoint.app_token, self.alice_core._dispatch._discarders_by_ghid[obj3.address])
        self.assertTrue(joint3._inoperable)
        
        self.app1.delete_obj_threadsafe(obj1)
        self.assertNotIn(obj1.address, self.persister.librarian)
        self.assertTrue(obj1._inoperable)
        
        # --------------------------------------------------------------------
        # Comment this out if no interactivity desired
            
        # # Start an interactive IPython interpreter with local namespace, but
        # # suppress all IPython-related warnings.
        # with warnings.catch_warnings():
        #     warnings.simplefilter('ignore')
        #     IPython.embed()
        
    @classmethod
    def tearDownClass(cls):
        cls.aengel.stop()

if __name__ == "__main__":
    from _fixtures import logutils
    logutils.autoconfig()
    
    unittest.main()