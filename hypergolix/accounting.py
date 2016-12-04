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
# import weakref
# import traceback
# import threading

# from golix import SecondParty
from golix import Ghid

# from golix.utils import AsymHandshake
# from golix.utils import AsymAck
# from golix.utils import AsymNak

# Local dependencies
# from .persistence import _GarqLite
# from .persistence import _GdxxLite


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


class Account:
    ''' Accounts settle all sorts of stuff.
    '''
    
    def __init__(self, root_secret, user_id):
        ''' Gets everything ready for account bootstrapping.
        '''
        self._root_secret = root_secret
        self._user_id = user_id
        
    async def validate_root_node(self, new_validator, check_against):
        ''' Checks proper derivation of the root node -- essentially, a
        password check.
        '''
        
    async def load_root_node(self, password_validator):
        ''' Turns the root_secret and user_id into a directory of GAO
        resources. Then, cleans up the root secret and user_id.
        '''
        try:
            root = GAO(
                ghid = self._user_id,
                dynamic = None,
                author = None,
                legroom = 7,
                golcore = self._golcore,
                privateer = self._privateer,
                percore = self._percore,
                librarian = self._librarian
            )
            await root._pull()
            
        finally:
            del self._root_secret
            del self._user_id
            
        # Should probably not hard-code the password validator length or summat
        password_indicator = root[0:32]
        identity_ghid = Ghid.from_bytes(root[32:97])
        identity_master = Secret.from_bytes(root[97:150])
        persistent_ghid = Ghid.from_bytes(root[150:215])
        persistent_master = Secret.from_bytes(root[215:268])
        quarantine_ghid = Ghid.from_bytes(root[268:333])
        quarantine_master = Secret.from_bytes(root[333:386])
        secondary_manifest = Ghid.from_bytes(root[386:451])
        secondary_master = Secret.from_bytes(root[451:504])
        
        await self.validate_root_node(
            new_validator = password_indicator,
            check_against = password_validator
        )
            
    async def make_root_node(self):
        ''' Used for account creation, to initialize the root node with
        its resource directory.
        '''
    
    async def bootstrap_primary(self):
        ''' Pull and initialize (or push, for the initial account
        creation) the objects making up the account that have master
        secrets.
        '''
        
    async def bootstrap_secondary(self):
        ''' Pull and initialize (or push, for the initial account
        creation) the objects making up the account that lack master
        secrets.
        '''
        
    async def _bootstrap_single(self, gao):
        ''' Perform a single GAO bootstrap.
        
        Note that this is basically just copying oracle.get_object and
        oracle.new_object, until such time as that functionality is
        moved somewhere more appropriate.
        '''
        # This is the initial account creation.
        if gao.ghid is None:
            await gao._push()
            await self._salmonator.register(gao)
            
        # This is subsequent account restoration.
        else:
            await self._salmonator.register(gao)
            await self._salmonator.attempt_pull(gao.ghid, quiet=True)
            await gao._pull()
            
        self.oracle._lookup[gao.ghid] = gao


class Accountant:
    ''' The accountant handles all sorts of stuff to make distributed
    access possible. For example, it should track device IDs associated
    with the account, for the purposes of making a distributed GAO lock,
    etc etc etc.
    '''
    pass
