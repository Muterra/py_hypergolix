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
# Embeds contains all of the application-side integrations. These will only
# be used by applications written in python.

# Control * imports.
__all__ = [
    # 'EmbeddedIntegration', 
    # 'LocalhostIntegration'
]

# External dependencies
import abc
import msgpack

# Inter-package dependencies
from .utils import AppDef


class _EmbedBase(metaclass=abc.ABCMeta):
    ''' Base class for an application link. Note that an integration cannot 
    exist without also being an agent. They are separated to allow 
    mixing-and-matching agent/persister/integration configurations.
    '''
    @abc.abstractmethod
    def register_application(self, appdef):
        ''' Registers an application with the integration. If appdef is
        None, will create an app_id and endpoint for the app.
        
        Returns an AppDef object.
        '''
        pass