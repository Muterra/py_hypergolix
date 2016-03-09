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

# These are normal imports
from hypergolix.persisters import MemoryPersister

# These are abnormal imports
from golix import Guid
from golix import ThirdParty
from golix import SecondParty
from golix import FirstParty

# ###############################################
# Testing
# ###############################################
    
class TrashTest(unittest.TestCase):
    def setUp(self):
        self.server1 = MemoryPersister()
    
        self.agent1 = FirstParty()
        self.agent2 = FirstParty()
        self.agent3 = FirstParty()
    
        self.reader1 = self.agent1.second_party
        self.reader2 = self.agent2.second_party
        
    def test_trash(self):
        midc1 = self.reader1.packed
        midc2 = self.reader2.packed
        
        self.server1.publish(midc1)
        self.server1.publish(midc2)
            
        # Start an interactive IPython interpreter with local namespace, but
        # suppress all IPython-related warnings.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            IPython.embed()

if __name__ == "__main__":
    unittest.main()