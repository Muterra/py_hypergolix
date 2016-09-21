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

import unittest
import json

from hypergolix.config import _PServer
from hypergolix.config import _UserDef
from hypergolix.config import _InstrumentationDef
from hypergolix.config import _CfgDecoder
from hypergolix.config import _CfgEncoder


# ###############################################
# Fixtures and vectors
# ###############################################


vec_pserver = \
'''
{
    "__PServer__": true,
    "host": "foo",
    "port": 1234,
    "tls": false
}
'''


vec_userdef = \
'''
{
    "__UserDef__": true,
    "fingerprint": "foo",
    "user_id": "bar",
    "password": null
}
'''


vec_instrumentationdef = \
'''
{
    "__InstrumentationDef__": true,
    "verbosity": "info",
    "debug": false,
    "traceur": false
}
'''


vec_cfg = \
'''
{
    "servers": [
        {
            "__PServer__": true,
            "host": "foo",
            "port": 1234,
            "tls": false
        }
    ],
    "user": {
        "__UserDef__": true,
        "fingerprint": "foo",
        "user_id": "bar",
        "password": null
    },
    "instrumentation": {
        "__InstrumentationDef__": true,
        "verbosity": "info",
        "debug": false,
        "traceur": false
    }
}
'''


obj_pserver = _PServer('foo', 1234, False)
obj_userdef = _UserDef('foo', 'bar', None)
obj_instrumentationdef = _InstrumentationDef('info', False, False)
obj_cfg = {
    'servers': [obj_pserver],
    'user': obj_userdef,
    'instrumentation': obj_instrumentationdef
}


# ###############################################
# Testing
# ###############################################
        

class CfgSerialTest(unittest.TestCase):
    
    def setUp(self):
        self.encoder = _CfgEncoder()
        self.decoder = _CfgDecoder()
    
    def test_encode(self):
        objs = (
            obj_pserver,
            obj_userdef,
            obj_instrumentationdef,
            obj_cfg
        )
        serials = (
            vec_pserver,
            vec_userdef,
            vec_instrumentationdef,
            vec_cfg
        )
        
        for obj, serial in zip(objs, serials):
            with self.subTest(obj):
                encoded = self.encoder.encode(obj)
                # Use default json to bypass any issues in our custom decoding
                self.assertEqual(json.loads(encoded), json.loads(serial))
        
    def test_decode(self):
        objs = (
            obj_pserver,
            obj_userdef,
            obj_instrumentationdef,
            obj_cfg
        )
        serials = (
            vec_pserver,
            vec_userdef,
            vec_instrumentationdef,
            vec_cfg
        )
        
        for obj, serial in zip(objs, serials):
            with self.subTest(obj):
                self.assertEqual(self.decoder.decode(serial), obj)


if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    unittest.main()
