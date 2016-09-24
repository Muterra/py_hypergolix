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
import tempfile
import pathlib
import os
import sys

from hypergolix.config import _RemoteDef
from hypergolix.config import _UserDef
from hypergolix.config import _InstrumentationDef
from hypergolix.config import _CfgDecoder
from hypergolix.config import _CfgEncoder

from hypergolix.config import _make_blank_cfg
from hypergolix.config import _get_hgx_rootdir
from hypergolix.config import _ensure_dir
from hypergolix.config import _ensure_hgx_homedir
from hypergolix.config import _ensure_hgx_populated
from hypergolix.config import _get_hgx_config
from hypergolix.config import _set_hgx_config
from hypergolix.config import _set_remote
from hypergolix.config import _pop_remote
from hypergolix.config import _index_remote

from hypergolix.config import Config

from hypergolix.config import _ingest_args
from hypergolix.config import _handle_args

from hypergolix.exceptions import ConfigError


# ###############################################
# Fixtures and vectors
# ###############################################


from _fixtures.ghidutils import make_random_ghid


vec_remotedef = '''
    {
        "__RemoteDef__": true,
        "host": "foo",
        "port": 1234,
        "tls": false
    }
'''


vec_userdef = '''
    {
        "__UserDef__": true,
        "fingerprint": "foo",
        "user_id": "bar",
        "password": null
    }
'''


vec_instrumentationdef = '''
    {
        "__InstrumentationDef__": true,
        "verbosity": "info",
        "debug": false,
        "traceur": false
    }
'''


vec_cfg = '''
    {
        "remotes": [
            {
                "__RemoteDef__": true,
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


obj_remotedef = _RemoteDef('foo', 1234, False)
obj_userdef = _UserDef('foo', 'bar', None)
obj_instrumentationdef = _InstrumentationDef('info', False, False)
obj_cfg = {
    'remotes': [obj_remotedef],
    'user': obj_userdef,
    'instrumentation': obj_instrumentationdef
}


class _SuppressSTDERR:
    def __enter__(self):
        self._fd = sys.stderr.fileno()
        self._cache = os.dup(self._fd)
        self._devnull = os.open(os.devnull, os.O_RDWR)
        os.dup2(self._devnull, self._fd)
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        os.dup2(self._cache, self._fd)
        os.close(self._devnull)
        os.close(self._cache)


# ###############################################
# Testing
# ###############################################
        

class CfgSerialTest(unittest.TestCase):
    
    def setUp(self):
        self.encoder = _CfgEncoder()
        self.decoder = _CfgDecoder()
    
    def test_encode(self):
        objs = (
            obj_remotedef,
            obj_userdef,
            obj_instrumentationdef,
            obj_cfg
        )
        serials = (
            vec_remotedef,
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
            obj_remotedef,
            obj_userdef,
            obj_instrumentationdef,
            obj_cfg
        )
        serials = (
            vec_remotedef,
            vec_userdef,
            vec_instrumentationdef,
            vec_cfg
        )
        
        for obj, serial in zip(objs, serials):
            with self.subTest(obj):
                self.assertEqual(self.decoder.decode(serial), obj)
                
                
class EtcTest(unittest.TestCase):
    ''' Test most of the ancillary internal functions.
    '''
    
    def test_mk_blank(self):
        config = _make_blank_cfg()
        self.assertIn('remotes', config)
        self.assertIn('user', config)
        self.assertIn('instrumentation', config)
        
    def test_get_root(self):
        root = _get_hgx_rootdir()
        self.assertTrue(root.exists())
        
    def test_ensure_dir(self):
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            
            test1 = root / 't1'
            test2 = root / 't2'
            innocent = test2 / 'innocent.txt'
            nest1 = root / 't3' / 'n1'
            nest2 = root / 't3' / 'n2'
            
            self.assertFalse(test1.exists())
            _ensure_dir(test1)
            self.assertTrue(test1.exists())
            
            test2.mkdir()
            with innocent.open('w') as f:
                f.write('hello world')
            self.assertTrue(test2.exists())
            self.assertTrue(innocent.exists())
            _ensure_dir(test2)
            self.assertTrue(test2.exists())
            self.assertTrue(innocent.exists())
            
            self.assertFalse(nest1.exists())
            self.assertFalse(nest2.exists())
            _ensure_dir(nest1)
            self.assertTrue(nest1.exists())
            self.assertFalse(nest2.exists())
            _ensure_dir(nest2)
            self.assertTrue(nest1.exists())
            self.assertTrue(nest2.exists())
        
    def test_ensure_homedir(self):
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            
            home = root / '.hypergolix'
            logdir = home / 'logs'
            cachedir = home / 'ghidcache'
            testfile = cachedir / 'test.txt'
            self.assertFalse(home.exists())
            _ensure_hgx_homedir(root)
            self.assertTrue(home.exists())
            self.assertTrue(logdir.exists())
            self.assertTrue(cachedir.exists())
            
            with testfile.open('w') as f:
                f.write('Hello world')
            self.assertTrue(testfile.exists())
                
            _ensure_hgx_homedir(root)
            self.assertTrue(testfile.exists())
        
    def test_populate_home(self):
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            
            logdir = root / 'logs'
            cachedir = root / 'ghidcache'
            testfile1 = logdir / 'test.txt'
            testfile2 = cachedir / 'test.txt'
            self.assertFalse(logdir.exists())
            self.assertFalse(cachedir.exists())
            
            _ensure_hgx_populated(root)
            self.assertTrue(logdir.exists())
            self.assertTrue(cachedir.exists())
            
            with testfile1.open('w') as f:
                f.write('Hello world')
            self.assertTrue(testfile1.exists())
            
            with testfile2.open('w') as f:
                f.write('Hello world')
            self.assertTrue(testfile2.exists())
            
            _ensure_hgx_populated(root)
            self.assertTrue(logdir.exists())
            self.assertTrue(cachedir.exists())
            self.assertTrue(testfile1.exists())
            self.assertTrue(testfile2.exists())
        
    def test_cfg_getset(self):
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            
            with self.assertRaises(ConfigError):
                _get_hgx_config(root)
            
            blank = _make_blank_cfg()
            _ensure_hgx_homedir(root)
            _set_hgx_config(root, blank)
            re_blank = _get_hgx_config(root)
            
            self.assertEqual(blank, re_blank)
        
    def test_manipulate_remotes(self):
        cfg = _make_blank_cfg()
        rem1 = _RemoteDef('host1', 123, False)
        rem2 = _RemoteDef('host2', 123, False)
        rem2a = _RemoteDef('host2', 123, True)
        rem3 = _RemoteDef('host3', 123, True)
            
        self.assertIsNone(_index_remote(cfg, rem1))
        self.assertIsNone(_index_remote(cfg, rem2))
        self.assertIsNone(_index_remote(cfg, rem2a))
        self.assertIsNone(_index_remote(cfg, rem3))
        
        _set_remote(cfg, rem1)
        self.assertEqual(_index_remote(cfg, rem1), 0)
        _set_remote(cfg, rem2)
        self.assertEqual(_index_remote(cfg, rem1), 0)
        self.assertEqual(_index_remote(cfg, rem2), 1)
        self.assertEqual(_index_remote(cfg, rem2a), 1)
        _set_remote(cfg, rem3)
        self.assertEqual(_index_remote(cfg, rem1), 0)
        self.assertEqual(_index_remote(cfg, rem2), 1)
        self.assertEqual(_index_remote(cfg, rem3), 2)
        _set_remote(cfg, rem2a)
        self.assertEqual(_index_remote(cfg, rem1), 0)
        self.assertEqual(_index_remote(cfg, rem2), 1)
        self.assertEqual(_index_remote(cfg, rem3), 2)
        _pop_remote(cfg, rem2)
        self.assertEqual(_index_remote(cfg, rem1), 0)
        self.assertIsNone(_index_remote(cfg, rem2))
        self.assertEqual(_index_remote(cfg, rem3), 1)
        
        
class ConfigTest(unittest.TestCase):
    ''' Test the actual configuration context manager.
    '''
    
    def test_context(self):
        ''' Ensure the context manager results in an update when changes
        are made, works with existing configs, etc.
        '''
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            other_cfg = _make_blank_cfg()
            
            with Config(root) as config:
                self.assertEqual(config._cfg, other_cfg)
                config.debug_mode = True
                other_cfg['instrumentation'].debug = True
                self.assertEqual(config._cfg, other_cfg)
                
            with Config(root) as config:
                self.assertEqual(config._cfg, other_cfg)
                config.debug_mode = False
                other_cfg['instrumentation'].debug = False
                self.assertEqual(config._cfg, other_cfg)
                
            with Config(root) as config:
                self.assertEqual(config._cfg, other_cfg)
    
    def test_stuffs(self):
        ''' Tests attribute manipulation.
        '''
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            homedir = _ensure_hgx_homedir(root)
            
            with Config(root) as config:
                self.assertEqual(config.homedir, homedir)
                
                self.assertEqual(config.remotes, tuple())
                self.assertEqual(config.fingerprint, None)
                self.assertEqual(config.user_id, None)
                self.assertEqual(config.password, None)
                self.assertEqual(config.log_verbosity, 'warning')
                self.assertEqual(config.debug_mode, False)
                
                config.set_remote('host', 123, True)
                self.assertIn(('host', 123, True), config.remotes)
                
                config.remove_remote('host', 123)
                self.assertNotIn(('host', 123, True), config.remotes)
                
                # Test fingerprints and user_id
                fingerprint = make_random_ghid()
                user_id = make_random_ghid()
                
                config.fingerprint = fingerprint
                self.assertEqual(config.fingerprint, fingerprint)
                
                config.user_id = user_id
                self.assertEqual(config.user_id, user_id)
                
                # Test modification updates appropriately
                fingerprint = make_random_ghid()
                user_id = make_random_ghid()
                
                config.fingerprint = fingerprint
                self.assertEqual(config.fingerprint, fingerprint)
                
                config.user_id = user_id
                self.assertEqual(config.user_id, user_id)
                
                # Now everything else
                config.log_verbosity = 'info'
                self.assertEqual(config.log_verbosity, 'info')
                config.debug_mode = True
                self.assertEqual(config.debug_mode, True)
                
                
class CommandingTest(unittest.TestCase):
    ''' Test passing commands and manipulation thereof.
    '''
    
    def test_full(self):
        ''' Test a full command chain for everything.
        '''
        blank = _make_blank_cfg()
        debug = _make_blank_cfg()
        debug['instrumentation'].debug = True
        loud = _make_blank_cfg()
        loud['instrumentation'].verbosity = 'info'
        host1 = _make_blank_cfg()
        host1['remotes'].append(('host1', 123, True))
        host1hgxtest = _make_blank_cfg()
        host1hgxtest['remotes'].append(('host1', 123, True))
        host1hgxtest['remotes'].append(('hgxtest.hypergolix.com', 443, True))
        host1host2f = _make_blank_cfg()
        host1host2f['remotes'].append(('host1', 123, True))
        host1host2f['remotes'].append(('host2', 123, False))
        host1host2 = _make_blank_cfg()
        host1host2['remotes'].append(('host1', 123, True))
        host1host2['remotes'].append(('host2', 123, True))
        
        # Definitely want to control the order of execution for this.
        valid_commands = [
            ('--debug', debug),
            ('--no-debug', blank),
            ('--verbosity loud', loud),
            ('--verbosity normal', blank),
            ('-ah host1 123 t', host1),
            ('--addhost host1 123 t', host1),
            ('-a hgxtest', host1hgxtest),
            ('--add hgxtest', host1hgxtest),
            ('-r hgxtest', host1),
            ('--remove hgxtest', host1),
            # Note switch of TLS flag
            ('-ah host2 123 f', host1host2f),
            # Note return of TLS flag
            ('--addhost host2 123 t', host1host2),
            ('-rh host2 123', host1),
            ('--removehost host2 123', host1),
            ('-o local', blank),
            ('--only local', blank),
        ]
        
        failing_commands = [
            '-zz top',
            '--verbosity XXXTREEEEEEEME',
            '--debug --no-debug',
            '-o local -a hgxtest',
        ]
        
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            
            for cmd_str, cmd_result in valid_commands:
                with self.subTest(cmd_str):
                    argv = cmd_str.split()
                    args = _ingest_args(argv)
                    _handle_args(args, root)
                    
                    with Config(root) as config:
                        self.assertEqual(config._cfg, cmd_result)
                
        # Don't need the temp dir for this.
        for cmd_str in failing_commands:
            with self.subTest(cmd_str):
                argv = cmd_str.split()
                # Note that argparse will always push usage to stderr in a
                # suuuuuuper annoying way if we don't suppress it.
                with self.assertRaises(SystemExit), _SuppressSTDERR():
                    args = _ingest_args(argv)


if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    unittest.main()
