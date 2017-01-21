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

from hypergolix import Ghid

from hypergolix.config import Config

from hypergolix.config import Remote
from hypergolix.config import User
from hypergolix.config import Instrumentation
from hypergolix.config import Process

from hypergolix.config import _ensure_dir_exists

from hypergolix.cli import main as ingest_args
from hypergolix.config import handle_args

from hypergolix.exceptions import ConfigError


# ###############################################
# Fixtures and vectors
# ###############################################


from _fixtures.ghidutils import make_random_ghid


vec_remotedef_depr = '''
    {
        "__RemoteDef__": true,
        "host": "foo",
        "port": 1234,
        "tls": false
    }
'''


vec_remotedef = '''
    - host: foo
      port: 1234
      tls: false
'''


vec_userdef_depr = '''
    {
        "__UserDef__": true,
        "fingerprint": "foo",
        "user_id": "bar",
        "root_secret": null
    }
'''


vec_userdef = '''
    user:
      fingerprint: foo
      user_id: bar
      root_secret: null
'''

vec_process = '''
    process:
      ghidcache: null
      logdir: null
      pid_file: null
      ipc_port: 7772
'''


vec_instrumentationdef_depr = '''
    {
        "__InstrumentationDef__": true,
        "verbosity": "info",
        "debug": false,
        "traceur": false
    }
'''


vec_instrumentationdef = '''
    instrumentation:
      verbosity: info
      debug: false
      traceur: false
'''


vec_cfg_depr = '''
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
        "fingerprint": "AX8w7tJGI2mTURKYc4x89eCC2WVXnrFZg-wu9Ec3lItTa5QywkEm6yeXO7p_lvsL8p5nx4UOCFTSC8iE9RbLRX4=",
        "user_id": "AbyzNTc81z5RhFRsA1JJ_Yok5_a8QFCZah6fyaqq6-hH5ylbjIaWU6qXjnOURsq8A4tTB6d9JxLQwxOn8iOZkyQ=",
        "root_secret": null
    },
    "instrumentation": {
        "__InstrumentationDef__": true,
        "verbosity": "info",
        "debug": false,
        "traceur": false
    },
    "process": {
        "ipc_port": 7772,
        "__ProcessDef__": true
    }
}
'''


vec_cfg = '''process:
  ghidcache: null
  logdir: null
  pid_file: null
  ipc_port: 7772
instrumentation:
  verbosity: info
  debug: false
  traceur: false
user:
  fingerprint: AX8w7tJGI2mTURKYc4x89eCC2WVXnrFZg-wu9Ec3lItTa5QywkEm6yeXO7p_lvsL8p5nx4UOCFTSC8iE9RbLRX4=
  user_id: AbyzNTc81z5RhFRsA1JJ_Yok5_a8QFCZah6fyaqq6-hH5ylbjIaWU6qXjnOURsq8A4tTB6d9JxLQwxOn8iOZkyQ=
  root_secret: null
remotes:
- host: foo
  port: 1234
  tls: false
'''


obj_remotedef = Remote('foo', 1234, False)
obj_userdef = User(
    Ghid.from_str('AX8w7tJGI2mTURKYc4x89eCC2WVXnrFZg-wu9Ec3lItTa5QywkEm6yeXO' +
                  '7p_lvsL8p5nx4UOCFTSC8iE9RbLRX4='),
    Ghid.from_str('AbyzNTc81z5RhFRsA1JJ_Yok5_a8QFCZah6fyaqq6-hH5ylbjIaWU6qXj' +
                  'nOURsq8A4tTB6d9JxLQwxOn8iOZkyQ='), None)
obj_instrumentationdef = Instrumentation('info', False, False)
obj_process = Process(None, None, None, 7772)
obj_cfg = Config(
    # This won't get used anyways
    path = pathlib.Path(),
    user = obj_userdef,
    instrumentation = obj_instrumentationdef,
    process = obj_process
)
obj_cfg.remotes.append(obj_remotedef)


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


class _SuppressSTDOUT:
    def __enter__(self):
        self._fd = sys.stdout.fileno()
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
    ''' Test round-trip config serialization.
    '''
    
    def test_encode(self):
        ''' Test encoding both as round-trip and against a test vector.
        '''
        preencoded = vec_cfg
        encoded = obj_cfg.encode()
        
        freshconfig_1 = Config(pathlib.Path())
        freshconfig_2 = Config(pathlib.Path())
        
        freshconfig_1.decode(preencoded)
        freshconfig_2.decode(encoded)
        
        self.assertEqual(freshconfig_1, freshconfig_2)
        self.assertEqual(freshconfig_2, obj_cfg)
        
    def test_decode(self):
        ''' Test decoding both as round-trip and against a test vector.
        '''
        predecoded = obj_cfg
        decoded = Config(pathlib.Path())
        decoded.decode(vec_cfg)
        
        freshdump_1 = predecoded.encode()
        freshdump_2 = decoded.encode()
        
        self.assertEqual(freshdump_1, freshdump_2)
        self.assertEqual(freshdump_2, vec_cfg)
                
    def test_upgrade(self):
        ''' Test loading old configs is equivalent to loading new ones.
        '''
        new = Config(pathlib.Path())
        old = Config(pathlib.Path())
        
        new.decode(vec_cfg)
        old.decode(vec_cfg_depr)
        
        self.assertEqual(new, old)
                
                
class ConfigTest(unittest.TestCase):
    ''' Test most of the ancillary internal functions.
    '''
    
    def test_mk_blank(self):
        config = Config(pathlib.Path())
        self.assertTrue(hasattr(config, 'remotes'))
        self.assertTrue(hasattr(config, 'user'))
        self.assertTrue(hasattr(config, 'instrumentation'))
        self.assertTrue(hasattr(config, 'process'))
        
    def test_find_cfg_from_env(self):
        with tempfile.TemporaryDirectory() as root:
            os.environ['HYPERGOLIX_HOME'] = root
            
            try:
                fake_config = pathlib.Path(root) / 'hypergolix.yml'
                # Create a fake file to pick up its existence
                fake_config.touch()
                config = Config.find()
                self.assertEqual(config.path, fake_config)
            finally:
                del os.environ['HYPERGOLIX_HOME']
    
    @unittest.skip('Appdata superceded by local hgx on dev machines.')
    def test_find_cfg_from_appdata(self):
        with tempfile.TemporaryDirectory() as root:
            try:
                oldappdata = os.environ['LOCALAPPDATA']
            except KeyError:
                oldappdata = None
                
            os.environ['LOCALAPPDATA'] = root
            
            try:
                fake_config = pathlib.Path(root) / 'hypergolix.yml'
                # Create a fake file to pick up its existence
                fake_config.touch()
                config = Config.find()
                self.assertEqual(config.path, fake_config)
            
            finally:
                if oldappdata is None:
                    del os.environ['LOCALAPPDATA']
                else:
                    os.environ['LOCALAPPDATA'] = oldappdata
        
    def test_ensure_dir(self):
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            
            test1 = root / 't1'
            test2 = root / 't2'
            innocent = test2 / 'innocent.txt'
            nest1 = root / 't3' / 'n1'
            nest2 = root / 't3' / 'n2'
            
            self.assertFalse(test1.exists())
            _ensure_dir_exists(test1)
            self.assertTrue(test1.exists())
            
            test2.mkdir()
            with innocent.open('w') as f:
                f.write('hello world')
            self.assertTrue(test2.exists())
            self.assertTrue(innocent.exists())
            _ensure_dir_exists(test2)
            self.assertTrue(test2.exists())
            self.assertTrue(innocent.exists())
            
            self.assertFalse(nest1.exists())
            self.assertFalse(nest2.exists())
            _ensure_dir_exists(nest1)
            self.assertTrue(nest1.exists())
            self.assertFalse(nest2.exists())
            _ensure_dir_exists(nest2)
            self.assertTrue(nest1.exists())
            self.assertTrue(nest2.exists())
        
    def test_manipulate_remotes(self):
        config = Config(pathlib.Path())
        rem1 = Remote('host1', 123, False)
        rem2 = Remote('host2', 123, False)
        rem2a = Remote('host2', 123, True)
        rem3 = Remote('host3', 123, True)
            
        self.assertIsNone(config.index_remote(rem1))
        self.assertIsNone(config.index_remote(rem2))
        self.assertIsNone(config.index_remote(rem2a))
        self.assertIsNone(config.index_remote(rem3))
        
        config.set_remote(rem1.host, rem1.port, rem1.tls)
        self.assertEqual(config.index_remote(rem1), 0)
        config.set_remote(rem2.host, rem2.port, rem2.tls)
        self.assertEqual(config.index_remote(rem1), 0)
        self.assertEqual(config.index_remote(rem2), 1)
        self.assertEqual(config.index_remote(rem2a), 1)
        config.set_remote(rem3.host, rem3.port, rem3.tls)
        self.assertEqual(config.index_remote(rem1), 0)
        self.assertEqual(config.index_remote(rem2), 1)
        self.assertEqual(config.index_remote(rem3), 2)
        config.set_remote(rem2a.host, rem2a.port, rem2a.tls)
        self.assertEqual(config.index_remote(rem1), 0)
        self.assertEqual(config.index_remote(rem2), 1)
        self.assertEqual(config.index_remote(rem3), 2)
        config.remove_remote(rem2.host, rem2.port)
        self.assertEqual(config.index_remote(rem1), 0)
        self.assertIsNone(config.index_remote(rem2))
        self.assertEqual(config.index_remote(rem3), 1)
    
    def test_context(self):
        ''' Ensure the context manager results in an update when changes
        are made, works with existing configs, etc.
        '''
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            path = pathlib.Path(root / 'hypergolix.yml')
            
            config = Config(path)
            other_cfg = Config(path)
            self.assertEqual(config, other_cfg)
        
            with Config(path) as config:
                config.instrumentation.debug = True
                self.assertNotEqual(config, other_cfg)
                
            other_cfg.reload()
            self.assertEqual(config, other_cfg)
                
            with config:
                config.instrumentation.debug = False
                self.assertNotEqual(config, other_cfg)
                
            other_cfg.reload()
            self.assertEqual(config, other_cfg)
                
                
class CommandingTest(unittest.TestCase):
    ''' Test passing commands and manipulation thereof.
    
    All of this should be deprecated!
    '''
    
    def test_full(self):
        ''' Test a full command chain for everything.
        '''
        with tempfile.TemporaryDirectory() as root:
            root = pathlib.Path(root)
            
            blank = Config(root / 'hypergolix.yml')
            blank.coerce_defaults()
            
            debug = Config(root / 'hypergolix.yml')
            debug.coerce_defaults()
            debug.instrumentation.debug = True
            
            nodebug = Config(root / 'hypergolix.yml')
            nodebug.coerce_defaults()
            nodebug.instrumentation.debug = False
            
            loud = Config(root / 'hypergolix.yml')
            loud.coerce_defaults()
            loud.instrumentation.verbosity = 'info'
            loud.instrumentation.debug = False
            
            normal = Config(root / 'hypergolix.yml')
            normal.coerce_defaults()
            normal.instrumentation.verbosity = 'warning'
            normal.instrumentation.debug = False
            
            host1 = Config(root / 'hypergolix.yml')
            host1.coerce_defaults()
            host1.remotes.append(Remote('host1', 123, True))
            host1.instrumentation.verbosity = 'warning'
            host1.instrumentation.debug = False
            
            host1hgx = Config(root / 'hypergolix.yml')
            host1hgx.coerce_defaults()
            host1hgx.remotes.append(Remote('host1', 123, True))
            host1hgx.remotes.append(Remote('hgx.hypergolix.com', 443, True))
            host1hgx.instrumentation.verbosity = 'warning'
            host1hgx.instrumentation.debug = False
            
            host1host2f = Config(root / 'hypergolix.yml')
            host1host2f.coerce_defaults()
            host1host2f.remotes.append(Remote('host1', 123, True))
            host1host2f.remotes.append(Remote('host2', 123, False))
            host1host2f.instrumentation.verbosity = 'warning'
            host1host2f.instrumentation.debug = False
            
            host1host2 = Config(root / 'hypergolix.yml')
            host1host2.coerce_defaults()
            host1host2.remotes.append(Remote('host1', 123, True))
            host1host2.remotes.append(Remote('host2', 123, True))
            host1host2.instrumentation.verbosity = 'warning'
            host1host2.instrumentation.debug = False
            
            # Definitely want to control the order of execution for this.
            valid_commands = [
                ('config', blank),
                ('config --debug', debug),
                ('config --no-debug', nodebug),
                ('config --verbosity loud', loud),
                ('config --verbosity normal', normal),
                ('config -ah host1 123 t', host1),
                ('config --addhost host1 123 t', host1),
                ('config -a hgx', host1hgx),
                ('config --add hgx', host1hgx),
                ('config -r hgx', host1),
                ('config --remove hgx', host1),
                # Note switch of TLS flag
                ('config -ah host2 123 f', host1host2f),
                # Note return of TLS flag
                ('config --addhost host2 123 t', host1host2),
                ('config -rh host2 123', host1),
                ('config --removehost host2 123', host1),
                ('config -o local', normal),
                ('config --only local', normal),
                ('config -ah host1 123 t -ah host2 123 t', host1host2),
            ]
            
            failing_commands = [
                'config -zz top',
                'config --verbosity XXXTREEEEEEEME',
                'config --debug --no-debug',
                'config -o local -a hgx',
            ]
            
            for cmd_str, cmd_result in valid_commands:
                with self.subTest(cmd_str):
                    cfg_path = root / 'hypergolix.yml'
                    # NOTE THAT THESE TESTS ARE CUMULATIVE! We definitely DON'T
                    # want to start with a fresh config each time around, or
                    # the tests will fail!
                    
                    argv = cmd_str.split()
                    argv.append('--root')
                    argv.append(str(cfg_path))
                    
                    with _SuppressSTDOUT():
                        ingest_args(argv)
                    
                    config = Config.load(cfg_path)
                    # THE PROBLEM HERE IS NOT JUST COERCE DEFAULTS! config.py,
                    # in its handle_args section, is passing in default values
                    # that are interfering with everything else.
                    self.assertEqual(config, cmd_result)
                
        # Don't need the temp dir for this; un-context to escape permissions
        for cmd_str in failing_commands:
            with self.subTest(cmd_str):
                argv = cmd_str.split()
                argv.append('--root')
                argv.append(str(root))
                # Note that argparse will always push usage to stderr in a
                # suuuuuuper annoying way if we don't suppress it.
                with self.assertRaises(SystemExit), \
                    _SuppressSTDERR(), _SuppressSTDOUT():
                        ingest_args(argv)


if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    unittest.main()
