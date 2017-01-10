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

# from hypergolix.config import _RemoteDef
# from hypergolix.config import _UserDef
# from hypergolix.config import _InstrumentationDef
# from hypergolix.config import _CfgDecoder
# from hypergolix.config import _CfgEncoder

# from hypergolix.config import _make_blank_cfg
# from hypergolix.config import get_hgx_rootdir
# from hypergolix.config import _ensure_dir
# from hypergolix.config import _ensure_hgx_homedir
# from hypergolix.config import _ensure_hgx_populated
# from hypergolix.config import _get_hgx_config
# from hypergolix.config import _set_hgx_config
# from hypergolix.config import _set_remote
# from hypergolix.config import _pop_remote
# from hypergolix.config import _index_remote

# from hypergolix.config import Config

# from hypergolix.config import _ingest_args
# from hypergolix.config import _handle_args

# from hypergolix.exceptions import ConfigError


# ###############################################
# Fixtures and vectors
# ###############################################


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
        

if __name__ == "__main__":
    from hypergolix import logutils
    logutils.autoconfig()
    unittest.main()
