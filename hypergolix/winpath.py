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

# The following has been moderately modified from this StackOverflow answer:
# https://stackoverflow.com/questions/7914505/modify-path-environment-
# variable-globally-and-permanently-using-python

import winreg
import ctypes
import sys
import os
# from ctypes import WINFUNCTYPE, POINTER, windll, addressof, c_wchar_p
from ctypes.wintypes import LONG, HWND, UINT, WPARAM, LPARAM, DWORD


class Registry:
    def __init__(self, key_location, key_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key_location = key_location
        self.key_path = key_path
        self.reg_key = None
            
    def __enter__(self):
        ''' Open the key location and path.
        '''
        self.reg_key = winreg.OpenKey(self.key_location, self.key_path, 0,
                                      winreg.KEY_ALL_ACCESS)
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        ''' Close the key location and path.
        '''
        winreg.CloseKey(self.reg_key)

    def set_key(self, name, value):
        if self.reg_key is not None:
            try:
                reg_value, reg_type = winreg.QueryValueEx(self.reg_key, name)
                
            except OSError:
                # If the value does not exists yet, we (guess) use a string as
                # the reg_type
                reg_type = winreg.REG_SZ
            
            winreg.SetValueEx(self.reg_key, name, 0, reg_type, value)
        
        else:
            raise RuntimeError('Must first enter key context.')
            
    def get_key(self, name):
        if self.reg_key is not None:
            reg_value, reg_type = winreg.QueryValueEx(self.reg_key, name)
            return reg_value
        
        else:
            raise RuntimeError('Must first enter key context.')

    def del_key(self, name):
        if self.reg_key is not None:
            try:
                winreg.DeleteValue(self.reg_key, name)
            except OSError:
                # Ignores if the key value doesn't exists
                pass
        else:
            raise RuntimeError('Must first enter key context.')
            
    def flush(self):
        if self.reg_key is not None:
            winreg.FlushKey(self.reg_key)
            
        else:
            raise RuntimeError('Must first enter key context.')


class EnvironmentVariable:
    ''' Configure an environment variable.
    '''

    def __init__(self, registry, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._name = name
        self._registry = registry
        
    @property
    def value(self):
        ''' Return the current value of the key.
        '''
        return self._registry.get_key(self._name)
        
    @value.setter
    def value(self, value):
        ''' Update and refresh the value of the key.
        '''
        self._registry.set_key(self._name, value)
        self._registry.flush()
        self._broadcast()
        
    @value.deleter
    def value(self):
        ''' Delete and refresh the key.
        '''
        self._registry.del_key(self._name)
        self._registry.flush()
        self._broadcast()

    def _broadcast(self):
        HWND_BROADCAST = 0xFFFF
        WM_SETTINGCHANGE = 0x001A
        SMTO_ABORTIFHUNG = 0x0002
        # result = ctypes.c_long()
        
        SendMessageTimeout = ctypes.windll.user32.SendMessageTimeoutW
        SendMessageTimeout.argtypes = (
            HWND,
            UINT,
            WPARAM,
            ctypes.c_wchar_p,
            UINT,
            UINT,
            UINT
        )
        SendMessageTimeout.restype = LPARAM

        # SendMessageTimeoutProto = WINFUNCTYPE(
        #     POINTER(LONG),
        #     HWND,
        #     UINT,
        #     WPARAM,
        #     LPARAM,
        #     UINT,
        #     UINT,
        #     POINTER(POINTER(DWORD))
        # )
        # SendMessageTimeout = SendMessageTimeoutProto(
        #     ("SendMessageTimeoutW", windll.user32)
        # )
        if not SendMessageTimeout(HWND_BROADCAST, WM_SETTINGCHANGE, 0,
                                  'Environment', SMTO_ABORTIFHUNG, 5000, 0):
            err = ctypes.get_last_error()
            raise ctypes.WinError(err)


if __name__ == "__main__":
    if sys.argv[1:]:
        path_append = ' '.join(sys.argv[1:])
    
        sys_env = Registry(
            winreg.HKEY_LOCAL_MACHINE,
            'SYSTEM\CurrentControlSet\Control\Session Manager\Environment'
        )
        
        with sys_env:
            path = EnvironmentVariable(sys_env, 'PATH')
            old_path = path.value
            new_path = old_path + os.pathsep + path_append
            path.value = new_path
            print('Successfully appended ' + path_append + ' to %PATH%.')
