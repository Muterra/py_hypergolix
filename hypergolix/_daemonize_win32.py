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

This was written with consultation of the following resources:
    Source code and docs from the pyWin32 project it depends on
        https://sourceforge.net/projects/pywin32/
    Alexander Baker: How to create a Windows service in Python
        http://code.activestate.com/recipes/576451/
    Louis Riviere: Win services helper
        http://code.activestate.com/recipes/551780/
    Ryan Robitalle: Running Python scripts as a Windows service
        http://ryrobes.com/python/
        running-python-scripts-as-a-windows-service/
    
'''

# Global dependencies
import logging
import traceback

# Intra-package dependencies
from .utils import platform_specificker
from .utils import _default_to

_SUPPORTED_PLATFORM = platform_specificker(
    linux_choice = False,
    win_choice = True,
    # Dunno if this is a good idea but might as well try
    cygwin_choice = True,
    osx_choice = False,
    other_choice = False
)

if _SUPPORTED_PLATFORM:
    import win32service
    import win32api
    import win32con
    import win32event
    import win32evtlogutil

    from win32api import servicemanager

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

# Service installation, etc
        
        
def _get_shortname(longname):
    ''' Gets the shortname for a service from the long name. May be 
    needed to figure out the service_name for install, uninstall, etc.
    '''
    return win32service.GetServiceKeyName(longname)
    
    
def _make_invocation(python_path=None, start_args=None):
    ''' Returns a properly-formatted command that can be run to start a
    service.
    
    python_path: path to python executable. Uses sys.executable if None.
    start_args: str. the command to pass to the python_path.
    '''
    # AKA exeName
    python_path = _default_to(python_path, sys.executable)
    
    # Now we need to figure out what actual command to use to start the service
    if start_args is None:
        return python_path
    else:
        return python_path + ' ' + start_args


def _install_service(service_name, display_name, start_type, invocation, 
                    service_desc=None, service_deps=None):
    ''' Installs a Windows service.
    
    service_name:   the shortname. Used for the registry and stuff.
    display_name:   the longname. Used for the name field in the Windows
                    service manager.
    start_type:     'manual', 'auto', or 'disabled'
    invocation:     the command to invoke to start the service.
    service_desc:   the service description. Used in the description 
                    field in the Windows service manager.
    service_deps:   service dependencies. Sequence of strs.
    '''
    
    # Arg prep ################################################################
    
    service_deps = _default_to(service_deps, [])
    service_desc = _default_to(service_desc, '')
    
    try:
        start_type = {
            'manual': win32service.SERVICE_DEMAND_START,
            'auto' : win32service.SERVICE_AUTO_START,
            'disabled': win32service.SERVICE_DISABLED
        }[start_type]
    except KeyError as exc:
        raise ValueError('Invalid start_type.') from exc
        
    # This tells the service manager to run in its own process
    service_type = win32service.SERVICE_WIN32_OWN_PROCESS
    # This modifies it to be able to be interactive
    interactable = False
    if interactable:
        service_type = service_type | win32service.SERVICE_INTERACTIVE_PROCESS
    # This has (something???) to do with startup (boot???) error handling.
    error_control = win32service.SERVICE_ERROR_NORMAL
        
    # Actually doing stuff ####################################################
    
    hscm = win32service.OpenSCManager(
        machineName = None, 
        dbName = None,
        desiredAccess = win32service.SC_MANAGER_ALL_ACCESS
    )
    
    try:
        handle = win32service.CreateService(
            scHandle = hscm,
            name = service_name,
            displayName = display_name,
            desiredAccess = win32service.SERVICE_ALL_ACCESS,
            serviceType = service_type,
            startType = start_type,
            errorControl = error_control,
            binaryFile = invocation,
            loadOrderGroup = None,
            bFetchTag = 0,
            serviceDeps = service_deps,
            acctName = None, # Username for service to run under
            password = None # Password for above account
        )
        
        try:
            # NT cannot do this. Do it first, since NT is older than Vista
            win32service.ChangeServiceConfig2(
                hService = handle, 
                InfoLevel = win32service.SERVICE_CONFIG_DESCRIPTION, 
                info = service_desc
            )
            # Note that delayed start is only available on Vista and later
            # But we're not currently supporting delayed start anyways so just
            # ignore it.
            # win32service.ChangeServiceConfig2(
            #     hService = handle,
            #     InfoLevel = \
            #         win32service.SERVICE_CONFIG_DELAYED_AUTO_START_INFO,
            #     info = delayed_start
            # )
            
        except (win32service.error, NotImplementedError):
            logger.warning(
                'Failed to fully configure system with traceback: \n' +
                ''.join(traceback.format_exc())
            )
        
        # And don't forget to close our service handle.
        win32service.CloseServiceHandle(handle)
        
    # And ensure we close the service manager.
    finally:
        win32service.CloseServiceHandle(hscm)
        
    # As best I can tell, this is just a utility to record what class/module
    # created the service?
    # InstallPythonClassString(pythonClassString, serviceName)
        
        
def uninstall_service(service_name):
    ''' Removes an installed service.
    '''
    logger.info('Removing service "' + service_name + '"')
    
    try:
        hscm = win32service.OpenSCManager(
            machineName = None, 
            dbName = None,
            desiredAccess = win32service.SC_MANAGER_ALL_ACCESS
        )
        
        try:
            handle = win32service.OpenService(
                scHandle = hscm, 
                name = service_name, 
                desiredAccess = win32service.SERVICE_ALL_ACCESS
            )
            win32service.DeleteService(handle)
            win32service.CloseServiceHandle(handle)
            
        finally:
            win32service.CloseServiceHandle(hscm)
            
    except win32service.error as exc:
        logger.error(
            'Error removing service: ' + str(exc.strerror) + '(' +
            str(exc.winerror)
            ') w/ traceback: \n' + 
            ''.join(traceback.format_exc())
        )
        raise
    
    
def install_service(service_name, *args, **kwargs):
    ''' Pass-through to install with some wrapping for error handling.
    '''    
    try:
        _install_service(service_name, *args, **kwargs)
    
    except:
        # Log the installation problem and then roll back the installation.
        logger.error(
            'Failed to install service w/ traceback: \n' + 
            ''.join(traceback.format_exc())
        )
        try:
            uninstall_service(service_name)
        except:
            logger.error(
                'Failed to remove unsuccessful partial installation. \n' + 
                ''.join(traceback.format_exc())
            )
            
        # Re-raise the outer error.
        raise
        
        
def _add_registry_option(service_name, option, value):
    ''' Sets a registry key for the service. Yuk. Just, gross.
    
    Value is enforced to be string (or subclass) only.
    '''
    if not isinstance(value, str):
        raise TypeError('Registry options must be strings.')
    
    key = win32api.RegCreateKey(
        win32con.HKEY_LOCAL_MACHINE, 
        'System\\CurrentControlSet\\Services\\' + service_name + '\\Parameters'
    )
    try:
        win32api.RegSetValueEx(
            key, 
            option, 
            0, 
            win32con.REG_SZ, 
            value
        )
    
    finally:
        win32api.RegCloseKey(key)
    

def _get_registry_option(service_name, option):
    ''' Gets a registry key from the service. Yuk. Just, gross.
    
    Raises KeyError if the option is missing.
    '''
    key = win32api.RegCreateKey(
        win32con.HKEY_LOCAL_MACHINE, 
        'System\\CurrentControlSet\\Services\\' + service_name + '\\Parameters'
    )
    try:
        try:
            return win32api.RegQueryValueEx(key, option)[0]
            
        except Exception as exc:
            raise KeyError(option) from exc
            
    finally:
        win32api.RegCloseKey(key)
        
        
# Service Starting, stopping, restart


def start_service(service_name, machine=None, *args, **kwargs):
    ''' Starts the service.
    
    args and kwargs must be strings. 
    kwargs will be trivially transformed to --key value pairs.
    '''
    

#
# Useful base class to build services from.
#
class ServiceFramework:
    # Required Attributes:
    # _svc_name_ = The service name
    # _svc_display_name_ = The service display name

    # Optional Attributes:
    _svc_deps_ = None        # sequence of service names on which this depends
    _exe_name_ = None        # Default to PythonService.exe
    _exe_args_ = None        # Default to no arguments
    _svc_description_ = None # Only exists on Windows 2000 or later, ignored on windows NT

    def __init__(self, args):
        import servicemanager
        self.ssh = servicemanager.RegisterServiceCtrlHandler(args[0], self.ServiceCtrlHandlerEx, True)
        servicemanager.SetEventSourceName(self._svc_name_)
        self.checkPoint = 0

    def GetAcceptedControls(self):
        # Setup the service controls we accept based on our attributes. Note
        # that if you need to handle controls via SvcOther[Ex](), you must
        # override this.
        accepted = 0
        if hasattr(self, "SvcStop"): accepted = accepted | win32service.SERVICE_ACCEPT_STOP
        if hasattr(self, "SvcPause") and hasattr(self, "SvcContinue"):
            accepted = accepted | win32service.SERVICE_ACCEPT_PAUSE_CONTINUE
        if hasattr(self, "SvcShutdown"): accepted = accepted | win32service.SERVICE_ACCEPT_SHUTDOWN
        return accepted

    def ReportServiceStatus(self, serviceStatus, waitHint = 5000, win32ExitCode = 0, svcExitCode = 0):
        if self.ssh is None: # Debugging!
            return
        if serviceStatus == win32service.SERVICE_START_PENDING:
            accepted = 0
        else:
            accepted = self.GetAcceptedControls()

        if serviceStatus in [win32service.SERVICE_RUNNING,  win32service.SERVICE_STOPPED]:
            checkPoint = 0
        else:
            self.checkPoint = self.checkPoint + 1
            checkPoint = self.checkPoint

        # Now report the status to the control manager
        status = (win32service.SERVICE_WIN32_OWN_PROCESS,
                 serviceStatus,
                 accepted, # dwControlsAccepted,
                 win32ExitCode, # dwWin32ExitCode;
                 svcExitCode, # dwServiceSpecificExitCode;
                 checkPoint, # dwCheckPoint;
                 waitHint)
        win32service.SetServiceStatus( self.ssh, status)

    def SvcInterrogate(self):
        # Assume we are running, and everyone is happy.
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)

    def SvcOther(self, control):
        try:
            print("Unknown control status - %d" % control)
        except IOError:
            # services may not have a valid stdout!
            pass

    def ServiceCtrlHandler(self, control):
        return self.ServiceCtrlHandlerEx(control, 0, None)

    # The 'Ex' functions, which take additional params
    def SvcOtherEx(self, control, event_type, data):
        # The default here is to call self.SvcOther as that is the old behaviour.
        # If you want to take advantage of the extra data, override this method
        return self.SvcOther(control)

    def ServiceCtrlHandlerEx(self, control, event_type, data):
        if control==win32service.SERVICE_CONTROL_STOP:
            return self.SvcStop()
        elif control==win32service.SERVICE_CONTROL_PAUSE:
            return self.SvcPause()
        elif control==win32service.SERVICE_CONTROL_CONTINUE:
            return self.SvcContinue()
        elif control==win32service.SERVICE_CONTROL_INTERROGATE:
            return self.SvcInterrogate()
        elif control==win32service.SERVICE_CONTROL_SHUTDOWN:
            return self.SvcShutdown()
        else:
            return self.SvcOtherEx(control, event_type, data)

    def SvcRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self.SvcDoRun()
        # Once SvcDoRun terminates, the service has stopped.
        # We tell the SCM the service is still stopping - the C framework
        # will automatically tell the SCM it has stopped when this returns.
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
