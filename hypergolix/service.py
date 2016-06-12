'''
Start a hypergolix service.

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

# Control * imports. Therefore controls what is available to toplevel
# package through __init__.py
__all__ = [
    'HypergolixService', 
    'HypergolixApplication'
]

import unittest
import warnings
import collections
import threading
import time
import argparse

from golix import Ghid

from hypergolix.core import AgentBase
from hypergolix.core import Dispatcher
from hypergolix.persisters import WSPersister
from hypergolix.ipc_hosts import WebsocketsIPC
from hypergolix.embeds import WebsocketsEmbed

# """Stack tracer for multi-threaded applications.


# Usage:

# import stacktracer
# stacktracer.start_trace("trace.html",interval=5,auto=True) # Set auto flag to always update file!
# ....
# stacktracer.stop_trace()
# """



# import sys
# import traceback
# from pygments import highlight
# from pygments.lexers import PythonLexer
# from pygments.formatters import HtmlFormatter
 
#  # Taken from http://bzimmer.ziclix.com/2008/12/17/python-thread-dumps/
 
# def stacktraces():
#     code = []
#     for threadId, stack in sys._current_frames().items():
#         code.append("\n# ThreadID: %s" % threadId)
#         for filename, lineno, name, line in traceback.extract_stack(stack):
#             code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
#             if line:
#                 code.append("  %s" % (line.strip()))
 
#     return highlight("\n".join(code), PythonLexer(), HtmlFormatter(
#       full=False,
#       # style="native",
#       noclasses=True,
#     ))


# # This part was made by nagylzs
# import os
# import time
# import threading

# class TraceDumper(threading.Thread):
#     """Dump stack traces into a given file periodically."""
#     def __init__(self,fpath,interval,auto):
#         """
#         @param fpath: File path to output HTML (stack trace file)
#         @param auto: Set flag (True) to update trace continuously.
#             Clear flag (False) to update only if file not exists.
#             (Then delete the file to force update.)
#         @param interval: In seconds: how often to update the trace file.
#         """
#         assert(interval>0.1)
#         self.auto = auto
#         self.interval = interval
#         self.fpath = os.path.abspath(fpath)
#         self.stop_requested = threading.Event()
#         threading.Thread.__init__(self)
    
#     def run(self):
#         while not self.stop_requested.isSet():
#             time.sleep(self.interval)
#             if self.auto or not os.path.isfile(self.fpath):
#                 self.stacktraces()
    
#     def stop(self):
#         self.stop_requested.set()
#         self.join()
#         try:
#             if os.path.isfile(self.fpath):
#                 os.unlink(self.fpath)
#         except:
#             pass
    
#     def stacktraces(self):
#         fout = open(self.fpath,"wb+")
#         try:
#             fout.write(stacktraces().encode())
#         finally:
#             fout.close()


# _tracer = None
# def trace_start(fpath,interval=5,auto=True):
#     """Start tracing into the given file."""
#     global _tracer
#     if _tracer is None:
#         _tracer = TraceDumper(fpath,interval,auto)
#         _tracer.setDaemon(True)
#         _tracer.start()
#     else:
#         raise Exception("Already tracing to %s"%_tracer.fpath)

# def trace_stop():
#     """Stop tracing."""
#     global _tracer
#     if _tracer is None:
#         raise Exception("Not tracing, cannot stop.")
#     else:
#         _trace.stop()
#         _trace = None


class HypergolixService(WebsocketsIPC, Dispatcher, AgentBase):
    def __init__(self, host, *args, **kwargs):
        super().__init__(
            dispatcher = self, 
            dispatch = self, 
            persister = WSPersister(host=host, port=7770, threaded=True, debug=True), 
            host = 'localhost', # IPC host
            port = 7772, # IPC port
            # threaded = True,
            # debug = True,
            *args, **kwargs
        )
        
    
class HypergolixLink(WebsocketsEmbed):
    def __init__(self, *args, **kwargs):
        super().__init__(
            host = 'localhost', # IPC host
            port = 7772, # IPC port
            # debug = True,
            # threaded = True,
            *args, **kwargs
        )


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Start the Hypergolix service.')
    parser.add_argument(
        '--host', 
        action = 'store',
        default = 'localhost', 
        type = str,
        help = 'Specify the persistence provider host [default: localhost]'
    )

    args = parser.parse_args()
    
    service = HypergolixService(
        host = args.host,
        threaded = False,
    )
    
    # trace_start('debug/service.html')
    
    service.ws_run()
    
    # trace_stop()