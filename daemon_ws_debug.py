\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()]>>
Listener successfully shut down.
.Listener successfully shut down.
Listener successfully shut down.
Listener successfully shut down.
Connection closed.
Stopping loop.
Connection closed.
Stopping loop.
Done running forever.
Flag set.
Socket connected.
Socket connected.
Connection established: client 774639875245
Connection established: server.
Socket connected.
Socket connected.
Connection established: client 509275080671
Connection established: server.
Listener successfully shut down.
.
Listener successfully shut down.
----------------------------------------------------------------------
Ran 3 tests in 13.833s

OK
Listener successfully shut down.
Listener successfully shut down.
Connection closed.
Stopping loop.
Done running forever.
Connection closed.
Task was destroyed but it is pending!
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future finished result=
None>>Stopping loop.

Task was destroyed but it is pending!
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()]>>
Task was destroyed but it is pending!
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()]>>
Task was destroyed but it is pending!
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()]>>

C:\Users\BadgerDoc\Dropbox\Documents\Projects--Muterra\py_hypergolix>python test
s/trashtest/trashtest_comms.py
Flag set.
Socket connected.
Connection established: client 146935118965
Socket connected.
Connection established: server.
Socket connected.
Connection established: client 1068264493428
Socket connected.
Connection established: server.
.Listener successfully shut down.
Listener successfully shut down.
Listener successfully shut down.
Flag set.
Listener successfully shut down.
Connection closed.
Stopping loop.
Socket connected.
Connection established: client 976317913299
Socket connected.
Connection established: server.
Socket connected.
Connection established: client 650698557504
Socket connected.
Connection established: server.
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.624 seconds
Connection closed.
Stopping loop.
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.640 seconds
Done running forever.
Task was destroyed but it is pending!
source_traceback: Object created at (most recent call last):
  File "C:\Python35\lib\threading.py", line 882, in _bootstrap
    self._bootstrap_inner()
  File "C:\Python35\lib\threading.py", line 914, in _bootstrap_inner
    self.run()
  File "C:\Python35\lib\threading.py", line 862, in run
    self._target(*self._args, **self._kwargs)
  File "c:\users\badgerdoc\dropbox\documents\projects--muterra\py_hypergolix\hyp
ergolix\comms.py", line 867, in ws_run
    self._ws_loop.run_until_complete(self._server.wait_closed())
  File "C:\Python35\lib\asyncio\base_events.py", line 325, in run_until_complete

    self.run_forever()
  File "C:\Python35\lib\asyncio\base_events.py", line 295, in run_forever
    self._run_once()
  File "C:\Python35\lib\asyncio\base_events.py", line 1246, in _run_once
    handle._run()
  File "C:\Python35\lib\asyncio\events.py", line 125, in _run
    self._callback(*self._args)
  File "C:\Python35\lib\site-packages\websockets\server.py", line 230, in close
    asyncio_ensure_future(websocket.close(1001), loop=self.loop)
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()] created at C:\Python35\lib\asyncio\tasks.py:369> created at C:\Pytho
n35\lib\site-packages\websockets\server.py:230>
Task was destroyed but it is pending!
source_traceback: Object created at (most recent call last):
  File "C:\Python35\lib\threading.py", line 882, in _bootstrap
    self._bootstrap_inner()
  File "C:\Python35\lib\threading.py", line 914, in _bootstrap_inner
    self.run()
  File "C:\Python35\lib\threading.py", line 862, in run
    self._target(*self._args, **self._kwargs)
  File "c:\users\badgerdoc\dropbox\documents\projects--muterra\py_hypergolix\hyp
ergolix\comms.py", line 867, in ws_run
    self._ws_loop.run_until_complete(self._server.wait_closed())
  File "C:\Python35\lib\asyncio\base_events.py", line 325, in run_until_complete

    self.run_forever()
  File "C:\Python35\lib\asyncio\base_events.py", line 295, in run_forever
    self._run_once()
  File "C:\Python35\lib\asyncio\base_events.py", line 1246, in _run_once
    handle._run()
  File "C:\Python35\lib\asyncio\events.py", line 125, in _run
    self._callback(*self._args)
  File "C:\Python35\lib\site-packages\websockets\server.py", line 230, in close
    asyncio_ensure_future(websocket.close(1001), loop=self.loop)
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()] created at C:\Python35\lib\asyncio\tasks.py:369> created at C:\Pytho
n35\lib\site-packages\websockets\server.py:230>
Executing <Task pending coro=<WSBasicClient.ws_client() running at c:\users\badg
erdoc\dropbox\documents\projects--muterra\py_hypergolix\hypergolix\comms.py:919>
 wait_for=<Future pending cb=[Task._wakeup()] created at C:\Python35\lib\asyncio
\tasks.py:402> cb=[_run_until_complete_cb() at C:\Python35\lib\asyncio\base_even
ts.py:118] created at C:\Python35\lib\asyncio\base_events.py:317> took 0.125 sec
onds
.Listener successfully shut down.
Listener successfully shut down.
Flag set.
Listener successfully shut down.
Connection closed.
Stopping loop.
Listener successfully shut down.
Socket connected.
Connection established: client 640971902027
Socket connected.
Connection established: server.
Socket connected.
Connection established: client 460448437115
Socket connected.
Connection established: server.
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.640 seconds
Done running forever.
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.655 seconds
Task was destroyed but it is pending!
source_traceback: Object created at (most recent call last):
  File "C:\Python35\lib\threading.py", line 882, in _bootstrap
    self._bootstrap_inner()
  File "C:\Python35\lib\threading.py", line 914, in _bootstrap_inner
    self.run()
  File "C:\Python35\lib\threading.py", line 862, in run
    self._target(*self._args, **self._kwargs)
  File "c:\users\badgerdoc\dropbox\documents\projects--muterra\py_hypergolix\hyp
ergolix\comms.py", line 867, in ws_run
    self._ws_loop.run_until_complete(self._server.wait_closed())
  File "C:\Python35\lib\asyncio\base_events.py", line 325, in run_until_complete

    self.run_forever()
  File "C:\Python35\lib\asyncio\base_events.py", line 295, in run_forever
    self._run_once()
  File "C:\Python35\lib\asyncio\base_events.py", line 1246, in _run_once
    handle._run()
  File "C:\Python35\lib\asyncio\events.py", line 125, in _run
    self._callback(*self._args)
  File "C:\Python35\lib\site-packages\websockets\server.py", line 230, in close
    asyncio_ensure_future(websocket.close(1001), loop=self.loop)
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future finished result=
None created at C:\Python35\lib\asyncio\tasks.py:369> created at C:\Python35\lib
\site-packages\websockets\server.py:230>
Connection closed.
Stopping loop.
Task was destroyed but it is pending!
source_traceback: Object created at (most recent call last):
  File "C:\Python35\lib\threading.py", line 882, in _bootstrap
    self._bootstrap_inner()
  File "C:\Python35\lib\threading.py", line 914, in _bootstrap_inner
    self.run()
  File "C:\Python35\lib\threading.py", line 862, in run
    self._target(*self._args, **self._kwargs)
  File "c:\users\badgerdoc\dropbox\documents\projects--muterra\py_hypergolix\hyp
ergolix\comms.py", line 867, in ws_run
    self._ws_loop.run_until_complete(self._server.wait_closed())
  File "C:\Python35\lib\asyncio\base_events.py", line 325, in run_until_complete

    self.run_forever()
  File "C:\Python35\lib\asyncio\base_events.py", line 295, in run_forever
    self._run_once()
  File "C:\Python35\lib\asyncio\base_events.py", line 1246, in _run_once
    handle._run()
  File "C:\Python35\lib\asyncio\events.py", line 125, in _run
    self._callback(*self._args)
  File "C:\Python35\lib\site-packages\websockets\server.py", line 230, in close
    asyncio_ensure_future(websocket.close(1001), loop=self.loop)
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()] created at C:\Python35\lib\asyncio\tasks.py:369> created at C:\Pytho
n35\lib\site-packages\websockets\server.py:230>
.
----------------------------------------------------------------------
Listener successfully shut down.
Ran 3 tests in 150.634s

OK
Listener successfully shut down.
Listener successfully shut down.
Listener successfully shut down.
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.889 seconds
Connection closed.
Stopping loop.
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.905 seconds
Connection closed.
Stopping loop.
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.905 seconds
Executing <Task finished coro=<WebSocketCommonProtocol.run() done, defined at C:
\Python35\lib\site-packages\websockets\protocol.py:389> result=None created at C
:\Python35\lib\site-packages\websockets\protocol.py:615> took 0.562 seconds
Done running forever.
Task was destroyed but it is pending!
source_traceback: Object created at (most recent call last):
  File "C:\Python35\lib\threading.py", line 882, in _bootstrap
    self._bootstrap_inner()
  File "C:\Python35\lib\threading.py", line 914, in _bootstrap_inner
    self.run()
  File "C:\Python35\lib\threading.py", line 862, in run
    self._target(*self._args, **self._kwargs)
  File "c:\users\badgerdoc\dropbox\documents\projects--muterra\py_hypergolix\hyp
ergolix\comms.py", line 867, in ws_run
    self._ws_loop.run_until_complete(self._server.wait_closed())
  File "C:\Python35\lib\asyncio\base_events.py", line 325, in run_until_complete

    self.run_forever()
  File "C:\Python35\lib\asyncio\base_events.py", line 295, in run_forever
    self._run_once()
  File "C:\Python35\lib\asyncio\base_events.py", line 1246, in _run_once
    handle._run()
  File "C:\Python35\lib\asyncio\events.py", line 125, in _run
    self._callback(*self._args)
  File "C:\Python35\lib\site-packages\websockets\server.py", line 230, in close
    asyncio_ensure_future(websocket.close(1001), loop=self.loop)
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()] created at C:\Python35\lib\asyncio\tasks.py:369> created at C:\Pytho
n35\lib\site-packages\websockets\server.py:230>
Task was destroyed but it is pending!
source_traceback: Object created at (most recent call last):
  File "C:\Python35\lib\threading.py", line 882, in _bootstrap
    self._bootstrap_inner()
  File "C:\Python35\lib\threading.py", line 914, in _bootstrap_inner
    self.run()
  File "C:\Python35\lib\threading.py", line 862, in run
    self._target(*self._args, **self._kwargs)
  File "c:\users\badgerdoc\dropbox\documents\projects--muterra\py_hypergolix\hyp
ergolix\comms.py", line 867, in ws_run
    self._ws_loop.run_until_complete(self._server.wait_closed())
  File "C:\Python35\lib\asyncio\base_events.py", line 325, in run_until_complete

    self.run_forever()
  File "C:\Python35\lib\asyncio\base_events.py", line 295, in run_forever
    self._run_once()
  File "C:\Python35\lib\asyncio\base_events.py", line 1246, in _run_once
    handle._run()
  File "C:\Python35\lib\asyncio\events.py", line 125, in _run
    self._callback(*self._args)
  File "C:\Python35\lib\site-packages\websockets\server.py", line 230, in close
    asyncio_ensure_future(websocket.close(1001), loop=self.loop)
task: <Task pending coro=<WebSocketCommonProtocol.close() running at C:\Python35
\lib\site-packages\websockets\protocol.py:223> wait_for=<Future pending cb=[Task
._wakeup()] created at C:\Python35\lib\asyncio\tasks.py:369> created at C:\Pytho
n35\lib\site-packages\websockets\server.py:230>

C:\Users\BadgerDoc\Dropbox\Documents\Projects--Muterra\py_hypergolix>