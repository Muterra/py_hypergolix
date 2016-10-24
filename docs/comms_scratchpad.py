from loopa import TaskCommander
from hypergolix.comms import RequestResponseProtocol
from hypergolix.comms import request
from hypergolix.comms import BasicServer
from hypergolix.comms import MsgBuffer
from hypergolix.comms import WSConnection
from hypergolix.comms import ConnectionManager


class ProtoDef(metaclass=RequestResponseProtocol, success_code=b'AK',
               failure_code=b'NK', error_codes={}, default_version=b'\x02'):
    ''' Note that there should probably be a way to specify an optional
    maximum version in addition to the default version. Otherwise,
    defaults will always attempt to handle things. Perhaps should also
    support a current version, so that loggers can warn when running
    requests for newer, unknown protocol versions that are still below
    the max version.
    '''
    # Note that the decorator defaults to versions=None, which supports any and
    # all versions.
    @request(b'PB')
    async def publish(self, connection, *args, **kwargs):
        ''' Explicitly specify to wait forever.
        '''
        # Returns bytes to send
        return bytes(14)

    @publish.request_handler
    async def publish(self, connection, body):
        ''' What to do with the request. Will be handled "server"-side
        for publish requests. Must return bytes-like response.
        '''
        # expects bytes in body
        # returns bytes in result
        return b''

    @publish.response_handler
    async def publish(self, connection, response, exc):
        ''' What to do with the response. What this returns will be
        returned as the result of publish(). By default, we will raise
        any exception and return any result.
        '''
        # response will be bytes if successful request; None if unsuccessful
        # exc will be an exception if unsuccessful request; None if successful
        if exc is not None:
            raise exc
        else:
            return response
            
    @request(b'TT', versions=[b'\x00'])
    async def versioned_0(self, connection, *args, **kwargs):
        ''' Do things.
        '''
        return bytes(5)
        
    @versioned_0.request_handler
    async def versioned_0(self, connection, body):
        ''' Response handler for this version only.
        '''
        return b''
        
    # Note that response handlers are not required.
            
    @request(b'TT', versions=[b'\x01'])
    async def versioned_1(self, connection, *args, **kwargs):
        ''' Do things with a different version.
        '''
        return bytes(10)
        
    @versioned_1.request_handler
    async def versioned_1(self, connection, body):
        ''' Response handler for the different version only.
        '''
        # Must be different method, even if same content.
        return b''
            
    @request(b'TT')
    async def versioned_2(self, connection, *args, **kwargs):
        ''' With no version specified, all other versions use this
        method.
        '''
        return bytes(10)
        
    @versioned_2.request_handler
    async def versioned_2(self, connection, body):
        ''' Requests that don't match other versions will use this
        method.
        '''
        return b'defaultversion'
        

class Responder(ProtoDef):
    ''' Add in response-specific stuff here if necessary.
    '''
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.example_responder_needs_this = 5


class Requestor(ProtoDef):
    ''' Add in request-specific server stuff here if necessary.
    '''
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.example_requestor_needs_this = 5


# Context managers shouldn't be used for wrapping -- whatever defines success
# and failure codes should be responsible for catching errors.


# ###############################################
# "Server" code (ie, anyone intended to have multiple connections)
# ###############################################


server_commander = TaskCommander(
    reusable_loop = True,
    threaded = False
)

responder = Responder()
msg_buffer = MsgBuffer(responder)
managed_server = BasicServer(connection_cls=WSConnection)

server_commander.register_task(
    managed_server,
    # This is where we specify what handles incoming requests
    msg_handler = msg_buffer,
    # Here we specify the server's host
    host = '',
    # And here, the server's port
    port = 3
)
server_commander.register_task(
    msg_buffer
)
server_commander.start()


# And then within the loop, when sending things during eg. postal runs, call
# the msg_buffer send methods instead of the protodef/responder ones:
async def some_handler_here(connection, msg):
    msg_buffer.update_subscription(connection, msg)


# ###############################################
# "Server" code 2 (ex. IPC, where concurrent connections should be few)
# ###############################################


server_commander = TaskCommander(
    reusable_loop = True,
    threaded = False
)

responder = Responder()
managed_server = BasicServer(connection_cls=WSConnection)

server_commander.register_task(
    managed_server,
    # This is where we specify what handles incoming requests
    msg_handler = responder,
    # Here we specify the server's host
    host = '',
    # And here, the server's port
    port = 3
)
server_commander.start()


# And then within the loop, when sending things during eg. postal runs, call
# the msg_buffer send methods instead of the protodef/responder ones:
async def some_other_handler_here(connection, msg):
    responder.update_subscription(connection, msg)


# ###############################################
# "Client" code (ie, anyone intended to have only a single connection)
# ###############################################


client_commander = TaskCommander(
    reusable_loop = True,
    threaded = False
)


requestor = Requestor()
# ConnectionManager handles everything else re: connections, including creating
# them. It wraps the Requestor, passing all requests to the current connection.
client = ConnectionManager(
    connection_cls = WSConnection,
    msg_handler = requestor
)
# We can directly use the requestor as the handler, since we're not expecting
# tons of incoming traffic. But, that's defined within the client itself, not
# through args passed to the connection.
client_commander.register_task(
    client,
    host = '',
    port = 3,
    tls = True
)
client_commander.start()


# Okay, now in the application code, we just call as usual, but through the
# client instead of the protodef/requestor
async def some_client_code(*args, **kwargs):
    await client.publish(b'ullllllogna')
