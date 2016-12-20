from hypergolix.comms import RequestResponseProtocol
from hypergolix.comms import request

from hypergolix.hypothetical import API
from hypergolix.hypothetical import public_api
from hypergolix.hypothetical import fixture_api


class ReqResAPI(API, RequestResponseProtocol):
    ''' Compose fixturable API with a request/response protocol.
    '''


class ProtocolAPI(metaclass=RequestResponseProtocol, success_code=b'AK',
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
    @public_api
    @request(b'PB')
    async def publish(self, connection, *args, **kwargs):
        ''' Zip zop zoop
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
        
    @publish.fixture
    async def publish(self, connection, *args, **kwargs):
        ''' A fixture version of publish. Note that, unless trying to
        separate each half of the request/response loop for testing, we
        don't have any reason to fixture the request handler or the
        response handler. So this way, we can just fixture the expected
        behavior of the entire request chain.
        '''
            
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
