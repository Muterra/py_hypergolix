''' API planning for a hypothetical auto-fixturing library.

Some thoughts:
+ What about defining the interfaces somewhere as an ABC-esque metaclass
  or something? That would let us define the public API somewhere
  standardized.
+ What about bidirectional interface spec? As in, for test generation,
  how do you make sure that the required external APIs from other
  components get called appropriately?
+ Can do the decorators without substantial performance hit by just
  adding fixture() function as an attribute to the method object. BUT,
  that requires a different method name!
  
Actually, can set anything as attr, and decorator can return the
original object. IE,

@interface
def func(self, *args, **kwargs):
    pass
    
func.__interface__ = True
func.fixture = decorator
func.__fixture__ = fixture
func.spec = decorator
func.__spec__ = spec

but both fixture and spec return the same func object, just with their
respective attributes filled in.

------

Furthermore, you don't want to define your ABC/interface in a separate
place. Yes, you may want to enforce interface consistency, but that's a
process control question, not a coding problem.
'''


from hypothetical import API
from hypothetical import public_api
from hypothetical import fixture_api


class ComponentWithFixturableAPI(metaclass=API):
    ''' This is a component that we would like to automatically generate
    fixtures for. We would also like to have testing be some degree of
    automated.
    '''
    
    @public_api
    def __init__(self, *args, **kwargs):
        ''' Do the standard init for the object.
        '''
        
    @__init__.fixture
    def __init__(self, *args, **kwargs):
        ''' This is converted into the __init__ method for the fixture.
        It is not required.
        '''
        
    @fixture_api
    def some_fixture_method(self, *args, **kwargs):
        ''' The @fixture_api decorator converts this into something that
        is only available to the fixture (and not the normal object).
        Use it for testing-specific code, like resetting the fixture to
        its pristine state.
        '''
    
    @public_api
    def no_fixture_method_here(self, *args, **kwargs):
        ''' This is a normal method that needs no fixture.
        '''
    
    @public_api
    def some_method_here(self, *args, **kwargs):
        ''' This is the normal call for a fixtured method. When it is
        called, actual code runs.
        '''
        
    @some_method_here.fixture
    def some_method_here(self, *args, **kwargs):
        ''' This is the fixtured method. When it is called, fixture code
        runs. It will not exist in the object class, and will only be
        available through cls.__fixture__().some_method_here().
        '''
        
    @some_method_here.interface
    def some_method_here(self, *args, **kwargs):
        ''' This defines the ABC. It's automatically injected into the
        cls.__interface__() object as an abc.abstractmethod, but
        additionally, it can define functions that must be called from
        within the method.
        '''
        

# To use it, use a metaclass-injected classmethod:
# This creates a fixture, for use in testing
component_fixture = ComponentWithFixturableAPI.__fixture__()
# This creates an abc-like-spec, for use in GIT process control / commit checks
component_interface = ComponentWithFixturableAPI.__interface__()

''' Notes on interfaces!

+ @interface can inject another __attr__ on top of abc.abstractmethod.
+ The actul interface class should probably use a subclass of
  abc.ABCMeta as a metaclass.
'''
