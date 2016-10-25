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
'''


from hypothetical import Interfaceable
from hypothetical import interface


class ComponentWithFixturableAPI(metaclass=Interfaceable):
    ''' This is a component that we would like to automatically generate
    fixtures for. We would also like to have testing be some degree of
    automated.
    '''
    
    def __init_fixture__(self, *args, **kwargs):
        ''' This is not required to work, but will be called when
        creating any fixture object for the component.
        '''
        
    def __reset_fixture__(self, *args, **kwargs):
        ''' Should this even exist? Should this be used, or should you
        just create a brand-new fixture object? I suppose it probably
        should exist, since otherwise you'd have problems propagating
        references into eg. other.assemble()
        '''
    
    @interface
    def some_method_here(self, *args, **kwargs):
        ''' This is the normal method. When it is called, actual code
        runs.
        '''
        
    @some_method_here.fixture
    def some_fixture_here(self, *args, **kwargs):
        ''' This is the fixtured method. When it is called, fixture code
        runs. Note that it needs to have a different name than the
        actual method (or we'll take a performance hit). Though,
        actually, we could probably make the @some_method_here.fixture
        decorator spit out a descriptor (a la properties) that handles
        both. It would allow lib devs to choose between performance hits
        with readability vs no performance hit, but less readable.
        '''
        

# To use it, use a metaclass-injected classmethod:
fixture = ComponentWithFixturableAPI.fixturize()
