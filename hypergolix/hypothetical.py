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

import logging


# ###############################################
# Boilerplate
# ###############################################


__all__ = [
    'API',
    'public_api',
    'fixture_api'
]


logger = logging.getLogger(__name__)


# ###############################################
# Lib
# ###############################################
        
        
def public_api(func):
    ''' Decorator to automatically mark the object as the normal thing.
    '''
    
    def fixture_closure(fixture_func, public_func=func):
        ''' Defines the decorator for @method.fixture.
        '''
        # This is the actual __fixture__ method, to be defined via decorator
        public_func.__fixture__ = fixture_func
        return public_func
    
    def interface_closure(interface_func, public_func=func):
        ''' Defines the decorator for @method.fixture.
        '''
        # This is the actual __interface method, to be defined via decorator
        public_func.__interface__ = interface_func
        return public_func
        
    func.fixture = fixture_closure
    func.interface = interface_closure
    
    # This denotes that it is an API
    func.__is_api__ = True
    
    return func
    
    
def fixture_api(func):
    ''' Decorator to mark the method as a fixture-only object.
    '''
    ''' Decorator to automatically mark the object as the normal thing.
    '''
    
    # Huh, well, this is easy.
    func.__is_fixture__ = True
    return func


class API(type):
    ''' Metaclass for defining an interfaceable API.
    '''
    
    def __new__(mcls, name, bases, namespace, *args, fixture_bases=None,
                **kwargs):
        ''' Modify the existing namespace:
        1.  remove any @fixture_api methods.
        2.  extract any @public_api.fixture methods
        3.  extract any @public_api.interface methods
        4.  create cls.__fixture__ class object
        5.  create cls.__interface__ class object
        '''
        public_name = name
        fixture_name = name + 'Fixture'
        interface_name = name + 'Interface'
            
        public_namespace = {}
        fixture_namespace = {}
        interface_namespace = {}
        
        if fixture_bases is None:
            fixture_bases = bases
        else:
            fixture_bases = (*fixture_bases, *bases)
            # NOTE:
            # See Python docs on super() with zero args: "the compiler fills in
            # the necessary details to correctly retrieve the class being
            # defined". Therefore, this is necessary when using fixture_bases:
            # super(type(self), self).__init__(*args, **kwargs)
    
        # No need to modify bases, either for the actual type or the
        # fixture/interface
        
        # Iterate over the entire defined namespace.
        for name, obj in namespace.items():
            # __is_fixture__ get sent only to the fixture.
            if hasattr(obj, '__is_fixture__'):
                fixture_namespace[name] = obj
        
            # __is_api__ is distributed based on some rules...
            elif hasattr(obj, '__is_api__'):
                # __is_api__ always gets sent to public.
                public_namespace[name] = obj
                # Note that we COULD pop the magic values for the public
                # function here, but it would break subclassing. They aren't
                # particularly expensive, so keep them.
                
                # If the .fixture magic attr was defined, use it; else, default
                # to passing the unfixtured function
                fixture_namespace[name] = getattr(obj, '__fixture__', obj)
                # Same goes with the interface.
                interface_namespace[name] = getattr(obj, '__interface__', obj)
                
            # All other objects go only to the fixture and the public (but not
            # the interface).
            else:
                public_namespace[name] = obj
                fixture_namespace[name] = obj
        
        # Remove ourselves from the metaclass chain
        if mcls is API:
            # ONLY if our metaclass is Triplicate itself should we change mcls
            use_mcls = mcls.mro(mcls)[1]
        else:
            use_mcls = mcls
        
        # Create the class
        cls = super().__new__(
            use_mcls,
            public_name,
            bases,
            public_namespace,
            *args,
            **kwargs
        )
        
        # Now add in the types for both the fixture and the interface.
        # Reuse same bases for both, but use the REAL metaclass for it.
        # TODO: make the fixture inherit from the cls above, so that it can
        # have access to the original methods using super()
        cls.__fixture__ = super().__new__(
            mcls,
            fixture_name,
            fixture_bases,
            fixture_namespace,
            *args,
            **kwargs
        )
        cls.__interface__ = super().__new__(
            mcls,
            interface_name,
            bases,
            interface_namespace,
            *args,
            **kwargs
        )
        
        # And don't forget to return the final cls object.
        return cls
        
    def __init__(self, *args, fixture_bases=None, **kwargs):
        ''' Just here to drop the fixture bases.
        '''
        super().__init__(*args, **kwargs)
