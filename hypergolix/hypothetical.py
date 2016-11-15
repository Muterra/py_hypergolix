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
import inspect
import functools


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


def fixture_noop(func):
    ''' Create a no-op version of the function to use as its fixture.
    Should be used as a decorator above @public_api.
    '''
    signature = inspect.Signature.from_callable(func)
    
    if inspect.iscoroutinefunction(func):
        @func.fixture
        @functools.wraps(func)
        async def noop_fixture(*args, _signature=signature, **kwargs):
            # Make sure the passed args, kwargs are correct, but otherwise,
            # do nothing.
            _signature.bind(*args, **kwargs)
    
    else:
        @func.fixture
        @functools.wraps(func)
        def noop_fixture(*args, _signature=signature, **kwargs):
            # Make sure the passed args, kwargs are correct, but otherwise,
            # do nothing.
            _signature.bind(*args, **kwargs)
        
    return noop_fixture
        
        
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
    
    def __new__(mcls, clsname, bases, namespace, *args, _short_circuit=False,
                **kwargs):
        ''' Modify the existing namespace:
        1.  remove any @fixture_api methods.
        2.  extract any @public_api.fixture methods
        3.  extract any @public_api.interface methods
        4.  create cls.__fixture__ class object
        5.  create cls.__interface__ class object
        '''
        # Dead-end for fixtures and interfaces
        if _short_circuit:
            cls = super().__new__(mcls, clsname, bases, namespace, *args,
                                  **kwargs)
        
        # For the parent/public class...
        else:
            public_name = clsname
            fixture_name = clsname + 'Fixture'
            interface_name = clsname + 'Interface'
                
            public_namespace = {}
            fixture_namespace = {}
            interface_namespace = {}
        
            # No need to modify bases, either for the actual type or the
            # fixture/interface
            
            # Iterate over the entire defined namespace.
            for name, obj in namespace.items():
                is_fixture = getattr(obj, '__is_fixture__', False)
                is_interface = getattr(obj, '__is_interface__', False)
                
                # If the .fixture magic attr was defined, use it; else, just
                # fall back to inheritance
                fixture = getattr(obj, '__fixture__', None)
                interface = getattr(obj, '__interface__', None)
                
                # __is_fixture__ get sent only to the fixture.
                if is_fixture:
                    fixture_namespace[name] = obj
                    
                elif is_interface:
                    interface_namespace[name] = obj
                
                # All other objects pass via inheritance.
                else:
                    public_namespace[name] = obj
                
                # If we have an actual fixture defined, then it supercedes
                # public_namespace
                if fixture is not None:
                    fixture_namespace[name] = fixture
                    
                # Same goes with the interface.
                if interface is not None:
                    interface_namespace[name] = interface
            
            # Create the class
            cls = super().__new__(
                mcls,
                public_name,
                bases,
                public_namespace,
                *args,
                **kwargs
            )
            
            # Figure out the fixture inheritance tree. Fixtures always take
            # precedent over parents, but also always inherit them.
            fixture_bases = (cls, *(
                base.__fixture__ if hasattr(base, '__fixture__') else base
                for base in bases
            ))
            interface_bases = (cls, *(
                base.__interface__ if hasattr(base, '__interface__') else base
                for base in bases
            ))
            
            # NOTE:
            # See Python docs on super() with zero args: "the compiler fills in
            # the necessary details to correctly retrieve the class being
            # defined". Therefore, this is necessary when using fixture_bases:
            # super(Class, self).__init__(*args, **kwargs)
            
            # Now add in the types for both the fixture and the interface.
            # Reuse same bases for both, but use the REAL metaclass for it.
            # TODO: make the fixture inherit from the cls above, so that it can
            # have access to the original methods using super()
            cls.__fixture__ = mcls(
                fixture_name,
                fixture_bases,
                fixture_namespace,
                *args,
                _short_circuit = True,
                **kwargs
            )
            cls.__interface__ = mcls(
                interface_name,
                interface_bases,
                interface_namespace,
                *args,
                _short_circuit = True,
                **kwargs
            )
        
        # And don't forget to return the final cls object.
        return cls
        
    def __init__(cls, *args, _short_circuit=False, **kwargs):
        # Just strip out _short_circuit
        super().__init__(*args, **kwargs)
