''' Generate a bunch of proxy methods. For the love of all that is good
and productive, don't try to do this manually.
'''
    
# Directly format as per:
otherless = [
    # def               (self                    ):
    ('__bool__',            '',                     
                                # self._proxy_3141592
                'return bool(',                     ')'),
    
    # def               (self                    ):
    ('__bytes__',           '',                     
                                # self._proxy_3141592
                'return bytes(',                    ')'),
    
    # def               (self                    ):
    ('__str__',             '',                     
                                # self._proxy_3141592
                'return str(',                      ')'),
    
    # def               (self                    ):
    ('__format__',          ', *args, **kwargs',                     
                                # self._proxy_3141592
                'return format(',                   ', *args, **kwargs)'),
    
    # def               (self                    ):
    ('__len__',             '',                     
                                # self._proxy_3141592
                'return len(',                      ')'),
    
    # def               (self                    ):
    ('__length_hint__',     '',                     
                                # self._proxy_3141592
                'return operator.length_hint(',     ')'),
    
    # def               (self                    ):
    ('__call__',            ', *args, **kwargs',    
                                # self._proxy_3141592
                'return ',                          '(*args, **kwargs)'),
    
    # def               (self                    ):
    ('__getitem__',         ', key',                
                                # self._proxy_3141592
                'return ',                          '[key]'),
    
    # def               (self                    ):
    ('__missing__',         ', key',                
                                # self._proxy_3141592
                'return ',                          '.__missing__(key)'),
    
    # def               (self                    ):
    ('__setitem__',         ', key, value',         
                                # self._proxy_3141592
                '',                                 '[key] = value'),
    
    # def               (self                    ):
    ('__delitem__',         ', key',                
                                # self._proxy_3141592
                'del ',                             '[key]'),
    
    # def               (self                    ):
    ('__iter__',            '',                     
                                # self._proxy_3141592
                'return iter(',                     ')'),
    
    # def               (self                    ):
    ('__reversed__',        '',                     
                                # self._proxy_3141592
                'return reversed(',                 ')'),
    
    # def               (self                    ):
    ('__contains__',        ', item',               
                                # self._proxy_3141592
                'return item in ',                  ''),
    
    # def               (self                    ):
    ('__enter__',           '',                     
                                # self._proxy_3141592
                'return ',                          '.__enter__()'),
    
    # def               (self                    ):
    ('__exit__',            ', *args, **kwargs',    
                                # self._proxy_3141592
                'return ',                          '.__exit__(*args, **kwargs)'),
    
    # def               (self                    ):
    ('__aenter__',          '',                     
                                # self._proxy_3141592
                'return ',                          '.__aenter__()'),
    
    # def               (self                    ):
    ('__aexit__',           ', *args, **kwargs',    
                                # self._proxy_3141592
                'return ',                          '.__aexit__(*args, **kwargs)'),
    
    # def               (self                    ):
    ('__await__',           '',                     
                                # self._proxy_3141592
                'return ',                          '.__await__()'),
    
    # def               (self                    ):
    ('__aiter__',           '',                     
                                # self._proxy_3141592
                'return ',                          '.__aiter__()'),
    
    # def               (self                    ):
    ('__anext__',           '',                     
                                # self._proxy_3141592
                'return ',                          '.__anext__()'),
]

# Self and other and *args, **kwargs 
othered = [
    ('__add__',         '+',    None),
    ('__sub__',         '-',    None),
    ('__mul__',         '*',    None),
    ('__matmul__',      '@',    None),
    ('__truediv__',     '/',    None),
    ('__floordiv__',    '//',   None),
    ('__mod__',         '%',    None),
    ('__divmod__',      None,   'divmod'),
    ('__pow__',         None,   'pow'),
    ('__lshift__',      '<<',   None),
    ('__rshift__',      '>>',   None),
    ('__and__',         '&',    None),
    ('__xor__',         '^',    None),
    ('__or__',          '|',    None),
]

rothered = [
    '__radd__',
    '__rsub__',
    '__rmul__',
    '__rmatmul__',
    '__rtruediv__',
    '__rfloordiv__',
    '__rmod__',
    '__rdivmod__',
    '__rpow__',
    '__rlshift__',
    '__rrshift__',
    '__rand__',
    '__rxor__',
    '__ror__',
]

incrd = [
    ('__iadd__',        '+='),
    ('__isub__',        '-='),
    ('__imul__',        '*='),
    ('__imatmul__',     '@='),
    ('__itruediv__',    '/='),
    ('__ifloordiv__',   '//='),
    ('__imod__',        '%='),
    ('__ipow__',        '**='),
    ('__ilshift__',     '<<='),
    ('__irshift__',     '>>='),
    ('__iand__',        '&='),
    ('__ixor__',        '^='),
    ('__ior__',         '|='),
]

unaryish = [
    ('__neg__',         '-'),
    ('__pos__',         '+'),
    ('__abs__',         'abs'),
    ('__invert__',      '~'),
    ('__complex__',     'complex'),
    ('__int__',         'int'),
    ('__float__',       'float'),
    ('__round__',       'round'),
    ('__index__',       'operator.index'),
]
    
otherless_template = \
"""    def $dunder$(self$argspec$):
        ''' Wrap $dunder$ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        $opspec$self._proxy_3141592$closespec$
        
"""

othered_template_oper = \
"""    def $dunder$(self, other):
        ''' Wrap $dunder$ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return self._proxy_3141592 $dunder_oper$ other._proxy_3141592
        
        else:
            return self._proxy_3141592 $dunder_oper$ other
            
"""

othered_template_func = \
"""    def $dunder$(self, other, *args, **kwargs):
        ''' Wrap $dunder$ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            return $dunder_func$(
                self._proxy_3141592, 
                other._proxy_3141592,
                *args, 
                **kwargs
            )
        
        else:
            return $dunder_func$(
                self._proxy_3141592, 
                other,
                *args, 
                **kwargs
            )
            
"""

rothered_template_oper = \
"""    def $dunder$(self, other):
        ''' Wrap $dunder$ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return other._proxy_3141592 $dunder_oper$ self._proxy_3141592
        
        else:
            return other $dunder_oper$ self._proxy_3141592
            
"""

rothered_template_func = \
"""    def $dunder$(self, other):
        ''' Wrap $dunder$ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no reversed operations are passed *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            return $dunder_func$(other._proxy_3141592, self._proxy_3141592)
        
        else:
            return $dunder_func$(other, self._proxy_3141592)
            
"""

incrd_template = \
"""    def $dunder$(self, other):
        ''' Wrap $dunder$ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        # Note that no incremental operations are PASSED *args or **kwargs
        
        # We could do this to *any* HGXObjBase, but I don't like the idea of
        # forcibly upgrading those, since they might do, for example, some
        # different comparison operation or something. This seems like a 
        # much safer bet.
        if isinstance(other, NoopProxy):
            # Other proxies are very likely to fail, since the reveresed call 
            # would normally have already been called -- but try them anyways.
            self._proxy_3141592 $dunder_oper$ other._proxy_3141592
        
        else:
            self._proxy_3141592 $dunder_oper$ other
            
        return self
            
"""

# Take advantage of the fact that we can use parenthesis to satisfy both the
# functions and the operators for the unaries.
unaryish_template = \
"""    def $dunder$(self):
        ''' Wrap $dunder$ to pass into the _proxy object.
        
        This method was (partially?) programmatically generated by a 
        purpose-built script.
        '''
        return $dunder_oper$(self._proxy_3141592)
            
"""

outstr = ''
    
# Hit all of the otherless stuff
for dunder_tuple in otherless:
    dunder, argspec, opspec, closespec = dunder_tuple
    
    tempstr = otherless_template.replace('$dunder$', dunder)
    tempstr = tempstr.replace('$argspec$', argspec)
    tempstr = tempstr.replace('$opspec$', opspec)
    tempstr = tempstr.replace('$closespec$', closespec)
    
    outstr += tempstr

# Now move on to the othered stuff, but only forwards
for dunder_tuple in othered:
    dunder, opera, func = dunder_tuple
    
    if opera is not None:
        tempstr = othered_template_oper.replace('$dunder$', dunder)
        tempstr = tempstr.replace('$dunder_oper$', opera)
        
    else:
        tempstr = othered_template_func.replace('$dunder$', dunder)
        tempstr = tempstr.replace('$dunder_func$', func)
    
    outstr += tempstr
    
# And now the equivalent reversed
for dunder, dunder_tuple in zip(rothered, othered):
    # Note that dunder is already declared above
    __, opera, func = dunder_tuple
    
    if opera is not None:
        tempstr = rothered_template_oper.replace('$dunder$', dunder)
        tempstr = tempstr.replace('$dunder_oper$', opera)
        
    else:
        tempstr = rothered_template_func.replace('$dunder$', dunder)
        tempstr = tempstr.replace('$dunder_func$', func)
    
    outstr += tempstr
    
# All of the incremental assignment operators
for dunder_tuple in incrd:
    dunder, opera = dunder_tuple
    tempstr = incrd_template.replace('$dunder$', dunder)
    tempstr = tempstr.replace('$dunder_oper$', opera)
    outstr += tempstr
    
# And finally, the unary (or similar) operators
for dunder_tuple in unaryish:
    dunder, opera = dunder_tuple
    tempstr = unaryish_template.replace('$dunder$', dunder)
    tempstr = tempstr.replace('$dunder_oper$', opera)
    outstr += tempstr
    
with open('dunders.py', 'w+') as f:
    f.write(outstr)