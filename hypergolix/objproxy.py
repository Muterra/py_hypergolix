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

# Global dependencies
# import weakref
# import traceback
# import threading

# from golix import SecondParty
from golix import Ghid

# from golix.utils import AsymHandshake
# from golix.utils import AsymAck
# from golix.utils import AsymNak

# Local dependencies
# from .persistence import _GarqLite
# from .persistence import _GdxxLite


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


class ObjProxyBase:
    ''' ObjProxies, partly inspired by weakref.proxies, are a mechanism
    by which normal python objects can be "dropboxed" into hypergolix.
    The proxy object, and not the original object, must be referenced.
    
    Proxies pass through all attribute access to their proxied objects,
    with the exception of:
        1.  __init__
        2.  __repr__
        3.  __hash__ (see note [1] below)
        4.  __eq__ (see note [2] below)
        5.  hgx_ghid
        6.  hgx_api_id
        7.  hgx_private
        8.  hgx_dynamic
        9.  hgx_binder
        10. hgx_persistence
        11. hgx_live
        12. _hgx_update
        13. hgx_update_threadsafe
        14. hgx_update_loopsafe
        15. _hgx_callbacks
        16. hgx_callbacks_threadsafe
        17. hgx_callbacks_loopsafe
        18. _hgx_sync
        19. hgx_sync_threadsafe
        20. hgx_sync_loopsafe
        21. _hgx_share
        22. hgx_share_threadsafe
        23. hgx_share_loopsafe
        24. _hgx_freeze
        25. hgx_freeze_threadsafe
        26. hgx_freeze_loopsafe
        27. _hgx_hold
        28. hgx_hold_threadsafe
        29. hgx_hold_loopsafe
        30. _hgx_discard
        31. hgx_discard_threadsafe
        32. hgx_discard_loopsafe
        33. _hgx_delete
        34. hgx_delete_threadsafe
        35. hgx_delete_loopsafe
        36. _hgx_proxy_state
        37. _hgx_hgxlink
        38. _hgx_pack
        39. _hgx_unpack
    (as well as some name-mangled internal attributes).
    
    [1] Proxies are hashable if their ghids are defined, but unhashable 
    otherwise. Note, however, that their hashes have nothing to do with
    their objects, and instead are equal to the hash of their ghids, 
    combined with the (hard-coded) hash of the string 'hgxproxy'.
    
    [2] Equality comparisons, on the other hand, reference the proxy's 
    state directly. So if the states compare equally, the two ObjProxies 
    will compare equally, regardless of the proxy state (ghid, api_id, 
    etc).
    
    Side note: support for ie __enter__ and __exit__ aren't capable with
    pass-through lookup unless they are declared at class definition 
    time. They may require metaclass fiddling or something. Or, we could
    just inelegantly declare them and "hope for the best" calling the
    referent's __enter__/__exit__. It also doesn't work to assign them 
    as properties/attributes after the fact.
    '''
    def __init__(self, hgxlink, state, api_id=None, dynamic=True, 
                private=False, ghid=None):
        ''' Allocates the object locally, but does NOT create it. You
        have to explicitly call hgx_update, hgx_update_threadsafe, or
        hgx_update_loopsafe to actually create the sync'd object and get
        a ghid.
        '''
            
    def __repr__(self):
        classname = type(self).__name__
        return (
            '<' + classname + ' at ' + str(self._hgx_ghid) + ', proxying ' + 
            repr(self._hgx_proxy_state) + '>'
        )
        
    def __hash__(self):
        ''' Have a hash, if our ghid address is defined; otherwise, 
        return None (which will in turn cause Python to raise a 
        TypeError in the parent call).
        
        1354915019369302316 is the hash of the string 'hgxproxy' and is
        included to allow faster hash bucket differentiation between
        ghids and objproxies.
        '''
        if self._hgx_ghid is not None:
            return hash(self._hgx_ghid) + 1354915019369302316
        else:
            return None
        
    def __eq__(self, other):
        ''' Pass the equality comparison straight into the state.
        '''
        # If the other instance also has an _hgx_proxy_state attribute, compare
        # to that, such that two proxies with the same object state will always
        # compare equally
        try:
            return self._hgx_proxy_state == other._hgx_proxy_state
            
        # If not, just compare our proxy state directly to the other object.
        except AttributeError:
            return self._hgx_proxy_state == other
            
    @property
    def hgx_persistence(self):
        ''' Dictates what Hypergolix should do with the object upon its
        garbage collection by the Python process.
        
        May be:
            'strong'    Object is retained until hgx_delete is 
                        explicitly called, regardless of python runtime 
                        behavior / garbage collection. Default.
            'weak'      Object is retained until hgx_delete is
                        explicitly called, or when python runtime
                        garbage collects the proxy, EXCEPT at python
                        exit
            'temp'      Object is retained only for the lifetime of the
                        python object. Will be retained until hgx_delete
                        is explicitly called, or when python garbage
                        collects the proxy, INCLUDING at python exit.
        '''
        
    @hgx_persistence.setter
    def hgx_persistence(self, value):
        ''' Setter for hgx_persistence. Note that this attribute cannot
        be deleted.
        '''
        
# These are all the names in a plain 'ole object()
_OBJECT_NAMESPACE = {
    '__class__', 
    '__delattr__', 
    '__dir__', 
    '__doc__', 
    '__eq__', 
    '__format__', 
    '__ge__', 
    '__getattribute__', 
    '__gt__', 
    '__hash__', 
    '__init__', 
    '__le__', 
    '__lt__', 
    '__ne__', 
    '__new__', 
    '__reduce__', 
    '__reduce_ex__', 
    '__repr__', 
    '__setattr__', 
    '__sizeof__', 
    '__str__', 
    '__subclasshook__'
}

# These are all of the names in a user-defined class object, as best I can tell
_USER_NAMESPACE = {
    '__class__', 
    '__delattr__', 
    '__dict__', 
    '__dir__', 
    '__doc__', 
    '__eq__', 
    '__format__', 
    '__ge__', 
    '__getattr__', 
    '__getattribute__', 
    '__gt__', 
    '__hash__', 
    '__init__', 
    '__le__', 
    '__lt__', 
    '__module__', 
    '__ne__', 
    '__new__', 
    '__reduce__', 
    '__reduce_ex__', 
    '__repr__', 
    '__setattr__', 
    '__sizeof__', 
    '__str__', 
    '__subclasshook__', 
    '__weakref__', 
}
_USER_NAMESPACE_2 = {
    '__init__', 
    '__doc__', 
    '__module__', 
    '__gt__', 
    '__subclasshook__', 
    '__dir__', 
    '__eq__', 
    '__le__', 
    '__dict__', 
    '__class__', 
    '__ge__', 
    '__format__', 
    '__hash__', 
    '__repr__', 
    '__lt__', 
    '__setattr__', 
    '__weakref__', 
    '__delattr__', 
    '__getattribute__', 
    '__reduce_ex__', 
    '__sizeof__', 
    '__ne__', 
    '__reduce__', 
    '__str__', 
    '__new__'
}

_OVERRIDABLE NAMESPACE = { 
    '__eq__', 
    '__ge__', 
    '__gt__', 
    '__le__', 
    '__lt__', 
    '__ne__', 
    '__subclasshook__' 
}