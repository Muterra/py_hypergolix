[![Hypergolix logo](/assets/hypergolix-logo.png)](https://www.hypergolix.com)

Hypergolix is a local background service that makes IoT development effortless and secure. It uses the [Golix protocol](https://github.com/Muterra/doc-golix) to provide all data with strong, end-to-end encryption. Usage:

1. Download Hypergolix client
2. Start Hypergolix client in the background
3. Log in to Hypergolix
4. Applications communicate with Hypergolix via IPC

All secret material is handled by Hypergolix, as is network delivery. **Hypergolix itself requires ```python >= 3.5.1```,** but applications can also communicate with Hypergolix via websockets on localhost port 7772. An application link is included with Hypergolix that supports two entry points:

```python
class AppObj:    
    def __init__(self, embed, state, api_id=None, private=False, dynamic=True, 
    callbacks=None):
        ''' Create a new AppObj.
        
        embed is the Hypergolix link object.
        state is bytes-like.
        api_id is bytes-like and len(65) (see below).
        private is truthy.
        dynamic is truthy.
        callbacks is iterable of hashable, callable objects.
        '''
        
    @property
    def author(self):
        ''' The GHID address (key fingerprint) of the object's creator.
        '''
        
    @property
    def address(self):
        ''' The GHID (hash) address of the object itself.
        '''
            
    @property
    def private(self):
        ''' Read-only property. If True, the object is private, and used
        only by a particular application. If False, the object is not
        private, and may be shared with other applications and entities.
        '''
        
    @property
    def api_id(self):
        ''' The api_id (if one exists) of the object. This identifier 
        can more or less be thought of as a unique identifier for a 
        particular binary API schema. Optional for private objects.
        '''
        
    @property
    def callbacks(self):
        ''' Lists current update callbacks.
        '''
            
    @property
    def is_dynamic(self):
        ''' Indicates whether this object is dynamic.
        returns True/False.
        '''
        
    @property
    def is_owned(self):
        ''' Indicates whether this object was created by the Agent 
        (private key) currently logged in with the Hypergolix service.
        
        returns True/False.
        '''
            
    @property
    def mutable(self):
        ''' Returns true if and only if self is a dynamic object and is
        owned by the current agent.
        '''
        
    @property
    def is_link(self):
        ''' Returns True if this object is a dynamic proxy to another
        object. If not, returns False if dynamic and None if static.
        '''
            
    @property
    def link_address(self):
        ''' Only available when is_link is True. Otherwise, will return
        None.
        '''
        
    @property
    def state(self):
        ''' Read property returning the binary state of the object. If
        the object is mutable (ie owned and dynamic), AppObj.state is
        also writeable.
        '''
            
    @state.setter
    def state(self, value):
        ''' Wraps update() for dynamic objects. Attempts to directly set
        self._state for static objects, but will return AttributeError
        if state is already set.
        '''
        
    def add_callback(self, callback):
        ''' Registers a callback to be called when the object receives
        an update.
        
        callback must be hashable and callable. Function definitions and
        lambdas are natively hashable; callable classes may or may not 
        be; you may need to define __hash__.
        
        On update, callbacks are passed the object.
        '''
        
    def remove_callback(self, callback):
        ''' Removes a callback.
        
        Raises KeyError if the callback has not been registered.
        '''
        
    def clear_callbacks(self):
        ''' Resets all callbacks.
        '''
            
    def update(self, state):
        ''' Updates a mutable object to a new state. This update is 
        automatically distributed to any shared recipients, as well as
        any other machines the current Agent is logged in at.
        
        May only be called on a dynamic object that was created by the
        attached Agent.
        '''
            
    def share(self, recipient):
        ''' Share the object with recipient, as identified by their GHID 
        (a public key fingerprint).
        '''
        
    def freeze(self):
        ''' Creates a static snapshot of the dynamic object. Returns a 
        new static AppObj instance. Does NOT modify the existing object.
        May only be called on dynamic objects. 
        '''
        
    def hold(self):
        ''' Binds to the object, preventing its deletion.
        '''
        
    def discard(self):
        ''' Tells the hypergolix service that the application is done 
        with the object, but does not directly delete it. No more 
        updates will be received.
        '''
            
    def delete(self):
        ''' Tells any persisters to delete. Clears local state. Future
        attempts to access will raise ValueError, but does not (and 
        cannot) remove the object from memory.
        '''
```

```python
class HypergolixLink:
    def __init__(self, app_token=None, *args, **kwargs):
        ''' Connects to the Hypergolix service.
        
        app_token is bytes-like and len(4). When resuming an existing
        application, the app_token must be passed to access any existing
        private objects for the application. It is unique for every 
        agent: application pair.
        '''
    
    @property
    def app_token(self):
        ''' Read-only property returning the current application's app 
        token.
        '''
    
    def register_api(self, api_id, object_handler):
        ''' Registers the embed with the service as supporting the
        passed api_id.
        
        object_handler is called for each *new* object shared with the 
        Agent currently logged in with the Hypergolix service that has
        declared itself as compliant with api_id.
        
        Applications may register multiple api_ids.
        
        Returns True.
        '''
        
    @property
    def whoami(self):
        ''' Return the GHID address of the currently active agent (their
        public key fingerprint).
        '''
        
    def get_object(self, ghid):
        ''' Retrieve the object identified by its GHID (hash address).
        
        Returns an AppObj.
        '''
        
    def new_object(self, *args, **kwargs):
        ''' Wraps AppObj.__init__, implicitly using self as the embed.
        '''
        
    def update_object(self, obj, *args, **kwargs):
        ''' Wrapper for obj.update.
        '''
        
    def share_object(self, obj, *args, **kwargs):
        ''' Wrapper for obj.share.
        '''
        
    def freeze_object(self, obj, *args, **kwargs):
        ''' Wrapper for obj.freeze.
        '''
        
    def hold_object(self, obj, *args, **kwargs):
        ''' Wrapper for obj.hold.
        '''
        
    def discard_object(self, obj, *args, **kwargs):
        ''' Wrapper for obj.discard.
        '''
        
    def delete_object(self, obj, *args, **kwargs):
        ''' Wrapper for obj.delete.
        '''
```

# Contributing

Help is welcome and needed. Unfortunately we're so under-staffed that we haven't even had time to make a thorough contribution guide. In the meantime:

## Guide

+ Issues are great! Open them for anything: feature requests, bug reports, etc. 
+ Fork, then PR.
+ Open an issue for every PR.
  + Use the issue for all discussion.
  + Reference the PR somewhere in the issue discussion.
+ Please be patient. We'll definitely give feedback on anything we bounce back to you, but especially since we lack a contribution guide, style guide, etc, this may be a back-and-forth process.
+ Please be courteous in all discussion.

## Project priorities

Note: these needs are specific to external contributors. Internal development priorities differ substantially.

+ Contribution guide
+ Code of conduct
+ Proper testing suite
+ Documentation
+ ```__all__``` definition for all modules
+ Clean up and remove unused imports

See also:

+ [Golix contributions](https://github.com/Muterra/doc-golix#contributing)
+ [Hypergolix demo contributions](https://github.com/Muterra/py_hypergolix_demos#contributing)

## Sponsors and backers

If you like what we're doing, please consider [sponsoring the project](https://opencollective.com/golix#sponsor) or [becoming a backer](https://opencollective.com/golix).

**Sponsors**

  <a href="https://opencollective.com/golix/sponsors/0/website" target="_blank"><img src="https://opencollective.com/golix/sponsors/0/avatar"></a>
  <a href="https://opencollective.com/golix/sponsors/1/website" target="_blank"><img src="https://opencollective.com/golix/sponsors/1/avatar"></a>
  <a href="https://opencollective.com/golix/sponsors/2/website" target="_blank"><img src="https://opencollective.com/golix/sponsors/2/avatar"></a>
  <a href="https://opencollective.com/golix/sponsors/3/website" target="_blank"><img src="https://opencollective.com/golix/sponsors/3/avatar"></a>
  <a href="https://opencollective.com/golix/sponsors/4/website" target="_blank"><img src="https://opencollective.com/golix/sponsors/4/avatar"></a>

-----

**Backers**

  <a href="https://opencollective.com/golix/backers/0/website" target="_blank"><img src="https://opencollective.com/golix/backers/0/avatar"></a>
  <a href="https://opencollective.com/golix/backers/1/website" target="_blank"><img src="https://opencollective.com/golix/backers/1/avatar"></a>
  <a href="https://opencollective.com/golix/backers/2/website" target="_blank"><img src="https://opencollective.com/golix/backers/2/avatar"></a>
  <a href="https://opencollective.com/golix/backers/3/website" target="_blank"><img src="https://opencollective.com/golix/backers/3/avatar"></a>
  <a href="https://opencollective.com/golix/backers/4/website" target="_blank"><img src="https://opencollective.com/golix/backers/4/avatar"></a>
  <a href="https://opencollective.com/golix/backers/5/website" target="_blank"><img src="https://opencollective.com/golix/backers/5/avatar"></a>
  <a href="https://opencollective.com/golix/backers/6/website" target="_blank"><img src="https://opencollective.com/golix/backers/6/avatar"></a>
  <a href="https://opencollective.com/golix/backers/7/website" target="_blank"><img src="https://opencollective.com/golix/backers/7/avatar"></a>
  <a href="https://opencollective.com/golix/backers/8/website" target="_blank"><img src="https://opencollective.com/golix/backers/8/avatar"></a>
  <a href="https://opencollective.com/golix/backers/9/website" target="_blank"><img src="https://opencollective.com/golix/backers/9/avatar"></a>
  <a href="https://opencollective.com/golix/backers/10/website" target="_blank"><img src="https://opencollective.com/golix/backers/10/avatar"></a>
  <a href="https://opencollective.com/golix/backers/11/website" target="_blank"><img src="https://opencollective.com/golix/backers/11/avatar"></a>
  <a href="https://opencollective.com/golix/backers/12/website" target="_blank"><img src="https://opencollective.com/golix/backers/12/avatar"></a>
  <a href="https://opencollective.com/golix/backers/13/website" target="_blank"><img src="https://opencollective.com/golix/backers/13/avatar"></a>
  <a href="https://opencollective.com/golix/backers/14/website" target="_blank"><img src="https://opencollective.com/golix/backers/14/avatar"></a>
  <a href="https://opencollective.com/golix/backers/15/website" target="_blank"><img src="https://opencollective.com/golix/backers/15/avatar"></a>
  <a href="https://opencollective.com/golix/backers/16/website" target="_blank"><img src="https://opencollective.com/golix/backers/16/avatar"></a>
  <a href="https://opencollective.com/golix/backers/17/website" target="_blank"><img src="https://opencollective.com/golix/backers/17/avatar"></a>
  <a href="https://opencollective.com/golix/backers/18/website" target="_blank"><img src="https://opencollective.com/golix/backers/18/avatar"></a>
  <a href="https://opencollective.com/golix/backers/19/website" target="_blank"><img src="https://opencollective.com/golix/backers/19/avatar"></a>
  <a href="https://opencollective.com/golix/backers/20/website" target="_blank"><img src="https://opencollective.com/golix/backers/20/avatar"></a>
  <a href="https://opencollective.com/golix/backers/21/website" target="_blank"><img src="https://opencollective.com/golix/backers/21/avatar"></a>
  <a href="https://opencollective.com/golix/backers/22/website" target="_blank"><img src="https://opencollective.com/golix/backers/22/avatar"></a>
  <a href="https://opencollective.com/golix/backers/23/website" target="_blank"><img src="https://opencollective.com/golix/backers/23/avatar"></a>
  <a href="https://opencollective.com/golix/backers/24/website" target="_blank"><img src="https://opencollective.com/golix/backers/24/avatar"></a>
  <a href="https://opencollective.com/golix/backers/25/website" target="_blank"><img src="https://opencollective.com/golix/backers/25/avatar"></a>
  <a href="https://opencollective.com/golix/backers/26/website" target="_blank"><img src="https://opencollective.com/golix/backers/26/avatar"></a>
  <a href="https://opencollective.com/golix/backers/27/website" target="_blank"><img src="https://opencollective.com/golix/backers/27/avatar"></a>
  <a href="https://opencollective.com/golix/backers/28/website" target="_blank"><img src="https://opencollective.com/golix/backers/28/avatar"></a>
  <a href="https://opencollective.com/golix/backers/29/website" target="_blank"><img src="https://opencollective.com/golix/backers/29/avatar"></a>