[![Hypergolix logo](/assets/hypergolix-logo.png)](https://www.hypergolix.com)

Hypergolix is a local background service that makes IoT development effortless. It uses the [Golix protocol](https://github.com/Muterra/doc-golix) to provide all data with strong, end-to-end encryption. It automatically handles account management, encryption, and security for you. Once logged in, Hypergolix is available to applications via IPC. Secret key material is entirely handled within Hypergolix; encrypted content creation, retention, sharing, and synchronization are all handled through the serveice. Hypergolix accelerates secure application development, especially for embedded and connected devices, without compromising sociability.

**Hypergolix requires ```python >= 3.5.1```.**

# Todo (no particular order)

+ Reconcile AppObj and DynamicObject/StaticObject
+ Add a frame ghid history to DynamicObject
+ Create a format for using dynamic objects to perform keyshares
+ Document key ratcheting mechanism
    + Side note: using the previous frame ghid as a salt effectively creates a key expiration mechanism -- if you magically find / break the original exchange, you probably still cannot reconstruct the history chain required to ratchet the whole thing.
+ Document that (currently, until protocol change) using a link as a dynamic frame breaks the ratchet and is, generally speaking, a very dangerous decision.
+ Document golix-over-websockets defacto protocol
+ Add callbacks for updates to ```DynamicObject``` (for application use)

## Done

+ Write enough code to have a reason to have a todo list
+ Implement an axolotl-like key ratchet for dynamic objects instead of requiring new keyshares for each frame
    + Use the _legroom parameter as a hint for when to issue a new key
    + Everything else is done through a ratchet
    + Note that not only is this self-synchronizing, but it also allows dynamic objects that have been shared with multiple parties to remove access to future frames.
+ Improve / create key ratchet healing