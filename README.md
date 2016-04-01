# hypergolix

Hypergolix is a "full client" for the Golix protocol. That doesn't mean anything yet, so think of it as "docker for digital identities". If you're creating any internet-enabled application, Hypergolix is an alternative to rolling your own account management system. It is designed to give the person/program operating the client full, end-to-end cryptographic control over information sharing and retention.

It does not require application-level cryptographic code, and is available via IPC on the local machine (specifically through pipes, and potentially through localhost in the future).

# Todo (no particular order)

+ Add a frame guid history to DynamicObject
+ Create a format for using dynamic objects to perform keyshares
+ Document key ratcheting mechanism
    + Side note: using the previous frame guid as a salt effectively creates a key expiration mechanism -- if you magically find / break the original exchange, you probably still cannot reconstruct the history chain required to ratchet the whole thing.
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