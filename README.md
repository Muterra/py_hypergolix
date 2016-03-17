# hypergolix

Hypergolix is a "full client" for the Golix protocol. That doesn't mean anything yet, so think of it as "docker for digital identities". If you're creating any internet-enabled application, Hypergolix is an alternative to rolling your own account management system. It is designed to give the person/program operating the client full, end-to-end cryptographic control over information sharing and retention.

It does not require application-level cryptographic code, and is available via IPC on the local machine (specifically through pipes, and potentially through localhost in the future).

# Todo (no particular order)

+ Implement a key ratchet for dynamic objects instead of requiring new keyshares for each frame
    + Use the _legroom parameter as a hint for when to issue a new key
    + Everything else is done through a ratchet
+ Create a format for using dynamic objects to perform keyshares

## Done

+ Write enough code to have a reason to have a todo list