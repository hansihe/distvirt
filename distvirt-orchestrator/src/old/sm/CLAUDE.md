# State Machine Guidelines

## SM outputs are the only interface

The SM is the sole authority on state transitions. All side effects in the namespace layer (pod registration, worker commands, events, condition changes) must be driven by SM outputs — never by peeking at SM state after stepping, and never hardcoded around a `step()` call.

If the namespace layer needs to do something in response to a transition, the SM should emit an output for it. If there's no matching output variant, add one.
