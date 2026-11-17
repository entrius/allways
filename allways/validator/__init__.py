"""The validator, rebuilt as a set of single-purpose components.

Each component runs on its own thread and owns one job: reading chain state (``world``), streaming
program events (``feed``), sending deadline-bound votes (``actuator``), scoring (``scoring``), relaying
bonds per hub (``relay``), serving the product (``serving``) and exporting round results (``export``).

They share nothing mutable. They talk through three primitives in ``runtime.bus``:
``Latest`` (the newest snapshot of something, e.g. the World), ``Topic`` (a stream, e.g. decoded
events) and the actuator's ``DeadlineQueue`` (work that must land before a deadline). The
``Supervisor`` runs them and restarts the process only when a critical component stalls.

Hubs are data, not code paths: ``hubs.HUBS`` lists every backing hub, and per-hub work (relay,
pools, eligibility) iterates it. Adding a hub is a registry entry plus its vault adapter.
"""
