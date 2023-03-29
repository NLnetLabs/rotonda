
Roadmap
=======

- ✅ work item complete
- 🦀 work item in progress
- 💤 work item not started yet

General Features
================

In order of priority:

MVP

- ✅ BMP listener
- ✅ single-threaded RIB implementation
- ✅ lock-free, multi-threaded RIB implementation
- ✅ client-specific runtime application with BMP ingress and RIBs
- 🦀 `roto` filtering language
- 🦀 BGP passive speaker
- 🦀 BGP limited active speaker
- 🦀 Documentation
- 💤 Packaging

NEXT RELEASES

Each item may constitute a new release

- 🦀 ingress connectors (e.g. Kafka, MQTT)
- 🦀 egress connectors (e.g. Kafka, MQTT)
- 💤 create plugin system.
- 💤 `mrt` file connector from RIBs.
- 💤 egress connector for time-series database.
- 🦀 query engine REST API
- 💤 query engine CLI
- 🦀 historical records storage + snapshotting ("3 weeks of data")
- 🦀 Distributed `rotonda`-`rotonda` setup with `rotoro` (experimental)


Development per Component
=========================


## `Roto language`

MVP

- ✅ language lay-out
- ✅ EBNF scheme
- ✅ create experimental parser
- ✅ create experimental compiler
- ✅ create experimental virtual machine
- 🦀 implement all methods on `roto` types
- 🦀 Integrate into `rotonda-runtime`
- 💤 create user-friendly error messaging
- 🦀 extensive stress testing
- 💤 create high-level overview documentation

NEXT RELEASES

- 💤 create user-configurable graph DSL for units
- 💤 create user-configurable query DSL
- 💤 create dev documentation
- 💤 create manual-like docs
- 💤 create reference-level documentation 


## `Rotonda-fsm`

MVP

- ✅ BMP state machine
- 🦀 BGP state machine
- 🦀 BGP passive speaker (session management)
- 🦀 BGP minimal active speaker

NEXT RELEASES

- 💤 more BGP active speaker features

## `Routecore`

MVP

- ✅ prefix types
- ✅ route record example types
- ✅ BGP/BMP types for parsing
- 🦀 create minimal BGP packet builder: Withdrawal for one prefix
- 🦀 create minimal BGP packet modifier: Add Community

NEXT RELEASES

- 💤 create BGP packet builder
- 💤 BGPsec parser


## `Rotonda-runtime/Rotonda-units`

MVP

- ✅ setup `tokio` skeleton with logging etc.
- ✅ setup BMP listener
- ✅ REST API setup
- ✅ MQTT OutputConnector
- 🦀 integrate BGP passive listener (BGP EventSource)
- 🦀 integrate BGP limited speaker (BGP EventEmitter)
- 🦀 installation and usage documentation 
- 🦀 limited packaging
- 🦀 file OutputConnector

NEXT RELEASES

- 💤 user-configurable RIBs experimental implementation
- 💤 virtual RIB experimental implementation
- 💤 create experimental global registry
- 💤 Kafka connector
- 💤 split-off EventSource, OutputConnector units
- 💤 split-off EventEmitter, OutputConnector units
- 💤 time-series DB OutputConnector
- 💤 keep history window (serials)
- 💤 snapshot/restore functionality
- 💤 `systemd` integration
- 💤 implement tracing
- 💤 split `rotonda-runtime` into units
- 💤 create experimental distributed units


## `Rotoro`

NEXT RELEASES

- 💤 design wire protocol or select a layout (AVRO?CBOR?)
- 💤 create experimental de/serialization
- 💤 experimental integration in Rotonda-runtime


## `Rotonda-store`

MVP

- ✅ stabilize single-threaded store
- ✅ stabilize multi-threaded store
- ✅ robust error handling (get rid of all the unwraps)

NEXT RELEASES

- 🦀 stabilize API
- 🦀 benchmarks matrix IPv4/IPv6 `local_array`/`local_vec`
- 🦀 unit tests & acceptance tests matrix
- 💤 optimizations: better hashing, cache padding, etc.
- 💤 fuzzer IPv4/IPv6
