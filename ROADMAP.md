
Roadmap
=======

- ✅ work item complete
- 🦀 work item in progress
- 💤 work item not started yet
- ↑ updated version

General Features
================

RELEASED

rotonda 0.1
rotonda-fsm 0.1
roto 0.2
routecore 0.4
rotonda-store 0.3
rotoro -

- ✅ BMP listener
- ✅ BMP proxy
- ✅ single-threaded RIB implementation
- ✅ lock-free, multi-threaded RIB implementation
- ✅ client-specific runtime application with BMP ingress and RIBs
- ✅ `roto` filtering language
- ✅ BGP passive speaker
- ✅ daemonize
- 🦀 ~~BGP limited active speaker~~ POSTPONED
- ✅ Documentation
- ✅ Packaging

NEXT RELEASE

↑ rotonda 0.1.1
  rotonda-fsm 0.1
↑ roto 0.2.x
  routecore 0.4
  rotonda-store 0.3
  rotoro -

- 🦀 implement FilterMap (user-defined rx/tx and RIB storage type)
- 🦀 implement passive external data source: RIBs

NEXT RELEASES

- 💤 support more AFI/SAFIs, e.g. FlowSpec, L2VPN, MPLS
- 💤 emit BGP packets as events on OutputStream
- 💤 egress modified/created BGP packets (on peering sessions)
- 💤 RIB split over in-memory and on-disk.
- 💤 refactor configuration: dynamic units reconfiguration with RESTCONF/yang+json
- 💤 implement active external data sources: RTR
- 💤 more ingress connectors (e.g. Kafka, MQTT, mrt)
- 💤 more egress connectors (e.g. Kafka, Parquet, mrt)
- 💤 create plugin system.
- 💤 query engine over (split) RIBs.
- 💤 Rotonda shell
- 💤 Distributed `rotonda`-`rotonda` setup with `rotoro` (experimental)


Development per Component
=========================

## `Roto language`

RELEASED 0.2

- ✅ language lay-out
- ✅ EBNF scheme
- ✅ create experimental parser
- ✅ create experimental compiler
- ✅ create experimental virtual machine
- ✅ implement all methods on `roto` types
- ✅ integrate into `rotonda`
- ✅ implement FilterMap (user-defined rx/tx)
- 💤 ~create user-friendly error messaging~
- 🦀 ~extensive stress testing
- 🦀 create manual-like docs
- ✅ create high-level overview documentation

UNRELEASED 0.2.x

- 🦀 complete passive external data sources for RIBS

NEXT RELEASES

- 💤 implement more passive external data sources
- 💤 implement active external data sources: RTR
- 🦀 create reference-level documentation 
- 💤 create namespaces / modules system
- 💤 create user-configurable graph DSL for units
- 💤 create user-configurable query DSL
- 💤 create dev documentation


## `Rotonda-fsm`

RELEASE 0.1

- ✅ BMP state machine
- ✅ BGP state machine
- ✅ BGP passive speaker (session management)

UNRELEASED 0.2

- 🦀 BGP active speaker


## `Routecore`

RELEASE 0.4

- ✅ prefix types
- ✅ route record example types
- ✅ BGP/BMP types for parsing

UNRELEASED 0.5

- 🦀 create BGP packet builder
- 💤 partial FlowSpec parser

NEXT RELEASES

- 💤 BGPsec parser


## `Rotonda`

RELEASE 0.1

- ✅ setup `tokio` skeleton with logging etc.
- ✅ setup BMP listener
- ✅ REST API setup
- ✅ MQTT OutputConnector
- ✅ integrate BGP passive listener (BGP EventSource)
- ✅ virtual RIB experimental implementation
- ✅ installation and usage documentation 
- ✅ limited packaging
- ✅ limited `systemd` integration

UNRELEASED 0.2

- 🦀 passive external data sources: RIBs
- 🦀 implement FilterMap (user-defined rx/tx and RIB storage type)

NEXT RELEASES

- 💤 emit BGP packets as events on OutputStream
- 💤 egress modified/created BGP packets (on peering sessions)
- 🦀 refactor configuration: dynamic units reconfiguration with RESTCONF/yang+json
- 🦀 implement tracing
- 💤 RIB split over in-memory and on-disk
- 💤 namespaces / modules support
- 💤 more ingress connectors (e.g. Kafka, mrt)
- 💤 more egress connectors (e.g. Kafka, Parquet, mrt, (timescale) RDBMS)
- 🦀 BMP proxy
- 💤 create experimental global registry
- 💤 snapshot/restore functionality
- 💤 RIB diff functionality
- 🦀 `systemd` integration
- 💤 create experimental distributed units


## `Rotoro`

MVP

- 🦀 design wire protocol and select a layout (AVRO?)
- 💤 create experimental de/serialization
- 💤 experimental integration in `rotonda-runtime`


## `Rotonda-store`

0.3

- ✅ stabilize single-threaded store
- ✅ stabilize multi-threaded store
- ✅ robust error handling (get rid of all the unwraps)

NEXT RELEASES

- 🦀 stabilize API
- 🦀 benchmarks matrix IPv4/IPv6 `local_array`/`local_vec`
- 🦀 unit tests & acceptance tests matrix
- 💤 optimizations: implement QSBR (replace Ref Counting on update)
- 💤 optimizations: better hashing, cache padding, etc.
- 💤 fuzzer IPv4/IPv6
