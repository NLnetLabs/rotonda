
Roadmap
=======

- ✅ work item complete
- 🦀 work item in progress
- 💤 work item not started yet


## `Rotonda`

### NEXT RELEASE 0.2: Big refactor, features similar to 0.1

- ✅ Move functionality from rotonda to rotonda-store
- 🦀 Integrate changes from Routecore 0.5 & Roto 0.3
- 💤 Simplify configuration format
- 💤 Limited MRT import for testing

### UNRELEASED 0.3: Collector functionality

- 💤 Provide passive external data sources to Roto
- 💤 On-disk storage to prevent growing memory use
- 💤 More ingress connectors (e.g. Kafka, mrt)
- 💤 Switch to daemon-base
- 💤 Store other AFI/SAFI types than unicast & multicast

### LATER RELEASES

- 💤 CLI for querying RIBs
- 💤 Emit BGP packets as events on OutputStream
- 💤 Egress modified/created BGP packets (on peering sessions)
- 💤 Refactor configuration: dynamic units reconfiguration with RESTCONF/yang+json
- 💤 More egress connectors (e.g. Kafka, Parquet, mrt, (timescale) RDBMS)
- 💤 BMP & BGP proxy
- 💤 BMP out stream
- 💤 Long-term file storage
- 💤 Create experimental global registry
- 💤 Snapshot/restore functionality
- 💤 RIB diff functionality
- 💤 Create experimental distributed Rotondas


## `Roto language`

### NEXT RELEASE 0.3

- ✅ Reimplemented as a compiled language using cranelift
- ✅ Improved parsing error messages
- ✅ Improved type checking error messages
- ✅ New type checker
- ✅ Syntax refinements
- ✅ User-defined filter-maps
- ✅ Basic types & operations (integers, bools)
- ✅ Domain-specific types (asn, ip addr, prefixes)
- ✅ Ready for hot-reload
- ✅ Record and enum types defined by Roto script
- 🦀 Runtime registering of types, functions & methods
- 🦀 Drop implementation for runtime types

### UNRELEASED 0.4

- 💤 Schemas and dynamic types in Roto functions
- 💤 Borrowed data in Roto
- 💤 Complete passive external data sources for RIBS
- 💤 Create dev documentation
- 💤 Create reference-level documentation 
- 💤 Create namespaces / modules system

### LATER RELEASES

- 💤 implement more passive external data sources
- 💤 implement active external data sources: RTR
- 💤 create user-configurable graph DSL for units
- 💤 create user-configurable query DSL


## `Routecore`

### NEXT RELEASE 0.5

- ✅ Prefix types in `inetnum`
- ✅ Route record example types
- ✅ BGP/BMP types for parsing
- ✅ Create BGP packet editor & route workshop
- ✅ Partial FlowSpec parser
- ✅ Best path selection
- 🦀 Revamped AFI/SAFI with AddPath support

### UNRELEASED 0.6

- 💤 Optimizations

### LATER RELEASES

- 💤 BGPsec parser
- 💤 More AFI/SAFI types
- 💤 Move BMP FSM from Rotonda to Routecore
- 💤 Refactor BGP FSM with BoQ in mind
- 💤 Type-aware wire format path attributes


## `Rotonda-store`

### NEXT RELEASE 0.4

- ✅ Multi-unique ID implemented (replacing MergeUpdate from rotonda 0.1)
- ✅ Best path selection with caching
- ✅ Optimization of route status using roaring bitmaps
- 🦀 Optimizations: better hashing, cache padding, etc.

### UNRELEASED 0.5

- 💤 On-disk storage integration


## `Rotoro`

### MVP

- 🦀 Design wire protocol and select a layout (AVRO?)
- 💤 Create experimental de/serialization
- 💤 Experimental integration in `rotonda`