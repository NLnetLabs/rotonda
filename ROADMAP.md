
Roadmap
=======

- ✅ work item complete
- 🦀 work item in progress
- 💤 work item not started yet


## `Rotonda`

### 0.3: Collector functionality

- 💤 Provide passive external data sources to Roto, specifically with Routinator.
- 💤 On-disk storage to prevent growing memory use
- 💤 More ingress connectors (e.g. Kafka, mrt (finalise), rtr)

### LATER RELEASES

- 💤 Switch to daemon-base
- 💤 Store other AFI/SAFI types than unicast & multicast
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
- 💤 Create experimental distributed Rotondas (Sharding)


## `Roto language`

### NEXT RELEASE 0.4

- 🦀 Create dev documentation
- 🦀 Create reference-level documentation
- 💤 accomodate querying RIBs as external data source
- 💤 Schemas and dynamic types in Roto functions
- 💤 Global variables
- 💤 Create namespaces / modules system

### LATER RELEASES

- 💤 implement more passive external data sources
- 💤 create user-configurable graph DSL for units
- 💤 create user-configurable query DSL


## `Routecore`

### NEXT RELEASE 0.6

- 💤 Optimizations

### LATER RELEASES

- 💤 BGPsec parser
- 💤 More AFI/SAFI types
- 💤 Move BMP FSM from Rotonda to Routecore
- 💤 Refactor BGP FSM with BoQ in mind
- 💤 Type-aware wire format path attributes


## `Rotonda-store`

### NEXT RELEASE 0.5

- 🦀 On-disk storage integration
- 🦀 Optimizations: better hashing, cache padding, etc.

### LATER RELEASES

- 💤 Query Plans
- 💤 Indexing (dynamic and static)

## `Rotoro`

### MVP

- 🦀 Design wire protocol and select a layout (AVRO?)
- 💤 Create experimental de/serialization
- 💤 Experimental integration in `rotonda`
