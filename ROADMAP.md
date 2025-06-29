
Roadmap
=======

- ✅ work item complete
- 🦀 work item in progress
- 💤 work item not started yet


## `Rotonda`

### 0.4: Collector functionality

- ✅ Provide passive external data sources to Roto, specifically with Routinator.
- ✅ ingress connectors mrt & rtr

### NEXT RELEASE 0.5

- 🦀 On-disk storage to prevent growing memory use
- 💤 More ingress connectors (e.g. Kafka)
- 🦀 CLI for querying RIBs
- 💤 Emit BGP packets as events on OutputStream
- 🦀 Refactor configuration: dynamic units reconfiguration with RESTCONF/yang+json

### LATER RELEASES

- 💤 Parse, act on and store more AFI/SAFI types, particularly FlowSpec
- 💤 Switch to daemon-base
- 💤 Egress modified/created BGP packets (on peering sessions)
- 💤 More egress connectors (e.g. Kafka, Parquet, mrt, (timescale) RDBMS)
- 💤 BMP & BGP proxy
- 💤 BMP out stream
- 🦀 Long-term file storage
- 💤 Create experimental global registry
- 💤 Snapshot/restore functionality
- 💤 RIB diff functionality
- 💤 Create experimental distributed Rotondas (Sharding)


## `Roto language`

### RELEASE 0.6

- ✅ Create dev documentation
- ✅ Create reference-level documentation
- ✅ accomodate querying RIBs as external data source
- ✅ Global variables
- ✅ Create namespaces / modules system

### LATER RELEASES

- 🦀 implement lists
- 🦀 Schemas and dynamic types in Roto functions


## `Routecore`

### NEXT RELEASE 0.6

- 🦀 More AFI/SAFI types, particularly FlowSpec
- 🦀 Move BMP FSM from Rotonda to Routecore
- 💤 Refactor BGP FSM with BoQ in mind
- 🦀 more efficient path attributes data structure

### LATER RELEASES

- 💤 BGPsec parser


## `Rotonda-store`

### NEXT RELEASE 0.5

- ✅ On-disk storage integration
- 🦀 Optimizations: better hashing, cache padding, etc.
- 🦀 Storage for more AFI/SAFI types

### LATER RELEASES

- 💤 Query Plans
- 💤 Indexing (dynamic and static)

## `Rotoro`

### MVP

- 🦀 Design wire protocol and select/design/implement a layout
- 🦀 Create experimental de/serialization
- 💤 Experimental integration in `rotonda`
