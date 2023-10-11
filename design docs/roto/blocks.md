# Blocks

Blocks are the primitives that the `Rotonda` user defines and combines to create a `Rotonda` application. 

Each block has a type itself, e.g. `Unit`, `Filter`, etc. and most of them have a sub-type, e.g. a `Unit` can have `PhysicalRib` as sub-type. The way they can be combined depends on the types of the blocks involved.

## Module block

A module is the rug that ties the room together. It acts as a namespace. It doesn't have sub-types. Any other block type can be placed in a module. Consumers of a block in a module should prepend the block name with the dotted module name, e.g. `my-module.my-rib`.

## Unit Block

A `Unit` is the block that defines the flow of the data from west to east and the storage along the way. A `Rotonda` application should have at least one unit for it to function.

A `Unit` has an API that can be used by the `Rotonda` user to query the contents of the unit.

### Unit: Rib

A `RIB` is a unit that has a typed input, that receives a stream of data for that input, a `filter-map`, that can take that typed input, create a flow-decision (accept/reject) based on the received data and one or more external data source, transform the typed input into another typed piece of data and store it in a prefix store. Furthermore it can create a stream of typed output data.

(stream -> type A) -> (filter-map(type A) -> type B) -> store -> type B

```
// The `config` variable is an anonymous record, whose type will be converted
// by Roto to `PhysicalRibConfig`. 
unit rib-loc: PhysicalRib with config {
    merge_strategy: {
        key: (Route::peer_id, Route::as_path),
        overwrite: MOST_RECENT
        // other options: 
        // MOVING_WINDOW_COUNT(5)
        // MOVING_WINDOW_TIME_WEEKS(1)
    },
    filter_map: peer-filter,
    contains: Route
};

// All Roto `*Config` types have a `default()` method that copies the default
// values into the `config` variable of the unit.
rib-in-config = PhysicalRibConfig.default();
unit rib-in with rib-in-config;

unit rib-mon: VirtualRib with config {
    filter_map: my-asn-filter,
    contains: AsPathsSeen {
        as_path: AsPath,
        peer_as: Asn,
        timestamp: TimeStamp
    }
};
```

### Unit: Connector

A `Connector` is a `unit` that can transform a stream from an external data source into another stream of data that a `Rib` unit can understand on the west side of a `Rotonda` application. Some connector types can also transform data on the east side of the `Rotonda` application and transform it back into a stream that the external data source can understand.

```
unit bmp-1: BmpConnector with config {
    proxy: true
    filter: unicast-v4-v6-filter,
};

unit bgp-1: BgpConnector with config {
    // WestEastSplit | WestOnly | EastOnly
    topology: WestEastSplit,
    listen: 192.168.178.49:179,
    west_pure_filter: unicast-v4-v6-filter,
    // A Rotonda user should be able to specify a per-bgp-session filter, or the filter should have the capability to differentiate between
    // session capabilities.
    east_filter_map: FILTER.none,
};
```

Another `Connector` type could be `PcapConnector`, a Connector that knows how to parse a `pcap` that contains either BGP or BMP, and has some heuristic process to figure out what's in the `pcap` and outputs that as a `Roto TypeValue` in the pipeline.

## Filter Block

A `Filter` is a part of a `Unit`. There is the `PureFilter` type that only can make a `Accept` or `Reject` decision based on a typed input (and one or more data sources) and there is the `FilterMap` type that can also transform the typed input data into another type of data.

### Filter: PureFilter

### inputs

- `rx: BgpUpdateMessage | BmpMessage` (read-only)
- `tx|rx_tx: BgpUpdateMessage | BmpMessage` (read-only)
- `(datasources)`

### Outputs

- `AcceptReject`
- `OutputStream`

### Sections

- `define`
- `term`
- `apply`

### Filter: FilterMap

### inputs

- `rx`
- `tx|rx_tx`
- `(datasources)`

### outputs

- `AcceptReject`
- `tx|rx_tx`
- `OutputStream`

### sections
- `define`
- `term`
- `action`
- `apply`

## DataSource Block

A data source is a block that describes an external read-only data source to be used in a `Filter` block. Note that a `Rotonda` `PhysicalRib` is also considered an external data source, since a `Filter` block doesn't know in which unit it is going to be used.

### DataSource: PhysicalRib

Only a physical Rib can be consulted as a external data source, a virtual Rib can't and will not work.

```
data-source ds-rib-loc: Rib with config {
    related: rib-loc
};
```

### DataSource: Table
```
data-source customers-list: Table with config {
    source_file: "./file_name",
    check_update_interval: HOURS(1),
    mode: ACTIVE, // or PASSIVE
    contains: TableRow {
        customer_id: String,
        prefix: Prefix,
        allowed_asns: [Asn]
    }
};
```

## OutputStream Block

An output stream is a side-channel that a `Filter` block can use to send data to.

### OutputStream: `Mqtt`

```
output-stream mqtt: Mqtt with config {
    server: {
        host: <HOSTNAME>,
        port: <PORT>
    },
    client_id: "<CLIENT_ID>",
    rx: Message {
        message: String,
        from_asn: Asn
    }
};

output-stream logging: Log with config {
    level: INFO,
    target: STDERR,
    facility: DAEMON,
    file: "./rotonda.log"
};
```
### OutputStream: `Kafka`
### OutputStream: `File`
### OutputStream: `Log`

# Instance Types

An `Instance` block is the over-arching block that defines the `Rotonda` application, by specifying a way to wire up units.

```
instance rotonda-1: Daemon with config {
    pipelines = [
        Connector(bgp-1) -> Rib(rib-in) -> Rib(rib-loc) -> Rib(rib-out) -> Connector(bgp-1),
        Rib(rib-loc) -> Rib(rib-mon)
    ];
};
```

Beyond the MVP we want to create a CLI that allows a `Rotonda` user to build a pipeline from separate elements in an iterative process in a REPL or CLI.

# Merge Strategies

A `merge-strategy` consists of two phases: 

- Establish uniqueness of a Route through a configurable hash, defaults to `(peer_id, AS_PATH)`.
- Overwrite strategy: `upsert` single entry, moving window over a chronological set of entries, set by count or set by time received.

the merge strategy is a property of a RIB unit.