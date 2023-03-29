### `Rotonda 0.1` *"The Hard Coded"*

# Unit Structures

## RIB

A Routing Information Base (`RIB`) is a collection of prefixes with attached records. The prefix is the primary key of the collection, although the collection can also be queried efficiently on covering/covered prefixes. The type of the record will by default be the Path Attributes of a BGP message. Incoming BGP packets will typically be stored under all the prefixes in the NLRI of that BGP message.

A `RIB` first filters the incoming packets through a programmable `filter-map` before storing the packet into the `RIB`. This `filter-map` is user-programmable and can not only filter out messages, but can also modify the attributes of the incoming packets, or even create a new record for a given prefix, based on the incoming packet, hence the name `filter-MAP`.

The `RIB` content can be queried through a built-in HTTP interface. Currently Rotonda has a REST HTTP interface that allows for querying the `RIB` based on (covered, covering, exactly matching) prefixes, as well as communities present in the record.

Every packet coming into the `RIB` that passes the `RIB`'s `filter-map` will, apart from being stored, be emitted on its output.

We will also refer to a `RIB` as a "physical `RIB`", to distinguish it from a Virtual `RIB` (see below).

Currently Rotonda features one hard-coded physical `RIB`, that has one programmable `filter-map` in front of it, called **`pre`**. This `RIB` has two event sources, a BMP ingress and a BGP session ingress.

The current storage strategy of a `RIB` is to consider the tuple `(prefix, AS_PATH, peer id)` as unique for a BMP message, and the tuple `(prefix, AS_PATH, BGP Identifier)` as unique for a BGP message. This means that any incoming exploded-per-prefix BGP message that has the same 3-tuple mentioned above as an entry in the `RIB`, will **overwrite** that record in the `RIB`.

## VirtualRIB

A Virtual `RIB` acts like a `RIB`, but does not actually store anything. Instead it queries its (west) adjacent physical `RIB` for the stored (tuple, record) pair and then applies another `filter-map`, called **`post`**, to it.

Just like a physical `RIB`, however, it has a HTTP interface, that allows users to query the `RIB`'s content. It also emits the (modified) record that passes its `filter-map` on its output.

Currently Rotonda has one hard-coded virtual `RIB` that sits on the east side of the physical `RIB`.

## EventSource

An event source is a unit that takes input from systems external to Rotonda, such as a BMP or a BGP session and converts it into an internal Rotonda record-type.

Currently Rotonda features one BMP EventSource and one BGP EventSource unit.

## EventEmitter

An Event Emitter is a unit that takes an internal Rotonda record on its input and converts that into a format that a specifics external system can process further.

Currently Rotonda comes with a (limited) BGP Update EventEmitter unit and a JSON file output EventEmitter.

## DataSource

A Data Source is a dynamic source of data external to Rotonda that can be used in a `filter-map` to base filtering and modification decisions on, e.g. a CSV file. Data sources will be updated by Rotonda based on the transport of the data source. Data sources are read-only. A `filter-map` can invoke multiple data sources.

Currently Rotonda features two places to invoke data sources, in the **`pre`** `filter-map` in the physical `RIB`, and in the **`post`** `filter-map` in the virtual `RIB` next to it. The data sources can be `CSV` or `JSON` formatted files.
