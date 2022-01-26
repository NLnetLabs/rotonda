# `Rotoro`

## Why yet another protocol?

One of the design ideas of `Rotonda` is that users can mix and match few components into one runtime. When they do they won't need any additional protocols or even serialization: data can be simply passed around between components as Rust data-structures. A related design idea however is that any of the runtimes could live on their own as a separate application. When this is the case we will need serialization and a transport to communicate between the separate runtimes.

## So why not BGP?

The large majority of this data will be BGP data, so the obvious question is: "why can't we just use BGP as the protocol to communicate between runtimes?". After all, BGP handles connection state, as well as data. The thing is that we decided to disassemble BGP data upon arrival in a channel transformer at the very arrival in `Rotonda`.

Disassembling BGP serves two purposes, first it allows us to keep the `rotonda-store` as simple as possible, with only `(prefix, meta-data)` pairs. BGP packets are not structured this way: one packet can carry information for completely disparate prefixes (both IPv4 and IPv6). To store BGP packets more or less "as is" would require a relational storage model with extended query capabilities, e.g. a SQL database.

Even if we think this would be a good idea, we would wind up with a pretty complex (de)serialization scheme, going from a Rust data-structure to a BGP packet and back. I haven't tried, but I bet that capturing a BGP packet in one type will be well nigh impossible. As a consequence inbound and outbound connections would have to be tied to all kinds of different types and ordering of types.

## So why not RTR?

Another option would be to (re-)use RTR as a internal protocol for `Rotonda`. RTR is keyed on prefixes (it's RPKI data), it can transfer a complete state from a table, and it can transfer diffs from one table to another. Furthermore the `rtrtr` crate has the concept of units, that can be connected to each other. The biggest dis-advantage would be that RTR is explicitly meant to be used for RPKI data only. Another issue is that our internal protocol also needs to be able to stream single `(prefix, meta-data)` pairs (presumably) and that it needs a few other message types, not based on `(prefix, meta-data)` pairs (see Data Types down below).

Ideally, our `rotoro` protocol will be a superset of RTR and we can re-use the `rtrtr` crate for our internal protocol.

## Data types

- Full state and diffs for `(prefix, meta-data)` pairs, the data that is stored in `rotonda-store`.
- Configuration data
- Housekeeping (control/state) messages

## Possible formats and transports

Even if we are to partly re-purpose RTR, we will have to decide on a (or more?) transports.

### Raw TCP stream

The transport that RTR uses is TCP and TLS-over-TCP. Pros are that it has few dependencies, is low-overhead, yet has the connection management that we need. Another advantage is that the serialization overhead can be pretty minimal, since it's PDUs would be all binary.

Cons are that we would have to invent and implement a complete binary format. The payload of RTR is completely geared towards RPKI data, so we would have to invent a new format for the (meta-)data.

### Web-Sockets (WS)

Web-sockets offer more connection management facilities than bare TCP and the obvious format for the meta-data would be JSON. Web-sockets would also allow for easy communication with other applications (outside of `Rotonda`).
There's good support for web-sockets in Rust.

The cons are the overhead and the need for extensive (de-)serialization, both in terms of defining/implementing and taking up CPU/memory resources. Another con is that web-sockets will introduce more dependencies.

Finally, the argument of being able to connect to third-party, external application is a bit moot, since we're talking about an internal protocol here. Having an explicit layer that acts as a gateway between `Rotonda` and the outside world probably is a good idea and having a different protocol to the outside world is therefore a good idea?

### MQTT

MQTT is a well-defined, routable pub/sub messaging protocol. There's good support for relaying it anywhere through a broker (RabbitMQ, ZeroMQ, etc.). MQTT can use TCP, TLS/TCP and web-sockets as transport. It offers the typical things you'd need for a pub/sub system, e.g. managing and tracking subscriptions, QoS (At-most-once, At-least-once, Exactly-once).

The same discussion as for web-sockets applies here. Additionally, I think, it's overkill for `Rotonda` to use MQTT internally.
