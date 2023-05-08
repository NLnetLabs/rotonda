# Persistence Layer

Ideally, we're keeping 3 weeks worth of routing in memory. Our assumption is that this serves the 80% of use-cases. Beyond 3 weeks we would like to store the routing data somewhere safe, but way slower to retrieve, but still queryable in the same way the 'live' data that sits in memory would queryable.

I can see a number of different strategies to store the 'slow' data:

### Unstructured Local files

Lots of unstructured files. `Rotonda` would keep an in-memory index of what lives where, like each prefix in the `rotonda-store` would have a reference to files and offsets into those files.

### Structured local files

`Parquet` files jump to mind, but it could also be `sqlite` or any other structured format.
These files would be directly queryable, without `Rotonda` having much knowledge about what goes where.
Probably `Rotonda` still needs some information around time-windows of each files, and which prefixes live
in them.

### External KV store

`TiKV` or something like `Redis`, or `RocksDB`. `Rotonda` keeps an in-memory index of what lives where, still, very much like unstructured local files.

### Time-series database

Like structured files, but `Rotonda` would even have to have less information about what goes where, it would basically query the time-series database for that.
Options under consideration are: `Apache Druid`, `Clickhouse`, `Timescale`, `InfluxDB`.

### Generic SQL database

`Postgres`
