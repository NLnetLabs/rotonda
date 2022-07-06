# Rotonda Filters - The requirements

## Hard Requirements

- filter on all BGP attributes that are available after parsing a BGP message.
- filter on configurable meta-data, i.e. `router-id`.
- dynamic runtime adding/removing/modifying filters.
- re-route BGP messages based on filters to user-specified/created RIBs.
- use same filters for both incoming streams and routes in RIBs.
- read prefix-lists from external sources, e.g. files, r(o)t(o)r(o).

## Soft Requirements

- be as unoriginal as possible.

## Daft Attempts

### Filters

A filter-map consists of `terms` and a `filter`.

A `term` is a single statement, that consists of one or more conditions that are tested sequentially.
If all of them are true, then the term executes the actions in the `then` clause. If one of them are false, then the actions in the `or` (if available) clause are executed. The last action in both the `then` and `or` clauses must be the `reject` or the `accept` return-action. 
If one of the `then` or `or` clause are missing then an `accept` action is returned by default. A `term` is agnostic to the actual RIB it is being filtered against, so it cannot specify the RIB. It can however specify another (external) data-structure, like a table or a prefix-list, to match against by specifying a `with` clause.

```
// A fairly simple example of a term
// with a defined variable.
define last-as-64500-vars {
    last_as_64500 = AsPathFilter { last: 64500 };
}

term no-as-64500-until-len-16 for route {
        with {
            last-as-64500-vars;
        }
        from {
            prefix-filter 0.0.0.0/0 upto /16;
            protocol bgp {
                route.as-path.matches(last-as-64500);
            };
            protocol internal {
                router-id == 243;
            };
        }
        then {
            // a side-effect is allowed, but you can't
            // store anywhere in a term.
            send-to stdout;
            reject;
        }
    }
}
```

```
// there is nothing special about a namespace called
// `global`.
module global {
    define our-as for our_asn {
        our-as = AsPathFilter { last: our_asn };
    }

    term drop-ibgp for route {
        with {
            our-as;
        }
        from {
            # drop our own AS
            route.bgp.as-path.matches(our_asn);
        }
        then {
            send-to standard-logger ibgp;
            reject;
        }
    }
}
```

```
rib global.rov as rov-rib {
    prefix: Prefix,
    max_len: u8,
    asn: Asn,
}

module rpki {
    define rov-rib-vars for route {
        use rov-rib;
        found_prefix = rov-rib.longest_match(route.prefix);
    }

    term rov-valid for route {
        with rov-rib-vars;
        // A rule can have multiple with statements,
        // either named or anonymous.
        // with {
        //    max_len = 32;
        // }
        from {
            found_prefix.matches;
            route.prefix.len <= found_prefix.max_len;
            route.prefix.origin-asn == found_prefix.asn;
        }
        then {
            route.bgp.communities.add(1000:1);
            accept;
        }
    }

    term rov-invalid-length for route {
        with rov-rib-vars;
        from {
            found_prefix.matches;
            route.prefix.len > found_prefix.max_len;
            route.prefix.origina-asn == found_prefix.asn;
        };
        then {
            route.bgp.communities.add(1000:6);
            accept;
        }
    }

    term rov-invalid-asn for route {
        with rov-rib-vars;
        from {
            found_prefix.matches;
            route.prefix.len >= found_prefix.max_len;
            route.prefix.origin-asn != found_prefix.asn;
        };
        then {
            route.bgp.communities.add(1000:5);
            accept;
        }
    }

    term rov-unknown for route {
        with rov-rib;
        from {
            found_prefix.does_not_match;
        };
        then {
            route.bgp.communities.add(1000:2);
            accept;
        }
    }

    // compose the statements into a `filter`
    //
    // `and then` is only run when the
    // compound filter expressions returns `accept`.
    // You could also add a `or` statement, that
    // runs if the return code is `reject`.
    filter set-rov-communities for route {
        (
            rov-valid or
            ( rov-invalid-length and
            rov-invalid-asn )
        ) and then {
            accept;
        };
    }
}

rib global.irr_customers as irr_customers {
    id: "global.irr_customers",
    prefix: Prefix,
    origin_asn: [Asn],
    as_set: [{ prefix: Prefix, asn: Asn }],
    customer_id: u32
}

module irrdb {
    define irr-customers-table for route {
        found_prefix = irr_customers.longest_match(route.prefix);
    }

    // only checks if the prefix exists, not if it
    // makes sense.
    term irrdb-valid for route {
        with irr_customers;
        from {
            found_prefix.matches;
        }
        then {
            route.bgp.communities.add(1001:1);
            accept;
        }
    }

    term more-specific for route {
        with irr_customers;
        from {
            found_prefix.matches;
            found_prefix.len < route.prefix.len;
        }
        then {
            route.bgp.communities.add(1001:3);
            accept;
        }
    }

    term prefix-not-in-as-set for route {
        with irr_customers;
        from {
            found_prefix.matches;
            route.prefix not in found_prefix.as_set.prefix;
        };
        then {
            route.bgp.communities.add(1001:4);
            accept;
        }
    }

    term invalid-origin-as for route {
        with irr_customers;
        from {
            found_prefix.matches;
            route.origin-asn not in found_prefix.as_set.asn;
        };
        then {
            route.bgp.communities.add(1001:5);
            accept;
        }
    }

    term invalid-prefix-origin-as for route {
        with irr_customers;
        from {
            found_prefix.matches;
            route.origin-asn not in found_prefix.origin_asn;
        };
        then {
            route.bgp.communities.add(1001:6);
            accept;
        }
    }

    filter set-irrdb-communities for route {
        (
            irrdb-valid and
            irrdb-more-specific and
            irrdb-prefix-not-in-as-set and
            irrdb-invalid-origin-as and
            irrdb-invalid-prefix-origin-as
        ) and then {
            accept;
        };
    }
}

filter rpki+irrdb for route {
    filter rpki.set-rov-communities;
    filter irrdb.set-irrdb-communities;
}
```

### Imports

```
prefix-list bogons global.bogons;

table customer-prefixes
    from file "/home/user/irr-table.json" {
        prefix: Prefix,
        as_set: [Asn],
        origin_asn: Asn,
        customer_id: u32
}

rib global.irr-customers as irr-customers;

term drop-bogons for prefix {
    with customer-prefixes;
    from {
        prefix in
            exact_match(bogons);
    }
    then {
        reject;
    }
    
}

import irr-customer from table customer-prefixes for record {
    drop-bogons for record.prefix
    and then {
        destroy_and_create(irr-customers).insert(record);
    }
}

// `rotoro-stream` is not defined here, but would be a stream
// of parsed bgp messages.
import peer-stream from rotoro-stream for route {
    drop-ibgp for route
    and then {
        rib("global.rib-in-pre").insert_or_replace(route)
    }
}
```

### Queries

```
rib global.rib-in as rib-in {
    prefix: Prefix,
    as-path: AsPath,
    communities: Communities
}

// A literal query without arguments
query search-as3120-in-rib-loc {
    with {
        query_type: state-diff {
            start: "01-03-20221T00:00z";
            end: "02-03-2022T00:00z";
        };
    }
    from rib {
        route.bgp.as_path.contains(asn);
    }
    // implicitly this is added:
    // and then {
    //   send-to stdout;
    // }
}

// A term can be reused, just like
// in a filter.
term search-asn for asn {
    with { 
        global.rib-in;
    }
    from {
        global.rib-in => route.bgp.as_path.contains(asn);
    }
}

// A query function (with an argument),
// can be used like so:
// search-asn for 31200;
query search-asn for asn {
    with {
        query-type: created-records {
            time_span: last_24_hours()
        };
        format: json;
    }
    search-my-asn for asn and then {
        send-to: stdout;
    }
}

define my-asn-24-hours {
    asn = AS64500;
    query_type = created-records {
        time_span: last_24_hours()
    };
    format: json;
}

query search-my-asn for asn {
    with my-asn-24-hours;
    search-asn;
}
```

```
term search-my-asns-records for [asn] {
    from {
        bgp.as_path.contains([asn]);
    }
    then {
        send-to py_dict();
    }
}
```

```
// e.g. query-my-as-dif for AS3120 and with ("02-03-2022T00:00z",
// "02-03-20T23:59z") in rib("global.rib-ib")
query search-my-as-dif for [asn] and with (start_time, end_time) in rib {
    with {
        query_type: state-diff {
            start: start_time;
            end: end_time;
        };
    }
    search-my-asns-records for [asn]
}
```
