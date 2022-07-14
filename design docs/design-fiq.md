# Rotolo filter-impex-query language - The requirements

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

### Filters-maps and filters.

A filter-map consists of `term` statements and a `filter` statement.

A `term` is a single statement, that consists of one or more conditions that are
tested sequentially. If all of them are true, then the term executes the actions
in the `then` clause. If one of them are false, then the actions in the `or` (if
available) clause are executed. The last action in both the `then` and `or`
clauses must be the `reject` or the `accept` return-action. If one of the `then`
or `or` clause are missing then an `accept` action is returned by default. A
`term` is agnostic to the actual RIB it is being filtered against, so it cannot
specify the RIB. It can however specify another (external) data-structure, like
a table or a prefix-list, to match against by specifying a `with` clause.

The `define` is run once per filter call. This multiple references to the
vars in `with` clauses are not re-triggering calculation of the values of the
variables.

### Term example

```
module example {
    // A fairly simple example of a term with a defined variable, but without
    // using any global data-sources.
    define last-as-64500-vars {
        last_as_64500 = AsPathFilter { last: 64500 };
    }

    term no-as-64500-until-len-16 for route: Route {
            with {
                last-as-64500-vars;
            }
            from {
                prefix-filter 0.0.0.0/0 upto /16;
                protocol bgp {
                    route.as-path.matches(last-as-64500);
                };
                protocol internal {
                    route.router-id == 243;
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
}
```

```
// A hard-coded ibgp dropper term for AS64496
module ibg-dropper {
    term drop-ibgp for route:Route {
        from {
            route.bgp.as-path.matches(AsPathFilter { last: AS64496 });
        }
        then {
            reject;
        }
    }
}
```

```
// A ibgp-dropper that takes as argument the AS for which we're dropping the
// iBGPs.
module ibgp-dropper {
    define our-as for route: Route with our_asn: Asn {
        our-as = AsPathFilter { last: our_asn };
    }

    term drop-ibgp for route: Route {
        // `our-asn` is an argument of type `Asn` that should be defined by
        // the filter that includes this term.
        with {
            our-asn: Asn;
        }
        from {
            // drop if our own AS appears anywhere in
            // the as-path.
            route.bgp.as-path.matches(our_asn);
        }
        then {
            send-to standard-logger ibgp;
            reject;
        }
    }
}
```

### Filter

A filter is a chain of terms, that will be executed based on the return action
of the last executed term and the type of joining clause, either a `or` or an
`and`. A `accept` return action is evaluated as true, and a `reject` return
action is evaluated as false. The filter exits as early as possible and the
side-effects of the terms are executed up to the point of the last evaluated
term. This means that **order of the terms does matter**.

Like a term a filter ends in a `reject` or `accept` action. The implicit action
at the end is `accept`.

Filters can be composed also from other filters.

Special data structures for incoming data all live in the global namespace and
can be referenced by `global`. The data-structures are:

- rib—A hash-map keyed on prefix and that holds routing information as its
  values. Values fall into two groups: `bgp`, these are the BGP attributes, e.g.
  as-path, communities, etc. and `internal`, these are values that are specific
  to a RIB and defined at creation time.
- table—a hash-set.
- prefix-list—a list of prefixes.

```
rib global.rov as rov-rib with Route {
    prefix: Prefix,
    max_len: u8,
    asn: Asn,
}

module rpki {
    define rov-rib-vars for route: Route {
        use rov-rib;
        found_prefix = longest_match(rov-rib, route.prefix);
    }

    term rov-valid for route: Route {
        with rov-rib-vars;
        match {
            matches(found_prefix.prefix);
            route.prefix.len <= found_prefix.max_len;
            route.prefix.bgp.origin-asn == found_prefix.asn;
        }
    }

    term rov-invalid-length for route: Route {
        with rov-rib-vars;
        match {
            found_prefix.matches;
            route.prefix.len > found_prefix.max_len;
            route.prefix.bgp.origin-asn == found_prefix.asn;
        };
    }

    term rov-invalid-asn for route: Route {
        with rov-rib-vars;
        match {
            found_prefix.matches;
            route.prefix.len >= found_prefix.max_len;
            route.prefix.origin-asn != found_prefix.asn;
        };
    }

    term rov-unknown for route: Route {
        with rov-rib-vars;
        match {
            found_prefix.does_not_match;
        };
    }

    // compose the statements into a `action`
    //
    // `and then` is only run when the
    // compound filter expressions returns `accept`.
    // You could also add a `or` statement, that
    // runs if the return code is `reject`.
    //
    // Note that this filter will always return `accept`,
    // regardless of the validation. It's just used to
    // set the corresponding communities on the route.
    action set-rov-valid-community for route {
        route.bgp.communities.add(1000:1);
        send-to standard-logger rov;
    }
    action set-rov-unknown-community for route {
        route.bgp.communities.add(1000:2);
    }
    action set-rov-invalid-length-community for route {
        route.bgp.communities.add(1000:6);
    }
    action set-rov-invalid-asn for route {
        route.bgp.communities.add(1000:5);
    }

    apply for route: Route {
        with best-path;

        filter match rov-valid-community(route) {
            matching { 
                set-rov-valid-community;
                return accept;
            };
        };
        filter match rov-unkown(route) {
            matching {
                set-rov-unkown-community;
                return accept;
            } 
        }>>
        filter match rov-invalid-length { matching set-rov-invalid-length-community }>>
        filter match rov-invalid-asn { matching set-rov-invalid-asn-community };
        return accept;
    }
}

rib global.irr_customers as irr_customers: CustomerPrefixRecord {
    prefix: Prefix,
    origin_asn: set of Asn,
    as_set:  list of { prefix: Prefix, asn: Asn },
    customer_id: u32
}

rib global.rib-in as rib-in: set of Route {
    pretix: Prefix,
    bgp_records: set of BgpRecord
}

virtual shortest-path-rib as rib-in: Route {
    prefix: Prefix,
    bgp: BgpRecord
}

module shortestpath {
    filter shortest_path_filter for records: set of BgpRecord {
        // .select(fn) uses fn as a filter of all the entries in
        // a collection (prefix-filter, table, rib). So `fn` should take the
        // type of a single entry in the collection as its argument.
        records.select_for_attribute("as-path.length", select.Equal(1)).first;
    }

    map store-shortest-path for rib: shortest-path-rib from rib-in {
       rib-in.select_with_fn(shortest_path_filter(rib-in.bgp_records));
    }
}

module best-path-selection {
    define best-path for route: Route {
        use rib-in;
        found_prefix = rib-in.exact_match(route.prefix);
    }

    term exists for route: Route {
        with best-path;
        match {
            found_prefix.matches;
        };
    }

    term local-pref for route: Route {
        some {
            // localPrefHighest compares the local prefs, or if not present, substitutes with
            // 100 and then compares.
            route.bgp_records.select_for_attribute("local_pref", select.LocalPrefHighest);
        }
    }

    term originate for route: Route {
        some {
            route.bgp_records.select_for_attribute("next-hop", select.Equal(Prefix(0.0.0.0) || Prefix(0::));
        }
    }

    term as-path-length for route: Route {
        some {
            route.bgp_records.select_for_attribute("as-path.length", select.Min);
        }
    }

    action is-best-path for route: Route {
        send-to standard-logger best-path;
    }
    
    apply for route: Route {
        with best-path;

        filter exactly-one exists(found_prefix) matching { is-best-path(route); return accept; };
        filter some local-pref(route) matching one { is-best-path(route); return accept; } >> 
        filter some originate matching one { is-best-path(route); return accept; } >>
        filter exactly-one as-path-length matching { is-best-path(route); return accept; };
        reject;
    }


    // or as in described in https://datatracker.ietf.org/doc/draft-cppy-grow-bmp-path-marking-tlv/
    // mark: enum Mark { Invalid, Best, NonSelected, Primary, Backup, NonInstalled, BestExternal, AddPath }
    // reason: enum Reason { LocalPreference, Origin, AsPathLength, NextHop, Invalid }
    action invalid for route: Route {
        route.internal.mark.set(Invalid)
    }

    action set-best for route: Route {
        route.internal.mark.set(Best);
    }

    action set-non-selected for route: Route with reason: Reason {
        route.internal.mark.set(NonSelected);
        route.internal.mark_reason.set(reason);
    }

    action set-primary for route: Route with reason: Reason {
        route.internal.mark.set(Primary);
        route.internal.mark_reason.set(reason);
    }

    action set-backup for route: Route with reason: Reason {
        route.internal.mark.set(Backup);
        route.internal.mark_reason.set(reason);
    }

    action set-non-installed for route: Route with reason: Reason {
        route.internal.mark.set(NonInstalled);
        route.internal.mark_reason.set(reason);
    }

    action set-best-external for route: Route with reason: Reason {
        route.internal.mark.set(BestExternal);
        route.internal.mark_reason.set(reason);
    }

    action set-add-path for route: Route with reason: Reason {
        route.internal.mark.set(AddPath);
        route.internal.mark_reason.set(reason);
    }

    action set-best-and-backup for routes: set of Route {
        routes.last.internal.mark.set(Backup);
        routes.first.internal.mark.set(Best);
    }

    apply for route: Route {
        with best-path;

        filter exactly-one exists(found_prefix) matching { set-best(route); return accept; };
        filter some local-pref(route.bgp_records) not matching set-non-selected(LocalPref) matching pipe into next;
        filter some originate not matching set-non-selected(route, Originate) matching pipe into next;
        filter exactly-one as-path-length {
            matching { 
                set-best(route);
                return accept;
            } 
            not matching {               
                set-best-and-backup; 
                return accept;
            };
        }
    }
}


module irrdb {
    view irr-customers-table on global.irr-customers for route: Route {
        prefix: longest_match(route.prefix)
    }

    // only checks if the prefix exists, not if it
    // makes sense.
    filter irrdb-valid for route: Route {
        view found_prefix on irr-customers-table for route {
            prefix: matches(route.prefix)
        };
        found_prefix.is_not_empty;
        //then {
        //    route.bgp.communities.add(1001:1);
        //    accept;
        //}
    }

    term more-specific for route: Route {
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

    term prefix-not-in-as-set for route: Route {
        with irr_customers;
        from {
            found_prefix.matches;
            route.prefix not in found_prefix.as_set.[prefix];
        };
        then {
            route.bgp.communities.add(1001:4);
            accept;
        }
    }

    term invalid-origin-as for route: Route {
        with irr_customers;
        from {
            found_prefix.matches;
            route.origin-asn not in found_prefix.as_set.[asn];
        };
        then {
            route.bgp.communities.add(1001:5);
            accept;
        }
    }

    term invalid-prefix-origin-as for route: Route {
        with irr_customers;
        if {
            found_prefix.matches;
            route.origin-asn !== found_prefix.origin_asn;
        }
        then {
            route.bgp.communities.add(1001:6);
            route.accept;
        }
    }

    // Since all the terms mentioned here always return
    // `accept`, all of the chain will be run.
    filter set-irrdb-communities for route: Route {
        from {
            irrdb-valid and
            irrdb-more-specific and
            irrdb-prefix-not-in-as-set and
            irrdb-invalid-origin-as and
            irrdb-invalid-prefix-origin-as;
        }
        and then {
            accept;
        };
    }
}

module route-security {
    filter rpki+irrdb for route: Route {
        filter rpki.set-rov-communities;
        filter irrdb.set-irrdb-communities;
    }
}
```

### Imports

```
prefix-list global.bogons as bogons;

table customer-prefixes
    from file "/home/user/irr-table.json" with CustomerPrefixRecord {
        prefix: Prefix,
        as_set: [Asn],
        origin_asn: Asn,
        customer_id: u32
}

module imports {
    define customer-prefixes {
        use global.customer-prefixes as customer-prefixes;
        use global.irr-customers as irr-customers;
    }

    term drop-bogons for prefix: Prefix {
        with customer-prefixes;
        from {
            bogons.exact_match(prefix);
        }
        then {
            reject;
        }

    }

    import irr-customer from table customer-prefixes for record: CustomerPrefixRecord {
        drop-bogons for record.prefix
        and then {
            destroy_and_create(irr-customers).insert(record);
        }
    }

    // `rotoro-stream` is not defined here, but would be a stream
    // of parsed bgp messages.
    import peer-stream from rotoro-stream for route: Route {
        ibgp-dropper.drop-ibgp for route with AS211321;
        and then {
            global.rib-in.insert_or_replace(route);
        }
    }
}
```

### Exports

An `export` passes a route on from a RIB in memory to another Rotonda component
by taking a route and applying the specified filters to it. From the Rotonda
component this will look like just another RIB.

```
// A `virtual rib` just defines the name under which the other components can
// address the physical RIB + filters. This example describes a RIB called
// `rib-in-post-policy` that consists of all entries in the `rib-in` RIB and
// then has the specified filters applied to it at request time.

virtual rib rib-in-post-policy for route: Route;

module rib-in-post-policy {
    define {
        use rib-in-post-policy;
    }

    export rib-in-post-policy for route: Route {
        route-security.rpki+irrdb;
        tracer.add-rs-id;
        latency-tagger.latency-for-peer;
        // and then { accept; } is implicit here.
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

module queries {
    // A literal query without arguments
    query search-as3120-in-rib-loc for route: Route in global.rib-in {
        with {
            query_type: state-diff {
                start: "01-03-20221T00:00z";
                end: "02-03-2022T00:00z";
            };
        }
        from {
            route.bgp.as_path.contains(asn);
        }
        // implicitly this is added:
        // and then {
        //   send-to stdout;
        // }
    }

    // A term can be reused, just like
    // in a filter.
    term search-asn for route: Route with asn: Asn {
        from {
            route.bgp.as_path.contains(asn);
        }
    }

    // A query function (with an argument),
    // can be used like so:
    // search-asn-rib-in for 31200;
    query search-asn-rib for asn: Asn in global.rib-in {
        with {
            query-type: created-records {
                time_span: last_24_hours()
            };
            format: json;
        }
        search-my-asn for asn: Asn and then {
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

    query search-my-asn for asn: Asn {
        with my-asn-24-hours;
        search-asn;
    }

    term search-my-asns-records for route: Route with [asn: Asn] {
        from {
            route.bgp.as_path.contains([asn: Asn]);
        }
        then {
            send-to py_dict();
        }
    }

    // e.g. `query-my-as-dif for AS3120 with (
    //          "02-03-2022T00:00z",
    //          "02-03-20T23:59z") in rib-in`
    //
    // python:
    //
    // `query_my_as_dif.for(Asn(AS3120))
    //             .with(Datetime.from("02-03-2022T00:00z"),
    //                   Datetime.from("02-03-20T23:59z"))
    //             .in("rib_in")`
    query search-my-as-dif for [asn: Asn] {
        with (start_time: DateTime, end_time: DateTime) {
            with {
                query_type: state-diff {
                    start: start_time;
                    end: end_time;
                };
            }
            search-my-asns-records for [asn: Asn];
        }
    }
}
```
