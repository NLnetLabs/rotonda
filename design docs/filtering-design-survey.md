# Filtering in Rotonda - a Survey

# Prior Art

- IOS (Cisco) / FRR
- Bird
- Junos

IOS (Cisco) / FRR
===========

Declarative config files.

## "Objects"

### Access List

Packet based.

`access-list <ACL_ID> <ACL_TYPE> <IP_ADDR>|any`

adds a rule to access-list with id `<ACL_ID>`.

ex.:

`access-list 1 deny 10.20.0.0`

`access-list 1 permit any`

Also, extended ACLs:

`access-list <ACL_ID> <ACL_TYPE> ip <PREFIX_NET> <SIGNIFICANT_BITS_MASK> <MIN_LENGTH_MASK> <ADDRESS_MASK>`

This terrible, even Cisco admits this. That's why they have prefix lists.

### Distribute List

Route based. A collection of access-lists plus a filter action (e.g., in or out).

`neighbor <IP_ADDR> distribute-list <ACL_ID> <FILTER_DIRECTION>`

ex.: 

`neighbor 10.100.83.1 ditribute-list 1 out`

### Prefix List

Route based. the packet based approach needs a `access-list` to define the ACL and a `distribute-list` to tie it to a filter action.
`prefix-list` has no such requirement, there's only the `prefix-list` keyword.

To use a `prefix-list`:

`neighbor <IP_ADDR> prefix-list <PL_ID> <FILTER_DIRECTION>`

To define one:

`ip prefix-list <PL_ID> seq <SEQUENCE_NUMBER> <FILTER_ACTION> <IP_PREFIX>`

Note that `<PL_ID>` is a string, rather than an integer (as in the case of access|distribute-lists). A prefix can be extended with a `ge` or `le` modifier to indicate a sub|super range of the prefix.

## Filter Types / Actions

Next to the ACL-style filter-actions, `deny`, `permit` it can do filtering BGP attributes.

### AS_PATH

with regex (yes, uncontroversially terrible):

e.g.:

`ip as-path access-list 83 permit "1_701_(_5466_ | _1240_).*"`

### Route Map

a `route-map` ties a filter actions and filter types to routes. This is basically used for anything that's more than `deny`, `permit` and AS_PATH filtering.

e.g.:

```
route-map ASPEN deny 10
  match ip address prefix-list MILLER
  match local-preference 150
  match community WARREN
```

where `MILLER` and `WARREN` are defined elsewhere.

You can also change attributes of a route with a `route-map`:

e.g.:

```
route-map ASPEN deny permit 20
  match ip address prefix-list MILLER
    match local-preference 150
    match community WARREN
    set local-preference 100
    set comm-list WARREN delete
```

furthermore, route-maps are also used to determine which (transitive) attributes get sent to which peer, e.g. `send-community`.

### Peer Group

group a bunch of neighbors into a single entity and specify policies for them.

### Peer Template

Successor to the Peer Group. Also, you can't use both on a neighbor. Offers (explicit) inheritance, with an inheritance sequence number.
There's session templates and peer templates. No variables.

### Policy Template

## Differences between FRR and Cisco

FRR generally follows the Cisco CLI, it has sanitized it a bit though. FRR consequently has access-lists and prefix-lists, but no distribute-list, it uses the prefix-list syntax for both, including having the `seq` keyword on both access-lists and prefix-lists.

FRR does not offer peer templates, only peer groups.

FRR offers an embedded Lua interpreter, that can hook into a callback that gets invoked at the end of each data-plane event. This is a passive (listening) feature only.

Bird
====

Bird offers a small procedural DSL to create filters. Variables and functions can be created. Next to that it has an object called, well, filters. The DSL has control structures, well it only has `if-then-else`, and it can't do loops.

A filter is a named object that receives the attributes of a route (implicitly). This filter can then use read some of these values and mutate of them. I assume then most, if not all, of the BGP attributes are available, but I can't find it in the docs.

Another nice feature is that the DSL uses data types for the attributes, e.g. a `bgp_path` value that's passed into the filter is of type `bgppath`,e.g. `(path 4 3 2 1)`. It has then its own methods, .e.g `.first`, etc.

The Bird filter DSL is rather terse (in true C-style), e.g. the Cisco-style notation `10.2.0.0/24 le 26 ge 30` would be `10.2.0.0/24{26, 30}` in Bird. In Juniper-style this would be `10.2.0.0/24 prefix-length-range /26-/30`.

Junos (Juniper)
===============

Junos calls all of its filtering capabilities: `policy framework`. It's declarative.

All policies are composed of the following components that you configure:

 - Match conditions—Criteria against which a route or packets are compared. You can configure one or more criteria. If all criteria match, one or more actions are applied. The match conditions are:

    - Autonomous system (AS) path expression—A combination of AS numbers and regular expression operators.

    - Community—A group of destinations that share a common property.

    - Prefix list—A named list of prefixes.

    - Route list—A list of destination prefixes.

    - Subroutine—A routing policy that is called repeatedly from other routing policies.

 - Actions—What happens if all criteria match. You can configure one or more actions. Basically you can use *accept* and *reject* here, or a control-flow actions, e.g. *next term*.

 - Terms—Named structures in which match conditions and actions are defined. You can define one or more terms.

In this policy framework there's a strong distinction between *import* and *export* policies.

Prefix lists are comparable with the Cisco `prefix-list`, but its syntax is way more powerful, it lets you define ranges of more-specifics in various ways, i.e. with `orLonger`, `range`.

A *subroutine* is a policy that can be called repeatedly from other policies. A policy can also be called from another policy, but only in a chained fashion, a so-called *policy chain*.

Junos has no data-types, like Cisco. Matching on most attributes happens through regular expressions on strings.

Junos configuration is more verbose than any of the other implementations discussed here. Also, Junos configurations are less single-line CLI oriented than Cisco's. OTOH they can be declared in single-lines in the CLI.

```
policy-options {
    policy-statement gendefault {
        term upstreamroutes {
            from {
                protocol bgp;
                as-path upstream;
                route-filter 0.0.0.0/0 upto /16;
            }
            then {
                next-hop 10.0.45.1;
                accept;
            }
        }
        term end {
            then reject;
        }
    }
    as-path upstream "^64500 ";
}
```
# Discussion

The Cisco/FRR cli is the arch-version of a router CLI, and it that's both a blessing and a curse. The blessing is the familiarity people have with it, and the curse is all the cruft that has been piled on it. To my taste it's also basically too terse and therefore too cryptic for people not used to it. Also, probably people are more used to the older and maybe deprecated features, instead of the newer ones. The templating system is too rigid and too clunky.

The Juniper has a more modern feel to it, and is certainly more descriptive (I like the prefix list definitions). It feels more structured than Cisco's.

Bird's filter DSL is pretty flexible and powerful, but also a bit too terse to my taste. For simple filters it's probably quite a hurdle to overcome. Having actual data types is great, I think, both in terms of performance, readability and safety.

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

```
// A fairly simple example of a filter-statement
// with a defined variable.
define last-as-64500 {
    last_as_64500 = AsPathFilter { last: 64500 };
}

filter-statement no-as-64500-until-len-16 {
        from {
            prefix-filter 0.0.0.0/0 upto /16;
            protocol bgp {
                as-path last-as-64500;
            };
            protocol internal {
                router-id 243;
            };
        }
        then {
            // a side-effect is allowed, but you can't
            // store anywhere in a filter-statement.
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
    define our-as {
        our-as: AsPathFilter { last: OUR_ASN };
    }

    term drop-ibgp on route {
        from {
            # drop our own AS
            route.bgp.as-path.contains(global.our-as);
        }
        then {
            send-to standard-logger ibgp;
            reject;
        }
    }
}
```

```
rib global.rov as rov {
    prefix: Prefix,
    max_len: u8,
    asn: Asn,
}

module rpki {
    define rov-rib on route {
        found_prefix = rov.longest_match(route.prefix);
    }

    filter-statement rov-valid on route {
        with rov-rib;
        // A rule can have multiple with statements,
        // either named or anonymous.
        // with {
        //    max_len = 32;
        // }
        from rov-rib {
            found_prefix.matches;
            route.prefix.len <= found_prefix.max_len;
            route.prefix.origin-asn == found_prefix.asn;
        }
        then {
            route.bgp.communities.add(1000:1);
            accept;
        }
    }

    filter-statement rov-invalid-length on prefix {
        with rov-rib;
        from {
            found_prefix.matches;
            prefix.len > found_prefix.max_len;
            prefix.origina-asn == found_prefix.asn;
        };
        then {
            route.bgp.communities.add(1000:6);
            accept;
        }
    }

    filter-statement rov-invalid-asn on prefix {
        with rov-rib;
        from {
            found_prefix.matches;
            prefix.len >= found_prefix.max_len;
            prefix.origin-asn != found_prefix.asn;
        };
        then {
            route.bgp.communities.add(1000:5);
            accept;
        }
    }

    filter-statement rov-unknown on prefix {
        with rov-rib;
        from {
            found_prefix.does_not_match;
        };
        then {
            route.bgp.communities.add(1000:2);
            accept;
        }
    }

    // `and then` is only run when the
    // compound filter expressions returns `accept`.
    // You could also add a `or` statement, that
    // run if the outcode is `reject`.
    filter set-rov-communities {
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
    define irr-customers-table {
        found_prefix = irr_customers.longest_match(prefix);
    }

    // only checks if the prefix exists, not if it
    // makes sense.
    filter-statement irrdb-valid on route {
        with irr_customers;
        from {
            found_prefix.matches;
        }
        then {
            route.bgp.communities.add(1001:1);
            accept;
        }
    }

    filter-statement more-specific on route {
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

    filter-statement prefix-not-in-as-set on route {
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

    filter-statement invalid-origin-as on route {
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

    filter-statement invalid-prefix-origin-as on route {
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

    filter set-irrdb-communities {
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

filter rpki+irrdb {
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

filter-statement drop-bogons on record {
    with customer-prefixes;
    filter-statement customer {
        from {
            prefix in
                exact_match(global.bogons);
        }
        then {
            reject;
        }
    }
}

import irr-customer from table customer-prefixes on record {
    drop-bogons
    and then {
        destroy_and_create(irr-customers).insert(record);
    }
}

// `rotoro-stream` is not defined here, but would a stream
// of parsed bgp messages.
import peer-stream from rotoro-stream on route {
    drop-ibgp
    and then {
        rib("global.rib-in-pre").insert_or_replace(route)
    }
}
```

### Queries

```
// We start with a filter-statement, so it can be
// re-used later in a query.
filter-statement search-my-asn {
    from {
        bgp.as_path.contains(MY_ASN);
    }
}

// A literal-only Query with an argument,
// can be used like so:
// search-asn(31200);
query search-asn {
    // upper-case keys in the with section
    // are arguments
    with { 
        MY_ASN: Asn
    }
    query-type created-records {
        time_span: last_24_hours();
    }
    search-my-asn and then {
        send-to: stdout;
    }
}

define my-asn {
    MY_ASN: Asn = 64500;
    query_type: QueryType = created-records {
        time_span: last_24_hours();
    };
}

query search-my-asn {
    with my_asn;
    search-my-asn;
}
```

```
query-statement SearchMyAsRecords {
    term query-as {
        from {
            bgp.as_path contains-my-as;
        }
        then {
            export-rib to_py_dict();
        }
        contains-my-as: AsPathFilter { contains: MY_ASN };
    }
    with {
        MY_ASN: 64500;
        query_type: created-records {
            time_span: last_24_hours();
        };
    }
}
```

```
query-statement SearchMyAsDif(
    MY_ASN, 
    START_TIME, 
    END_TIME
) {
    term query-as;
    with {
        query_type: state-diff {
            start: START_TIME;
            end: END_TIME;
        };
    }
}
```

```
query-statement Search_AS3120 {
    term query-as;
    with {
        MY_ASN: 3120;
        query_type: state-diff {
            start: "01-03-20221T00:00z";
            end: "02-03-2022T00:00z";
        };
    }
}
```