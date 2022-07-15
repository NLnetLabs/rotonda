```
// An example that performs validation for Flowspec according to RFC8955

module flowspec-validator {
    rib global.rib-in as rib-in with Route {
        prefix: Prefix,
        originator_id: OriginatorId,
        as_path: AsPath
    }

    rib global.flowspec as rib-flowspec with FlowSpecRule {
        dest_prefix: Prefix,
        originator_id: OriginatorId,
        origin_as: As,
        rules: [FlowSpecRule],
        action: Action
    }

    define ribs for fs_rule: FlowSpecRule {
        use rib-in;
        use rib-flowspec;
        found_prefix = rib-in.longest_match(fs_rule.dest_prefix);
    }

    term validate-flowspec for fs_rule: FlowSpecRule {
        with ribs;
        from {
            // rules from RFC8955 Section 6
            // a. has a destination prefix
            fs_rule.dest_prefix.exists;
            // b. originator is the best-match
            found_prefix.matches;
            found_prefix.originator_id == fs_rule.originator_id;
            // c. no more-specific rules exist from another neighbor AS.
            found_prefix.more-specifics.[as-path.first].every(fs_rule.origin_as);
        }
        or {
            reject;
        }
    }

    filter flowspec with fs_rule: FlowSpecRule {
        validate-flowspec and then {
            rib-flowspec.store(fs_rule);
        }
    }
}
```

```
// Adds the ID of the Route Server to every route.
// See: https://www.euro-ix.net/en/forixps/large-bgp-communities/

module tracer {
    define rs-id {
        my_rs_id: u32 = 1;
    }

    term add-rs-id for route: Route {
        with rs-id;
        then {
            route.bgp.communities.add(1002: my_rs_id);
        }
    }

    filter add-rs-id {
        add-rs-id;
    }
}
```

```
// Adds a community with the latency towards that peer for a route,
// if the peer that sends the route appears in a peer-latency table.
// See: https://www.euro-ix.net/en/forixps/large-bgp-communities/

rib table global.peer-latencies as peer-latencies with PeerLatency {
    peer_id: PeerId,
    latency: u32
}

virtual rib rib-in-post-policy for route: Route;

module latency-tagger {
    define tagger for route: Route {
        use peer-latencies;
        peer-latency = peer-latencies.match(route.peer_id).latency;
    }

    term latency-for-peer for route: Route {
        with tagger;
        from {
            route.bgp.communities.add(1003: peer-latency);
        }
    }

    filter latency-for-peer for route: Route {
        with tagger;
        latency-tagger;
    }
}
```

# The Graph part of the language

This is basically a separate language, it describes the flow from storage unit to
storage unit and the parts in between, e.g. filter units, broadcast units.

```
module rib-in-post-policy {
    define {
        use rib-in;
        use rib-in-post-policy;
    }

    graph rib-in-post-policy for route: Route {
        // Registering an external dataset makes rotonda listen for changes and
        // making the right calls to update the relevant entries in the RIBs.
        register global.rov-rib;
        register global.rib-customers-table;
        register global.rib-in;
        register global.peer-latencies;

        rib-in => 
            route-security.rpki+irrdb(global.rov-rib, global.irr-customers-table) ->
            tracer.add-rs-id -> 
            latency-tagger.latency-for-peer(global.peer-latencies)
        => rib-in-post-policy;
    }
}
```
