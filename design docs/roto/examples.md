```
// An example that performs validation for Flowspec according to RFC8955

module flowspec-validator with (rib-in, rib-flowspec) {
    rib rib-in contains Route {
        prefix: Prefix,
        originator_id: OriginatorId,
        as_path: AsPath
    }

    rib rib-flowspec contains FlowSpecRule {
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
        match {
            // rules from RFC8955 Section 6
            // a. has a destination prefix
            fs_rule.dest_prefix.exists;
            // b. originator is the best-match
            found_prefix.matches;
            found_prefix.originator_id == fs_rule.originator_id;
            // c. no more-specific rules exist from another neighbor AS.
            found_prefix.more-specifics.[as-path.first].every(fs_rule.origin_as);
        }
    }

    apply for fs_rule: FsRule {
        use validate-flowspec;
        filter match validate-flowspec matching { return accept; };
        return reject;
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
        match {
            route.bgp.communities.add(1002: my_rs_id);
        }
    }

    apply for route: Route {
        filter match add-rs-id matching add-rs-id;
        return accept;
    }
```

```
// Adds a community with the latency towards that peer for a route,
// if the peer that sends the route appears in a peer-latency table.
// See: https://www.euro-ix.net/en/forixps/large-bgp-communities/

virtual rib rib-in-post-policy for route: Route;

module latency-tagger with peer-latencies {
    rib table peer-latencies contains PeerLatency {
        peer_id: PeerId,
        latency: u32
    }

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

    apply for route: Route {
        filter match latency-peer matching { latency-tagger; };
        return accept;
    }
}
```


```
module my-message-module with my_asn: Asn {
    define {
        // specify the types of that this filter receives
        // and sends.
        // rx_tx route: StreamRoute;
        rx route: Route;
        tx out: Route;
    }

    term rov-valid for route: Route {
        match {
            route.as-path.origin() == my_asn;
        }
    }
    
    action send-message {
        mqtt.send({ 
            message: String.format("🤭 I encountered {}", my_asn),  
            my_asn: my_asn
        });
    }

    apply {
        filter match rov-valid not matching {  
            send-message;
        };
    }
}

output-stream mqtt contains Message {
        message: String,
        asn: Asn
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
