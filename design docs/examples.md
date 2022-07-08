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