# Changelog
 
## Unreleased version

Released yyyy-mm-dd.


### New


### Bug fixes


### Breaking changes


### Other changes


### Acknowledgements



## 0.5.1

Released 2026-02-09.


### New

* Added `filter[peerAddress]` to the `/ribs` HTTP endpoint.

* HTTP API responses now include the `NextHop` part of the `MP_REACH_NLRI` path
  attribute.

* HTTP API supports gzip compressed responses, and streaming/chunking of
  responses.


### Bug fixes


* SELinux on CentOS panic on startup
 ([#132](https://github.com/NLnetLabs/rotonda/issues/132),
 [roto#259](https://github.com/NLnetLabs/roto/issues/259),
 [#150](https://github.com/NLnetLabs/rotonda/pull/150))


### Acknowledgements

We would like to very much thank Rein Fernhout (LevitatingBusinessMan) and
Denys Fedoryshchenko for their (ongoing) input and support in various ways.
 
## 0.5.0 'Mosaïque Public'

Released 2025-09-30.


Breaking changes

* The HTTP machinery has been refactored. The JSON endpoints are under
  different URLs and also their responses are slightly different from the old
  version. See the [documentation](https://rotonda.docs.nlnetlabs.nl) for all
  the details.

* Roto has been upgraded to version 0.7 which includes minor but breaking syntax
  changes. Most notably, `function` is now `fn`. More details can be found in
  the [Roto
  changelog](https://github.com/NLnetLabs/roto/blob/main/Changelog.md).

* The Roto filters `bgp_in`, `bmp_in` and `rib_in_pre` are now all passed the
  new `IngressInfo` object, containing information about the *ingress* of the
  message or route. This can be used to get the peer ASN and address for that
  specific session, for example.

* In Roto scripts, the `community(u32)` function to create a new Community has
  been removed in favor of the more flexible and more readable
  `Community.from(str)` which takes either Well-known community names or
  the canonical form "AS12345:9999".
  Similarly, a `LargeCommunity.from(str)` is introduced, which takes the
  canonical form "AS211321:8888:9999".

New

* For BMP ingresses, the Peer type, Peer distinguisher, and VRF/Table name are
  now tracked in the *ingress register*, and (if set) returned in HTTP API
  responses. ([#128](https://github.com/NLnetLabs/rotonda/pull/128))

* The new JSON API can filter based on user-defined Roto functions.

* In Roto scripts, a new `metrics` object is introduced, enabling user-defined
  counters/gauges for the `/metrics` Prometheus endpoint.

* The RTR version is now configurable via `initial_version` on the *rtr-tcp-in*
  unit, defaulting to version 2. While the RTR protocol describes version
  negotiation and downgrading should happen automatically, this setting can be
  used in case of compatibility issues between Rotonda and the cache.

Bug fixes

* The procedure to find known ingresses (e.g. when a BMP session is
  re-established) would not take into account the Peer type/distinguisher and
  VRF/Table name. This could cause mismatches, leading to routes being stored
  in the wrong spot and/or incorrectly overwriting other routes.
  Presumably, this only affected monitoring of Loc-RIBs.
  ([#128](https://github.com/NLnetLabs/rotonda/pull/128))

* The RTR version negotiation now properly responds with a lower version after
  receiving an Unsupported Version error from the cache. 



Known issues

* The web UI is very minimal in this version.

* Multiple BMP streams with the same source IP address are currently not
  properly distinguished, and will show up as one single connected router but
  with the monitored BGP sessions for both streams.

* The MRT endpoint is not (yet) available new HTTP API.

* The built-in metrics on the `/metrics` endpoint need work and are not to be
  trusted blindly at this point. 

* Certain error responses from the JSON endpoints are not JSON-formatted yet.



Acknowledgements

We would like to very much thank Hans Kuhn for their (ongoing) input
and support in various ways.


## 0.4.2 'Bonjour des Pyrénées'

Released 2025-06-18.

With this release, Rotonda switches to version 0.6.0 of the `roto` library.
While many things have improved behind the scenes and under the hood, there are
no breaking or otherwise noticeable changes for Rotonda users.

An overview of the changes in `roto` 0.6.0 can be found
[here](https://github.com/NLnetLabs/roto/blob/v0.6.0/Changelog.md).


## 0.4.1 'Melolontha²'

Released 2025-05-20.

New

* VRP information coming in via RTR updates (both full Cache Resets and Serial
  responses) now triggers reevaluation of stored prefixes.
* The new `vrp_update` filter can be used to act upon incoming VRP updates, for
  example for logging purposes.
* The new `rib_in_rov_status_update` function can be used to act upon changes
  with regards to the ROV status of stored routes.
* In roto, lists of ASNs and prefixes can be defined in the new `compile_lists`
  function. These lists can be used from all other filters to match on, for
  example for logging and monitoring purposes.
* A new `timestamped_print` method is added on the `output` type in roto.

Other changes

* The `file-out` logging target now periodically flushes to disk, to prevent
  information from waiting in the buffer in low traffic scenarios.


## 0.4.0 'Bold and Undaunting Youth'

Released 2025-04-24.

Breaking changes

* Roto filter names have changed, as dashes are replaced with underscores.

New

* An RTR component is introduced, enabling Rotonda to receive RPKI information
  from RP software via the RTR protocol.
* Route Origin Validation is available on incoming routes in the 'rib_in_pre'
  filter using a new `rpki.check_rov(..)` method. The ROV result is included
  in responses from the RIB's HTTP endpoint.

Bug fixes

* In some cases, log entries would not actually be written to the output file.
* Under certain circumstances existing more specific prefixes, that were not
  covered by a requested prefix were returned. This is now fixed.

Known issues

* Performing ROV from roto scripts currently only works on incoming routes,
  using the VRP data that is available at that moment. New VRP data coming in
  via VRP does not trigger reevaluation of stored routes yet.

Other changes

* Parsing of certain BMP/BGP fields, specifically BGP Capabilities, are more
  forgiving to prevent reconnection loops.


## 0.3.0 'Hempcrete & Hawthorn'

Released 2025-01-30.

Breaking changes

* In the embedded `roto` the _define_ and _apply_ blocks are gone. Users can
  now define variables and functions throughout the script.
* Arguments to filter-maps in `roto` have changed: some of them are now
  implicit.

  See the bundled example in `etc/examples` for more details.


New

* In the `roto` language users can now assign variables and functions through
   the use of `let` statements anywhere in a `roto` script.
* In `roto`, in addition to the breaking changes, there now is support for
  constants and strings. This enables, amongst other things, customizable
  logging. 
  Several other helper methods on the various types, including ones to format
  fields to strings, are introduced.
* The new `file-out` target enables logging to a file in JSON or CSV format.
  Several helpers to log specific features are available, as well as the
  aforementioned custom logging via strings.
* The `mrt-file-in` unit can now be configured to process multiple files in
  sequence, and is able to process the most common message types seen in
  `update` MRT files. Furthermore, _.bz2_ compressed archives can be processed.
  A new API endpoint enables queueing of files.
* Upon re-establishment of a previous BMP session, the previous session
  information is looked up and re-used. This should reduce memory use, and
  moreover, prevent confusing results when querying prefixes.
* BMP messages originating from a _LocRib_ are now recognized and stored.


Bug fixes

* Several metric counters were not increased properly.


Known issues

* The "more_specifics" field in a query result may include wrong prefixes.


Other changes

* We no longer build binary packages for Debian 9 "Stretch" and Ubuntu 16.04
  LTS "Xenial Xerus".


Acknowledgements

We would like to very much thank the following people for their (ongoing) input
and support in various ways: Tobias Fiebig, Ties de Kock, Lisa Bruder, Ralph
Koning, Bruno Blanes.



## 0.2.1

Released 2024-12-06.

Bug Fixes

* A non-monotonic clock measurement caused some threads to panic (under
  unknown conditions). These measurements now use a monotonic clock, that can
  not panic.
* The packaged filters.roto.example contained wrongly ordered parameters in
  its filter definitions, resulting in them not being executed. The orders are
  fixed and logging is added.

New

* The MRT component can now read gzipped files. Decompression is triggered for
  files with the '.gz' extension.

Other changes

* A zero-length `sysName` in a BMP InitiationMessage caused all subsequent
  messages to be marked as hard fails. We now warn on such empty values but
  proceed as usual, because the `sysName` is not crucial to the process in
  any way.


## 0.2.0 'Happy Fuzzballs'

Released 2024-11-21.

Breaking changes

This release contains mostly refactored components, aiming at better
performance and reduced resource requirements.

Most notably, our filter language `Roto` is now compiled to machine code and
offers user friendly error messages.


## 0.1.0  ‘For the sake of Whiskey, color Televisions and Pianos’

Released 2024-01-19

First release
