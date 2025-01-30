# Changelog

## 0.3.0

Released 2025-01-30.

Breaking changes

* In the embedded `roto` the _define_ and _apply_ blocks are gone. They
  are both replaced by _let_ statements.
* Arguments to filter-maps in `roto` have changed: some of them are now
  implicit. Look into the bundled examples in `etc/examples` for more details.


New

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
