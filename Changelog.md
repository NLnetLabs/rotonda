# Changelog

## 0.2.1

Released 2024-12-06

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

Released 2024-11-21

Breaking changes

This release contains mostly refactored components, aiming at better
performance and reduced resource requirements.

Most notably, our filter language `Roto` is now compiled to machine code and
offers user friendly error messages.


## 0.1.0  ‘For the sake of Whiskey, color Televisions and Pianos’

Released 2024-01-19

First release
