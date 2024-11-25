# Changelog

## Unreleased version

Bug Fixes

* A non-monotonic clock measurement caused some threads to panic (under
  unknown conditions). These measurements now use a monotonic clock, that can
  not panic.

Breaking changes


New


Other changes


Known limitations/issues



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
