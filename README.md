# Rotonda

Roll your own BGP application with Rotonda. 

This repository contains the units and the configurations to create a high-performance BGP application.

Other parts of `Rotonda` are the lock-free, concurrent read/write prefix store [`rotonda-store`](https://github.com/NLnetLabs/rotonda-store) and the filter & query language [`roto`](https://github.com/NLnetLabs/roto). `Rotonda` draws heavily upon the BGP packet parsing & creation library called [`routecore`](https://github.com/NLnetLabs/routecore).

### ROADMAP

Rotonda is under heavy development and should be considered **alpha** software. See the [Roadmap](ROADMAP.md) in this repository for more details.

### DOCUMENTATION

The documentation does not necessarily reflect the reality of Rotonda at this stage. Features that appear in the documentation might only be partly implemented — or not at all. Likewise, implemented features may be undocumented. Before we make an official release we will clearly mark the status of all features in the documentation.

Read the PRELIMINARY documentation [here](https://rotonda.docs.nlnetlabs.nl/).

###LICENSE

The rotonda crate is distributed under the terms of the MPL-2.0 license. See [LICENSE](https://github.com/NLnetLabs/rotonda/blob/main/LICENSE) for details

Rotonda is and always will be free, open-source software.
