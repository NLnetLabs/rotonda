# Rotonda

<img align="right" src="doc/manual/source/resources/rotonda-illustrative-icon.png" height="150">

Roll your own BGP application with Rotonda. 

BGP services that you will be able to build with Rotonda include, but are not
limited to, a route monitor, a route collector, a route server, or a route reflector.

Rotonda is and always
will be free, open-source software.

>### ROTONDA IS CURRENTLY IN ALPHA, DO NOT USE IN PRODUCTION
>
> Rotonda is being actively worked on and this repository and all the packages
> we supply are still in ALPHA stage. Use it to experiment freely (we value your
> feedback!), but do not use it with data and data-streams you cannot afford
> to lose.
>
> You should also be aware that all the APIs, configuration and the `roto`
> syntax and grammar are still (highly) unstable.
>
### INSTALLATION

There is no packaged, versioned release of `Rotonda` yet. You can install from
the main branch if you have a Rust toolchain installed.

First, you'll need some general build tools, to be able to download and
install `Rust`. On the command line issue this command, while making sure you
have enough privileges on the system to perform these actions:

```bash
apt install curl build-essential gcc make
```

This is for Debian based systems, on other distributions and/or operating
systems you will have to install these tools as well.

On most based systems you can then install `Rust` by issuing:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

and then finally you can build Rotonda directly from github:

```bash
cargo install rotonda --git https://github.com/NLnetLabs/rotonda
```

If you restart your shell, you can start Rotonda by simply typing:

```bash
rotonda
```

Note that rotonda will probably tell you it can't find its filters. Read the
documentation for more information about this. Also not that rotonda needs a
`Rust` version `1.71` or higher, which means that if you are using a Rust version
packaged by your distribution, that Rust version may be outdated.

Releases on `crates.io`, the online `Rust` library collection, and a proper
versioned release with `.deb` and `.rpm` packages is under way. If the above
installation method does not work for you, we advise you to wait for this
release.

### ROADMAP

`Rotonda` is under heavy development and should be considered **alpha**
software. See the [roadmap](ROADMAP.md) in this repository for more details.

### DOCUMENTATION

The documentation does not necessarily reflect the reality of `Rotonda` at this
stage. Features that appear in the documentation might only be partly
implemented — or not at all. Likewise, implemented features may be
undocumented. Before we make an official release we will clearly mark the
status of all features in the documentation.

Read the PRELIMINARY documentation [here](https://rotonda.docs.nlnetlabs.nl/).

### LICENSE

The rotonda crate is distributed under the terms of the MPL-2.0 license. See
[LICENSE](https://github.com/NLnetLabs/rotonda/blob/main/LICENSE) for details

Rotonda is and always will be free, open-source software.
