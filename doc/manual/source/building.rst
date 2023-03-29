Building From Source
====================

In addition to meeting the :ref:`system requirements <installation:System
Requirements>`, these are things you need to build Rotonda: 

- Rust
- Item 2
- Item 3

You can run Rotonda on any operating system and CPU architecture where you
can fulfil these requirements.

Dependencies
------------

Some of the cryptographic primitives used by Rotonda require a C toolchain.
You also need Rust because that’s the programming language that Rotonda has
been written in.

C Toolchain
"""""""""""

Some of the libraries Rotonda depends on require a C toolchain to be
present. Your system probably has some easy way to install the minimum set of
packages to build from C sources. For example, this command will install
everything you need on Debian/Ubuntu:

.. code-block:: text

  apt install build-essential

If you are unsure, try to run :command:`cc` on a command line. If there is a
complaint about missing input files, you are probably good to go.

Rust
""""

The Rust compiler runs on, and compiles to, a great number of platforms,
though not all of them are equally supported. The official `Rust Platform
Support`_ page provides an overview of the various support levels.

While some system distributions include Rust as system packages, Rotonda
relies on a relatively new version of Rust, currently 1.60.0 or newer. We
therefore suggest to use the canonical Rust installation via a tool called
:program:`rustup`.

Assuming you already have :program:`curl` installed, you can install
:program:`rustup` and Rust by simply entering:

.. code-block:: text

  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Alternatively, visit the `Rust website
<https://www.rust-lang.org/tools/install>`_ for other installation methods.

Building and Updating
---------------------

In Rust, a library or executable program such as Rotonda is called a
*crate*. Crates are published on `crates.io
<https://crates.io/crates/rotonda>`_, the Rust package registry. Cargo is
the Rust package manager. It is a tool that allows Rust packages to declare
their various dependencies and ensure that you’ll always get a repeatable
build. 

Cargo fetches and builds Rotonda’s dependencies into an executable binary
for your platform. By default you install from crates.io, but you can for
example also install from a specific Git URL, as explained below.

Installing the latest Rotonda release from crates.io is as simple as
running:

.. code-block:: text

  cargo install --locked rotonda

The command will build Rotonda and install it in the same directory that
Cargo itself lives in, likely ``$HOME/.cargo/bin``. This means Rotonda
will be in your path, too.

Updating
""""""""

If you want to update to the latest version of Rotonda, it’s recommended
to update Rust itself as well, using:

.. code-block:: text

    rustup update

Use the ``--force`` option to overwrite an existing version with the latest
Rotonda release:

.. code-block:: text

    cargo install --locked --force rotonda

Installing Specific Versions
""""""""""""""""""""""""""""

If you want to install a specific version of
Rotonda using Cargo, explicitly use the ``--version`` option. If needed,
use the ``--force`` option to overwrite an existing version:
        
.. code-block:: text

    cargo install --locked --force rotonda --version 0.2.0-rc2

All new features of Rotonda are built on a branch and merged via a `pull
request <https://github.com/NLnetLabs/rotonda/pulls>`_, allowing you to
easily try them out using Cargo. If you want to try a specific branch from
the repository you can use the ``--git`` and ``--branch`` options:

.. code-block:: text

    cargo install --git https://github.com/NLnetLabs/rotonda.git --branch main
    
.. Seealso:: For more installation options refer to the `Cargo book
             <https://doc.rust-lang.org/cargo/commands/cargo-install.html#install-options>`_.

