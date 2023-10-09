Rotonda |version|
=================

Roll your own BGP applications with programmable RIBs and Filters.

BGP applications that you can build with Rotonda include, but are not
limited to, a route monitor|collector|server|reflector, all this without
modifying a single line in the source code of Rotonda. Rotonda is and
always will be free, open-source software.

   .. DANGER:: ROTONDA IS CURRENTLY IN ALPHA, DO NOT USE IN PRODUCTION
      :name: rotonda-is-currently-in-alpha-do-not-use-in-production

      Rotonda is being actively worked on and this repository and all the
      packages we supply are still in ALPHA stage. Use it to experiment
      freely (we value your feedback!), but do not use it with data and
      data-streams you cannot afford to lose.

      You should also be aware that all the APIs, configuration and the
      ``roto`` syntax and grammar are still (highly) unstable.

      For more information see the `ROADMAP <https://github.com/NLnetLabs/rotonda/blob/main/ROADMAP.md>`__

Modular
   Rotonda applications are built by combining units into a pipeline
   through which BGP data will flow. You can filter, modify and store the
   BGP data along the way, and create signals based on it to send to other
   applications. Units can be added and removed in a *running* Rotonda
   application.

   Rotonda offers units to create BGP and BMP sessions, filters, Routing
   Information Bases (RIBs), and more.

Flexible
   The behavior of the units can be modeled by using a small, fun
   programming language called ``Roto``, that we created to combine
   flexibility and ease-of-use. ``Roto`` lets you configure a Rotonda
   application, program units and create queries. ``Roto`` scripts can be
   created with your favorite text editor, but they can also be composed
   from the command line that’s included in Rotonda.

Tailored Performance
   All data structures come with a trade-off between space and time, and
   Rotonda is no exception. Rotonda therefore offers units that perform the
   same task, but with different performance characteristics, so that you
   can optimize for your needs, be it a high-volume, low latency
   installation or a small installation in a constraint environment. None
   of this requires patching the Rotonda source, it’s all configurable with
   a nimble ``Roto`` script.

   Although Rotonda is still in alpha, these performance-critical parts
   have been battle-tested by, and are indeed being used in, large
   production environments.

Observable
   All Rotonda units have their own finely-grained logging capabilities,
   and some have built-in queryable JSON API interfaces to give information
   about their current state and content through Rotonda’s built-in HTTPS
   server. Signals can be sent to other applications. Moreover, Rotonda
   offers true observability by allowing the user to trace BMP/BGP packets
   start-to-end through the whole pipeline.

Storage Persistence
   By default a Rotonda application stores all the data that you want to
   collect in memory. It can be configured to persist parts to another
   storage location, such as files or a database. Whether you put RIBs to
   files or in a database, you can still query it transparently with
   ``Roto``.

External Data Sources
   ``Roto`` filter units can make decisions based on real-time external data
   sources, and RIB units can store them as needed. Similarly filter units
   can make decisions based on data present in multiple RIBs. External data
   sources can be, among others, files, databases or even a RIB backed by
   an RTR connection.

Robust & Scalable
   Multiple Rotonda instances can synchronize or shard data via our
   AVRO-based ``rotoro`` protocol, to create robust redundancy and/or
   scalability. Again you can still query all the distributed instances
   with ``Roto``.

Secure
   Rotonda applications can use data provided by the RPKI through connections
   with tools like Routinator and Krill. Besides that, Rotonda supports
   BGPsec out of the box. Again, no patching or recompiling required.

Open-source with professional support services
   NLnet Labs offers `professional support and consultancy
   services <https://www.nlnetlabs.nl/services/contracts/>`__ with a
   service-level agreement. Rotonda is liberally licensed under the
   [Mozilla Public License 2.0]
   (https://github.com/NLnetLabs/rotonda/blob/main/LICENSE).

   .. only:: html

      |discord| |mastodon|
      
      .. |discord| image:: https://img.shields.io/discord/818584154278199396   
         :alt: Discord
         :target: https://discord.gg/8dvKB5Ykhy

      .. |mastodon| image:: https://img.shields.io/mastodon/follow/109262826617293067?domain=https%3A%2F%2Ffosstodon.org&style=social
         :alt: Mastodon
         :target: https://fosstodon.org/@nlnetlabs

.. image:: img/rotonda-illustrative-icon.png
   :width: 60%
   :align: center
   :alt: Rotonda

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Getting Started
   :name: toc-getting-started

   installation
   building

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Quick Tour
   :name: toc-quick-tour
   :glob:

   quicktour/*

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Configuration
   :name: toc-configuration
   :glob:
   
   config/*

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Units
   :name: toc-units
   :glob:

   units/*

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Roto Language
   :name: toc-roto
   :glob:

   roto/*
