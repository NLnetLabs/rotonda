Manual Page
===========

Synopsis
--------

:program:`rotonda` [``options``] [:samp:`-o {output-file}`] [:samp:`-f {format}`]

:program:`rotonda` ``-h``

:program:`rotonda` ``-V``

Description
-----------

Rotonda consists of a set of types of components that can be mixed and
matched to build a BGP routing service. You can build one big stand-alone
application, or you can build a set of distributed runtimes that are
communicating through the rotoro ("Rotonda to Rotonda") protocol.

For all available commands, see `COMMANDS`_ below.

Options
-------

The available options are:

.. option:: -v, --verbose

      Print more information. If given twice, even more information is
      printed.

      More specifically, a single :option:`-v` increases the log level from
      the default of *warn* to *info*, specifying it more than once increases
      it to *debug*.
      
      See `LOGGING`_ below for more information on what information is logged
      at the different levels.

.. option:: -q, --quiet

      Print less information. Given twice, print nothing at all.

      A single :option:`-q` will drop the log level to *error*. Repeating
      :option:`-q` more than once turns logging off completely.

.. option:: --syslog

      Redirect logging output to syslog.

      This option is implied if a command is used that causes Rotonda to
      run in daemon mode.

.. option:: --syslog-facility=facility

      If logging to syslog is used, this option can be used to specify the
      syslog facility to use. The default is *daemon*.

.. option:: --logfile=path

      Redirect logging output to the given file.

.. option:: -h, --help

      Print some help information.

.. option:: -V, --version

      Print version information.

Commands
--------

Rotonda provides a number of operations. These can be requested by providing
different commands on the command line.

.. subcmd:: foo

    Description bar.

    .. option:: -o file, --output=file

           Specifies the output file to write the list to. If this option is
           missing or file is ``-`` the list is printed to standard output.

    .. option:: -f format, --format=format

           The output format to use. Rotonda currently supports the
           following formats:

           csv
                  The list is formatted as lines of comma-separated values.

           json
                  The list is placed into a JSON object.

Configuration File
------------------

Instead of providing all options on the command line, they can also be
provided through a configuration file. Such a file can be selected through
the :option:`-c` option. If no configuration file is specified this way but a
file named :file:`$HOME/.rotonda.conf` is present, this file is used.

The configuration file is a file in TOML format. In short, it consists of a
sequence of key-value pairs, each on its own line. Strings are to be enclosed
in double quotes. Lists can be given by enclosing a comma-separated list of
values in square brackets.

The configuration file can contain the following entries. All path values are
interpreted relative to the directory the configuration file is located in.
All values can be overridden via the command line options.

.. Glossary::

      log-level
            A string value specifying the maximum log level for which log
            messages should be emitted. The default is *warn*.

            See `LOGGING`_ below for more information on what information is
            logged at the different levels.

      log
            A string specifying where to send log messages to. This can be
            one of the following values:

            default
                  Log messages will be sent to standard error if Rotonda
                  stays attached to the terminal or to syslog if it runs in
                  daemon mode.

            stderr
                  Log messages will be sent to standard error.

            syslog
                  Log messages will be sent to syslog.

            file
                  Log messages will be sent to the file specified through
                  the log-file configuration file entry.

            The default if this value is missing is, unsurprisingly,
            *default*.

      log-file
            A string value containing the path to a file to which log
            messages will be appended if the log configuration value is set
            to file. In this case, the value is mandatory.

      syslog-facility
            A string value specifying the syslog facility to use for logging
            to syslog. The default value if this entry is missing is
            *daemon*.

      pid-file
            A string value containing a path pointing to the PID file to be
            used in daemon mode.

      working-dir
            A string value containing a path to the working directory for the
            daemon process.

      chroot
            A string value containing the path any daemon process should use
            as its root directory.

      user
            A string value containing the user name a daemon process should
            run as.

      group
            A string value containing the group name a daemon process should
            run as.

Logging
-------

In order to allow diagnosis of all data, as well as its overall health,
Rotonda logs an extensive amount of information. The log levels used by
syslog are utilized to allow filtering this information for particular use
cases.

The log levels represent the following information:

error
      Information related to events that prevent Rotonda from continuing
      to operate at all as well as all issues related to local configuration
      even if Rotonda will continue to run.

warn
      Information about events and data that influences the set of VRPs
      produced by Rotonda. This includes failures to communicate with
      repository servers, or encountering invalid objects.

info
      Information about events and data that could be considered abnormal but
      do not influence the set of VRPs produced. For example, when filtering
      of unsafe VRPs is disabled, the unsafe VRPs are logged with this level.

debug
      Information about the internal state of Rotonda that may be useful
      for, well, debugging.

