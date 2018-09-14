Quickstart
==========

``intake-iris`` provides quick and easy access to stored in iris files.

.. iris: https://scitools.org.uk/iris/docs/latest/

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c informaticslab intake-iris

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Note that iris sources do not yet support streaming from an Intake server.

Ad-hoc
~~~~~~

After installation, the functions ``intake.open_netcdf`` and ``intake.open_grib``
will become available. They can be used to open iris datasets.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

Catalog entries must specify ``driver: netcdf`` or ``driver: grib``,
as appropriate.


Using a Catalog
~~~~~~~~~~~~~~~

