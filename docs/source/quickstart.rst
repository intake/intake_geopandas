Quickstart
==========

``intake_geopandas`` provides quick and easy access to data with geopandas.

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c informaticslab intake-_geopandas

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Note that geopandas manifests sources do not yet support streaming from an Intake server.

Ad-hoc
~~~~~~

After installation, the function ``intake.open_shape``
will become available. They can be used to open Shapefiles.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

Catalog entries must specify ``driver: shape``.


Using a Catalog
~~~~~~~~~~~~~~~

