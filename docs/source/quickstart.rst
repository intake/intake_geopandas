Quickstart
==========

``intake-s3-manifests`` provides quick and easy access to data stored S3 inventory manifests.

.. S3 inventory manifests: https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html#storage-inventory-location-manifest

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c informaticslab intake-s3-manifests

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Note that S3 inventory manifests sources do not yet support streaming from an Intake server.

Ad-hoc
~~~~~~

After installation, the function ``intake.open_s3_manifest``
will become available. They can be used to open S3 inventory manifests datasets.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

Catalog entries must specify ``driver: s3-manifest``.


Using a Catalog
~~~~~~~~~~~~~~~

