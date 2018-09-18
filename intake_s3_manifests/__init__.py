from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import json
import dask.dataframe as dd
from datetime import datetime, timedelta

import intake.container
from intake.source.base import Schema
from intake.source.csv import CSVSource


class S3ManifestSource(CSVSource):
    """Common behaviours for plugins in this repo"""
    version = __version__
    container = 'dataframe'
    partition_access = True
    name = 's3-manifest'

    def __init__(self, bucket, manifest_date='latest', s3_prefix='s3://', s3_manifest_kwargs=None, metadata=None,
                 extract_key_regex=None, storage_options=None):
        """
        Parameters
        ----------
        bucket : str
            The S3 bucket to for which you want to load the manifest.
        manifest_date: str
            The date of the manifest you wish to load in for format `YYYY-MM-DD`. Defaults to `latest` which will
            load the most recent manifest.
        s3_prefix: str
            The prefix for accessing S3. Defaults to the `s3://` protocol. If you are using fuse for example you may
            want to set this to the mount point of the bucket.
        s3_manifest_kwargs : dict
            Any further arguments to pass to Dask's read_csv (such as block size)
            or to the `CSV parser <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html>`_
            in pandas (such as which columns to use, encoding, data-types)
        extract_key_regex: string
            Pandas is able to extract information from a composite string using regular expressions. If the objects
            in your bucket follow a strict naming convension you can provide a regular expression with named groups
            to extract the information from the key into separate columns.
            e.g if your bucket contains images from an ecommerce website they may follow the format
            `<category>_<item name>_<item_id>.jpg` which you could extract using the expression
            `(?P<Category>.*)_(?P<Name>.*)_(?P<ID>..).jpg`.
        storage_options : dict
            Any parameters that need to be passed to the remote data backend,
            such as credentials.
        """
        self._bucket = bucket
        self._manifest_date = manifest_date
        if self._manifest_date == 'latest':
            self._manifest_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self._s3_prefix = s3_prefix
        self._urlpath = '{prefix}{bucket}/manifest/{date}/manifest.json'.format(prefix=self._s3_prefix,
                                                                                bucket=self._bucket,
                                                                                date=self._manifest_date)
        self._extract_key_regex = r'%s' % extract_key_regex
        self._storage_options = storage_options
        self._s3_manifest_kwargs = s3_manifest_kwargs or {}
        self._dataframe = None

    def _open_dataset(self):

        with open(self._urlpath) as f:
            manifest_meta = json.load(f)
            manifests = [file['key'] for file in manifest_meta['files']]

            df = dd.concat([dd.read_csv('{prefix}{bucket}/{key}'.format(prefix=self._s3_prefix, bucket=manifest_meta['sourceBucket'], key=manifest), names=['Bucket', 'Key', 'Size', 'Created']) for manifest in manifests])
            df = df[~df['Key'].str.contains("manifest/")]
            if self._extract_key_regex is not None:
                metadata = df.Key.str.extract(self._extract_key_regex, expand=False)
                df = dd.concat([df, metadata], axis=1, sort=False)

        self._dataframe = df

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe.get_partition(i).compute()
