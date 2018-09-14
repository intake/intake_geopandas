from . import __version__
from intake.source.base import DataSource, Schema

import iris
from iris.cube import CubeList


class DataSourceMixin(DataSource):
    """Common behaviours for plugins in this repo"""
    version = __version__
    container = 'iris'
    partition_access = True

    def _open_dataset(self):
        self._ds = iris.load(self.urlpath, **self._kwargs)

    def _get_schema(self):
        """Make schema object, which embeds iris object and some details"""
        if self._ds is None:
            self._open_dataset()

            metadata = {}
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata)
        return self._schema

    def read(self):
        """Return a version of the iris cube/cubelist with all the data in memory"""
        self._load_metadata()
        if isinstance(self._ds, CubeList):
            self._ds.realise_data()
        else:
            _ = self._ds.data
        return self._ds

    def read_chunked(self):
        """Return iris object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def read_partition(self, i):
        """Fetch one chunk of data at tuple index i
        """

        import numpy as np
        self._load_metadata()
        if not isinstance(i, (tuple, list)):
            raise TypeError('For iris sources, must specify partition as '
                            'tuple')
        if isinstance(i, list):
            i = tuple(i)
        if isinstance(self._ds, CubeList):
            arr = self._ds[i[0]].lazy_data()
            i = i[1:]
        else:
            arr = self._ds.lazy_data()
        if isinstance(arr, np.ndarray):
            return arr
        # dask array
        return arr[i].compute()

    def to_dask(self):
        """Return iris object where variables are dask arrays"""
        return self.read_chunked()

    def close(self):
        """Delete open file from memory"""
        self._ds = None
        self._schema = None
