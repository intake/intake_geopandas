# -*- coding: utf-8 -*-
import iris
from .base import DataSourceMixin


class NetCDFSource(DataSourceMixin):
    """Open a netCDF file with iris.

    Parameters
    ----------
    urlpath: str or list
        Path to source file. May include glob "*" characters. Must be a
        location in the local file-system or opendap url.
    """
    name = 'netcdf'

    def __init__(self, urlpath, iris_kwargs=None, metadata=None,
                 **kwargs):
        self.urlpath = urlpath
        self._kwargs = iris_kwargs or kwargs
        self._ds = None
        super(NetCDFSource, self).__init__(metadata=metadata)

    def _open_dataset(self):
        self._ds = iris.load(self.urlpath, **self._kwargs)
