# -*- coding: utf-8 -*-
from .base import DataSourceMixin


class NetCDFSource(DataSourceMixin):
    """Open a netCDF file with iris.

    Parameters
    ----------
    urlpath: str or list
        Path to source file. May include glob "*" characters. Must be a
        location in the local file-system or opendap url.

    warnings: str (optional)
        Behaviour for the warning filter. Iris is very vocal with warnings
        and this parameter allows dataset authors to ignore warnings if they
        this it is appropriate.
        https://docs.python.org/3/library/warnings.html#the-warnings-filter
    """
    name = 'netcdf'

    def __init__(self, urlpath, warnings='default', iris_kwargs=None, metadata=None,
                 **kwargs):
        self.urlpath = urlpath
        self.warnings = warnings
        self._kwargs = iris_kwargs or kwargs
        self._ds = None
        super(NetCDFSource, self).__init__(metadata=metadata)
