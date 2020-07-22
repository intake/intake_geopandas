# -*- coding: utf-8 -*-
import os
import warnings
from abc import ABC, abstractmethod

import fsspec
import geopandas
import requests
from intake.source.base import DataSource, Schema

from . import __version__


class GeoPandasSource(DataSource, ABC):
    """
    Base class intake source for loading GeoDataFrames.
    """

    version = __version__
    container = 'dataframe'
    partition_access = True

    @abstractmethod
    def _open_dataset(self):
        """
        Open dataset using geopandas and use pattern fields to set new columns.
        """
        raise NotImplementedError('GeoPandasSource is an abstract class')

    def _get_schema(self):
        if self._dataframe is None:
            self._open_dataset()

        dtypes = self._dataframe.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(
            datashape=None,
            dtype=dtypes,
            shape=(None, len(dtypes)),
            npartitions=1,
            extra_metadata={},
        )

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe

    def read(self):
        self._get_schema()
        return self._dataframe

    def to_dask(self):
        raise NotImplementedError()

    def _close(self):
        self._dataframe = None


class GeoPandasFileSource(GeoPandasSource):
    def __init__(
        self,
        urlpath,
        bbox=None,
        geopandas_kwargs=None,
        storage_options=None,
        metadata=None,
    ):
        """
        Parameters
        ----------
        urlpath : str or iterable, location of data
            Either the absolute or relative path to the file or URL to be
            opened. Some examples:
            - ``{{ CATALOG_DIR }}data/states.shp``
            - ``http://some.domain.com/data/states.geo.json``
        bbox : tuple | GeoDataFrame or GeoSeries, default None
            Filter features by given bounding box, GeoSeries, or GeoDataFrame.
            CRS mis-matches are resolved if given a GeoSeries or GeoDataFrame.
        geopandas_kwargs : dict
            Any further arguments to pass to geopandas's read_file function.
        """
        self.urlpath = urlpath
        self._bbox = bbox
        self._geopandas_kwargs = geopandas_kwargs or {}
        self._dataframe = None
        self.storage_options = storage_options or {}

        # warn if using fsspec caching and same_names not True for zip files
        if 'cache' in self.urlpath and 'zip' in self.urlpath:
            same_names = False  # default
            # find different same_names setting
            for c in ['blockcache', 'filecache', 'simplecache']:
                if c in self.storage_options:
                    if 'same_names' in self.storage_options[c]:
                        same_names = self.storage_options[c]['same_names']
            if not same_names:
                warnings.warn(
                    'Need same_names = True for local caching of `zip` files.'
                )

        super().__init__(metadata=metadata)

    def _open_dataset(self):
        """
        Open dataset using geopandas and use pattern fields to set new columns.
        """
        if 'cache' in self.urlpath:  # new caching
            url = fsspec.open_local(self.urlpath, **self.storage_options)
            if not os.path.exists(url):
                # explicitly loading the file if not cached
                url = fsspec.open(self.urlpath, **self.storage_options)
        elif self.cache:  # old caching
            self.cache[0].load(self.urlpath)
            if isinstance(self.cache[0], str):
                url = self.cache[0]
            else:
                url = self.urlpath
        else:  # no caching
            url = self.urlpath

        # opening locally cached files, geopandas expects local zip paths with `zip://`
        # see https://geopandas.org/io.html
        try:
            if requests.head(url).status_code == 200:
                is_remote = True
            else:
                is_remote = False
        except (
            requests.exceptions.MissingSchema,
            requests.exceptions.InvalidSchema,
            requests.exceptions.ConnectionError,
        ):
            is_remote = False
        if not is_remote:
            # extract ending, expect url
            if '.' in self.urlpath.split('/')[-1]:
                ending = self.urlpath.split('.')[-1]
            else:
                ending = None
            if ending == 'zip':
                url = f'{ending}://{url}'
        print(f'Loading from {url}')
        self._dataframe = geopandas.read_file(
            url, bbox=self._bbox, **self._geopandas_kwargs
        )


class GeoJSONSource(GeoPandasFileSource):
    name = 'geojson'


class ShapefileSource(GeoPandasFileSource):
    name = 'shapefile'


class GeoPandasSQLSource(GeoPandasSource):
    def __init__(
        self, uri, sql_expr=None, table=None, geopandas_kwargs=None, metadata=None
    ):
        """
        Parameters
        ----------
        uri : str
            The connection string for the PostGIS database.
        sql_expr: str, optional
            The SQL expression used to load from the database.
            Must include either `sql_expr` or `table`.
        table: str, optional
            The table to load from the database.
            This is ignored if `sql_expr` is provided.
        geopandas_kwargs : dict
            Any further arguments to pass to geopandas's read_postgis function.
        """
        self.uri = uri
        if sql_expr:
            self.sql_expr = sql_expr
        elif table:
            self.sql_expr = f'SELECT * FROM {table}'
        else:
            raise ValueError('Must provide either a sql_expr or a table')

        self._geopandas_kwargs = geopandas_kwargs or {}
        self._dataframe = None

        super().__init__(metadata=metadata)

    def _open_dataset(self):
        self._dataframe = geopandas.read_postgis(
            self.sql_expr, self.uri, **self._geopandas_kwargs
        )


class PostGISSource(GeoPandasSQLSource):
    name = 'postgis'


class SpatiaLiteSource(GeoPandasSQLSource):
    name = 'spatialite'
