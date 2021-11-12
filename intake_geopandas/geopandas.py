# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod

import fsspec
import geopandas
from intake.source.base import DataSource, Schema

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


class GeoPandasSource(DataSource, ABC):
    """
    Base class intake source for loading GeoDataFrames.
    """

    version = __version__
    container = "dataframe"
    partition_access = True

    @abstractmethod
    def _open_dataset(self):
        """
        Open dataset using geopandas and use pattern fields to set new columns.
        """
        raise NotImplementedError("GeoPandasSource is an abstract class")

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
    name = "geopandasfile"

    def __init__(
        self,
        urlpath,
        use_fsspec=False,
        storage_options=None,
        bbox=None,
        geopandas_kwargs=None,
        metadata=None,
    ):
        """
        A source for a file opened by geopandas. Specializations of this are provided
        for shapefiles and geojson, but this base class can also be used directly
        for other file types by providing an OGR driver to `geopandas_kwargs`, e.g.
        `geopandas_kwargs={"driver": "GPKG"}` to open a geopackage.

        Parameters
        ----------
        urlpath : str or iterable, location of data
            Either the absolute or relative path to the file or URL to be
            opened. Some examples:
            - ``{{ CATALOG_DIR }}data/states.shp``
            - ``http://some.domain.com/data/states.geo.json``

        use_fsspec: bool
            Whether to use fsspec to open `urlpath`. By default, `urlpath` is passed
            directly to GeoPandas, which opens the file using `fiona`. However, for some
            use cases it may be beneficial to read the file using `fsspec` before
            passing the resulting bytes to GeoPandas (e.g., when using `fsspec`
            caching).
            Note that fiona/GDAL and `fsspec` have mutually-incompatible URL chaining
            syntaxes, so the URLs passed to each may be significantly different.

        storage_options: dict
            Storage options to pass to fsspec when opening. Only used when
            `use_fsspec=True`.

        bbox : tuple | GeoDataFrame or GeoSeries, default None
            Filter features by given bounding box, GeoSeries, or GeoDataFrame.
            CRS mis-matches are resolved if given a GeoSeries or GeoDataFrame.

        geopandas_kwargs : dict
            Any further arguments to pass to geopandas's read_file function.
        """
        self.urlpath = urlpath
        self._use_fsspec = use_fsspec
        self.storage_options = storage_options or {}
        self._bbox = bbox
        self._geopandas_kwargs = geopandas_kwargs or {}
        self._dataframe = None

        super().__init__(metadata=metadata)

    def _open_dataset(self):
        """
        Open dataset using geopandas.
        """
        if self._use_fsspec:
            with fsspec.open_files(self.urlpath, **self.storage_options) as f:
                f = self._resolve_single_file(f) if len(f) > 1 else f[0]
                self._dataframe = geopandas.read_file(
                    f,
                    bbox=self._bbox,
                    **self._geopandas_kwargs,
                )
        else:
            self._dataframe = geopandas.read_file(
                self.urlpath, bbox=self._bbox, **self._geopandas_kwargs
            )

    def _resolve_single_file(self, filelist):
        """
        Given a list of fsspec OpenFiles, choose one to pass to geopandas.
        """
        raise NotImplementedError(
            "Opening multiple files is not supported by this driver"
        )


class GeoJSONSource(GeoPandasFileSource):
    name = "geojson"


class ShapefileSource(GeoPandasFileSource):
    name = "shapefile"

    def _resolve_single_file(self, filelist):
        """
        Given a list of fsspec OpenFiles, find a .shp file.
        """
        local_files = fsspec.open_local(self.urlpath, **self.storage_options)
        for f in local_files:
            if f.endswith(".shp"):
                return f
        raise ValueError(
            f"No shapefile found in {filelist}, if you are using fsspec caching"
            " consider using same_names=True"
        )


class GeoParquetSource(GeoPandasFileSource):
    name = "geoparquet"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._df = None

    def _open_dataset(self):
        """
        Open dataset using geopandas.
        """
        if self._use_fsspec:
            with fsspec.open_files(self.urlpath, **self.storage_options) as f:
                f = self._resolve_single_file(f) if len(f) > 1 else f[0]
                self._dataframe = geopandas.read_parquet(
                    f,
                    **self._geopandas_kwargs,
                )
        else:
            self._dataframe = geopandas.read_parquet(
                self.urlpath, **self._geopandas_kwargs
            )

    def _get_schema(self):
        if self._df is None:
            self._df = self._to_dask()
        dtypes = {k: str(v) for k, v in self._df._meta.dtypes.items()}
        self._schema = Schema(
            datashape=None,
            dtype=dtypes,
            shape=(None, len(self._df.columns)),
            npartitions=self._df.npartitions,
            extra_metadata={},
        )
        return self._schema

    def to_dask(self):
        self._load_metadata()
        return self._df

    def read(self):
        return self.to_dask().compute()

    def _to_dask(self):
        """
        Create a lazy dask-geodataframe using dask-geopandas
        """
        import dask_geopandas

        self._df = dask_geopandas.read_parquet(
            self.urlpath, storage_options=self.storage_options, **self._geopandas_kwargs
        )
        self._load_metadata()
        return self._df


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
            self.sql_expr = f"SELECT * FROM {table}"
        else:
            raise ValueError("Must provide either a sql_expr or a table")

        self._geopandas_kwargs = geopandas_kwargs or {}
        self._dataframe = None

        super().__init__(metadata=metadata)

    def _open_dataset(self):
        self._dataframe = geopandas.read_postgis(
            self.sql_expr, self.uri, **self._geopandas_kwargs
        )


class PostGISSource(GeoPandasSQLSource):
    name = "postgis"


class SpatiaLiteSource(GeoPandasSQLSource):
    name = "spatialite"
