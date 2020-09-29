from intake.source.base import Schema

from .geopandas import GeoPandasFileSource


class RegionmaskSource(GeoPandasFileSource):
    name = 'regionmask'

    def __init__(
        self,
        urlpath,
        use_fsspec=False,
        storage_options=None,
        bbox=None,
        geopandas_kwargs=None,
        metadata=None,
        regionmask_kwargs=None,
    ):
        """
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
        regionmask_kwargs : dict
            Any further arguments to pass to regionmask.from_geopandas.
        """
        self._regionmask_kwargs = regionmask_kwargs or {}

        super().__init__(
            urlpath=urlpath,
            metadata=metadata,
            use_fsspec=use_fsspec,
            storage_options=storage_options,
            geopandas_kwargs=geopandas_kwargs,
            bbox=bbox,
        )

    def _open_dataset(self):
        try:
            import regionmask
        except ImportError:
            raise ImportError('please install regionmask')
        super()._open_dataset()
        self._dtypes = self._dataframe.dtypes.to_dict()
        self._dtypes = {n: str(t) for (n, t) in self._dtypes.items()}
        self._dataframe = regionmask.from_geopandas(
            self._dataframe, **self._regionmask_kwargs
        )

    def _get_schema(self):

        if self._dataframe is None:
            self._open_dataset()

        return Schema(
            datashape=None,
            dtype=self._dtypes,
            shape=(None, len(self._dtypes)),
            npartitions=1,
            extra_metadata={},
        )
