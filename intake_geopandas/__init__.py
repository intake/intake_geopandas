# -*- coding: utf-8 -*-
import intake

from ._version import get_versions
from .geopandas import (
    GeoJSONSource,
    GeoPandasFileSource,
    GeoParquetSource,
    PostGISSource,
    ShapefileSource,
    SpatiaLiteSource,
)
from .regionmask import RegionmaskSource

__version__ = get_versions()['version']
del get_versions

__all__ = [
    'GeoJSONSource',
    'GeoPandasFileSource',
    'GeoParquetSource',
    'PostGISSource',
    'ShapefileSource',
    'SpatiaLiteSource',
    'RegionmaskSource',
]
