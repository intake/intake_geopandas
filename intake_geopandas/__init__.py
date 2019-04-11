# -*- coding: utf-8 -*-
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from .geopandas import (
    GeoJSONSource,
    PostGISSource,
    ShapefileSource,
    SpatiaLiteSource
)
__all__ = [
    'GeoJSONSource',
    'PostGISSource',
    'ShapefileSource',
    'SpatiaLiteSource'
]
