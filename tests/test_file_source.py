# -*- coding: utf-8 -*-
import os
import pytest

from pkg_resources import get_distribution, parse_version

from intake_geopandas import (
    GeoJSONSource,
    GeoPandasFileSource,
    GeoParquetSource,
    ShapefileSource,
)
from geopandas import GeoDataFrame

geom_col_type = (
    "object"
    if parse_version(get_distribution("geopandas").version) < parse_version("0.6")
    else "geometry"
)


@pytest.fixture
def shape_filenames():
    basedir = os.path.dirname(__file__)
    return dict(stations=os.path.join(basedir, "data", "stations", "stations.shp"))


@pytest.fixture
def shape_datasource(shape_filenames):
    return ShapefileSource(shape_filenames["stations"])


@pytest.fixture
def geojson_filenames():
    basedir = os.path.dirname(__file__)
    return dict(countries=os.path.join(basedir, "data", "countries.geo.json"))


@pytest.fixture
def gpkg_filename():
    basedir = os.path.dirname(__file__)
    return os.path.join(basedir, "data", "countries.gpkg")


@pytest.fixture
def geoparquet_filename():
    basedir = os.path.dirname(__file__)
    return os.path.join(basedir, "data", "countries.parquet")


@pytest.fixture
def geojson_datasource(geojson_filenames):
    return GeoJSONSource(geojson_filenames["countries"])


def test_shape_datasource(shape_datasource):
    info = shape_datasource.discover()
    shape_datasource.read()
    assert info["dtype"] == {
        "name": "object",
        "marker-col": "object",
        "marker-sym": "object",
        "line": "object",
        "geometry": geom_col_type,
    }


def test_geojson_datasource(geojson_datasource):
    info = geojson_datasource.discover()
    geojson_datasource.read()
    assert info["dtype"] == {
        "geometry": geom_col_type,
        "id": "object",
        "name": "object",
    }


def test_alternative_ogr_driver(gpkg_filename):
    gpkg_datasource = GeoPandasFileSource(
        gpkg_filename,
        geopandas_kwargs={"driver": "GPKG"},
    )
    gdf = gpkg_datasource.read()
    assert isinstance(gdf, GeoDataFrame)


def test_geoparquet_source(geoparquet_filename):
    datasource = GeoParquetSource(geoparquet_filename)
    gdf = datasource.read()
    assert isinstance(gdf, GeoDataFrame)


def test_geoparquet_source_fsspec(geoparquet_filename):
    datasource = GeoParquetSource(geoparquet_filename, use_fsspec=True)
    gdf = datasource.read()
    assert isinstance(gdf, GeoDataFrame)


@pytest.mark.parametrize("use_fsspec", [True, False])
def test_geoparquet_to_dask(geoparquet_filename, use_fsspec):
    datasource = GeoParquetSource(geoparquet_filename, use_fsspec=use_fsspec)
    dgdf = datasource.to_dask()
    gdf = dgdf.compute()
    assert isinstance(gdf, GeoDataFrame)
