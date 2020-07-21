# -*- coding: utf-8 -*-
import os
import pytest

from pkg_resources import get_distribution, parse_version

from intake_geopandas import GeoJSONSource, ShapefileSource

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


def test_countries_datasource(geojson_datasource):
    info = geojson_datasource.discover()
    geojson_datasource.read()
    assert info["dtype"] == {
        "geometry": geom_col_type,
        "id": "object",
        "name": "object",
    }


def test_cache():
    import intake
    item = intake.open_catalog('tests/data/shape.catalog.yaml')['MEOW_cache']
    if isinstance(item.cache[0], str):
        item.cache[0].clear_cache()
    # read new
    shp = item.read()
    urlpath = item.urlpath
    expected_location_on_disk = item.cache[0]._path(urlpath)
    assert isinstance(expected_location_on_disk, str)
    import geopandas
    assert isinstance(shp, geopandas.GeoDataFrame)
    import os
    assert os.path.exists(expected_location_on_disk)

