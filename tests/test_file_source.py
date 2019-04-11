# -*- coding: utf-8 -*-
import os
import pytest

from intake_geopandas import GeoJSONSource, ShapefileSource


@pytest.fixture
def shape_filenames():
    basedir = os.path.dirname(__file__)
    return dict(
        stations=os.path.join(basedir, 'data', 'stations', 'stations.shp')
    )


@pytest.fixture
def shape_datasource(shape_filenames):
    return ShapefileSource(shape_filenames['stations'])


@pytest.fixture
def geojson_filenames():
    basedir = os.path.dirname(__file__)
    return dict(countries=os.path.join(basedir, 'data', 'countries.geo.json'))


@pytest.fixture
def geojson_datasource(geojson_filenames):
    return GeoJSONSource(geojson_filenames['countries'])


def test_shape_datasource(shape_datasource):
    info = shape_datasource.discover()

    assert info['dtype'] == {'name': 'object',
                             'marker-col': 'object',
                             'marker-sym': 'object',
                             'line': 'object',
                             'geometry': 'object'}


def test_countries_datasource(geojson_datasource):
    info = geojson_datasource.discover()
    print(info)
