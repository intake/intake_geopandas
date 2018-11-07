# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import os
import pytest
from unittest.mock import patch

from intake_geopandas import geopandas


@pytest.fixture
def data_filenames():
    basedir = os.path.dirname(__file__)
    return dict(stations=os.path.join(basedir, 'data', 'stations', 'stations.shp'))


@pytest.fixture
def stations_datasource(data_filenames):
    return geopandas.ShapeSource(data_filenames['stations'])


def test_stations_datasource(stations_datasource):
    data = stations_datasource.read()

    assert 'name' in data
    assert 'marker-col' in data
    assert 'marker-sym' in data
    assert 'line' in data
    assert 'geometry' in data
