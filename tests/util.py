# -*- coding: utf-8 -*-

import os
import pytest
import shutil
import tempfile
import iris

from intake_iris.netcdf import NetCDFSource

TEST_DATA_DIR = 'tests/data'
TEST_DATA = 'example_1.nc'
TEST_URLPATH = os.path.join(TEST_DATA_DIR, TEST_DATA)


@pytest.fixture
def cdf_source():
    return NetCDFSource(TEST_URLPATH)


@pytest.fixture
def dataset():
    return iris.load(TEST_URLPATH)
