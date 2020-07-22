# -*- coding: utf-8 -*-
import os
import shutil

import geopandas
import intake
import pytest
from fiona.errors import DriverError

from intake_geopandas import GeoJSONSource, ShapefileSource


def try_clean_old_cache(item):
    if isinstance(item.cache, list):
        if len(item.cache) > 0:
            if isinstance(item.cache[0], str):
                item.cache[0].clear_cache()


def try_clean_new_cache(item):
    c = None
    for c in ['blockcache', 'filecache', 'simplecache']:
        if c in item.storage_options:
            caching = c
    assert c is not None, 'caching not found'
    path = item.storage_options[caching]['cache_storage']
    if isinstance(path, str):
        if os.path.exists(path):
            shutil.rmtree(path)


@pytest.mark.parametrize('cat_item', ['MEOW', 'MEOW_new_cache', 'MEOW_old_cache'])
def test_cache(cat_item):
    """Test new fsspec and old intake caching."""
    item = intake.open_catalog('tests/data/shape.catalog.yaml')[cat_item]

    # delete previous caching
    if 'cache' in cat_item and 'old' in cat_item:
        try_clean_old_cache(item)
    elif 'new' in cat_item:
        try_clean_new_cache(item)
    # read new
    shp = item.read()
    assert isinstance(shp, geopandas.GeoDataFrame)
    # test caching
    if 'cache' in cat_item:
        if 'old' in cat_item:
            urlpath = item.urlpath
            expected_location_on_disk = item.cache[0]._path(urlpath)
        elif 'new' in cat_item:
            expected_location_on_disk = item.storage_options['simplecache'][
                'cache_storage'
            ]
        assert isinstance(expected_location_on_disk, str)
        assert os.path.exists(expected_location_on_disk)
        # delete caching
        if 'old' in cat_item:
            try_clean_old_cache(item)
        elif 'new' in cat_item:
            try_clean_new_cache(item)


@pytest.mark.parametrize('same_names', [False, True])
def test_same_name_required_else_warn(same_names):
    """Test that same_names is required to load zip file from cache. Warns during init
    if same_names is False for zip file."""
    ShapefileSource_args = {
        'urlpath': 'simplecache::http://maps.tnc.org/files/shp/MEOW-TNC.zip',
        'storage_options': {
            'simplecache': {'same_names': same_names, 'cache_storage': 'tmpfile'}
        },
    }
    if not same_names:
        with pytest.warns(UserWarning, match='same_names = True'):
            item = ShapefileSource(**ShapefileSource_args)
    else:
        item = ShapefileSource(**ShapefileSource_args)
    expected_location_on_disk = item.storage_options['simplecache']['cache_storage']
    try_clean_new_cache(item)
    assert not os.path.exists(expected_location_on_disk)
    if not same_names:
        with pytest.raises(DriverError):
            item.read()
    else:
        item.read()
    assert os.path.exists(expected_location_on_disk)
    try_clean_new_cache(item)
    assert not os.path.exists(expected_location_on_disk)


@pytest.fixture
def GeoJSONSource_countries_remote():
    url = (
        'simplecache::https://raw.githubusercontent.com/intake/'
        'intake_geopandas/master/tests/data/countries.geo.json'
    )
    return GeoJSONSource(
        **{
            'urlpath': url,
            'storage_options': {'simplecache': {'cache_storage': 'tempfile'}},
        }
    )


@pytest.mark.parametrize('same_names', [False, True])
def test_remote_GeoJSONSource(GeoJSONSource_countries_remote, same_names):
    """GeoJSONSource works with either `same_names` True or False."""
    item = GeoJSONSource_countries_remote
    item.storage_options['simplecache']['same_names'] = same_names
    expected_location_on_disk = item.storage_options['simplecache']['cache_storage']
    try_clean_new_cache(item)
    assert not os.path.exists(expected_location_on_disk)
    item.read()
    assert os.path.exists(expected_location_on_disk)
    try_clean_new_cache(item)
    assert not os.path.exists(expected_location_on_disk)
