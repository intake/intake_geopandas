# -*- coding: utf-8 -*-
import os
import shutil

import pytest
from fiona.errors import DriverError

from intake_geopandas import GeoJSONSource, ShapefileSource


def try_clean_cache(item):
    c = None
    for c in ['blockcache', 'filecache', 'simplecache']:
        if c in item.storage_options:
            caching = c
    assert c is not None, 'caching not found'
    path = item.storage_options[caching]['cache_storage']
    if isinstance(path, str):
        if os.path.exists(path):
            shutil.rmtree(path)


@pytest.mark.parametrize(
    'url',
    [
        'http://maps.tnc.org/files/shp/MEOW-TNC.zip',
        'https://biogeo.ucdavis.edu/data/gadm3.6/shp/gadm36_ALA_shp.zip',
        'zip://gadm36_ALA_0*::https://biogeo.ucdavis.edu/data/gadm3.6/shp/'
        'gadm36_ALA_shp.zip',
    ],
    ids=['url_zip', 'url_2_shp_zip', 'url_2_shp_extract_one_from_zip'],
)
@pytest.mark.parametrize('strategy', ['simplecache', 'filecache'])
def test_different_cachings_and_url(url, strategy):
    """Test different caching strategies for different urls."""
    item = ShapefileSource(
        f'{strategy}::{url}',
        storage_options={strategy: {'same_names': True, 'cache_storage': 'tempfile'}},
    )
    print(item)
    expected_location = item.storage_options[strategy]['cache_storage']
    try_clean_cache(item)
    assert not os.path.exists(expected_location)
    item.read()
    assert os.path.exists(expected_location)
    try_clean_cache(item)


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
    if not same_names:  # initialization warning when same_names False
        with pytest.warns(UserWarning, match='same_names = True'):
            item = ShapefileSource(**ShapefileSource_args)
    else:  # no warning when same_names True
        item = ShapefileSource(**ShapefileSource_args)
    expected_location_on_disk = item.storage_options['simplecache']['cache_storage']
    try_clean_cache(item)
    assert not os.path.exists(expected_location_on_disk)
    if not same_names:
        # fiona expects paths ending with '.zip' or '.shp'
        with pytest.raises(DriverError):
            item.read()
    else:
        item.read()
    assert os.path.exists(expected_location_on_disk)
    try_clean_cache(item)
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
    try_clean_cache(item)
    assert not os.path.exists(expected_location_on_disk)
    item.read()
    assert os.path.exists(expected_location_on_disk)
    try_clean_cache(item)
    assert not os.path.exists(expected_location_on_disk)
