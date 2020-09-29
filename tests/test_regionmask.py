# -*- coding: utf-8 -*-
import os

import geopandas
import intake
import pytest

from intake_geopandas import RegionmaskSource

try:
    import regionmask

    regionmask_installed = True
except ImportError:
    regionmask_installed = False

geopandas_version_allows_fsspec_caching = (
    int(geopandas.__version__[:5].replace('.', '')) > 81
)  # checks geopandas larger than 0.8.1


@pytest.mark.skipif(not regionmask_installed, reason='regionmask needs to be installed')
@pytest.mark.parametrize(
    'url',
    [
        'http://maps.tnc.org/files/shp/MEOW-TNC.zip',
        'https://biogeo.ucdavis.edu/data/gadm3.6/shp/gadm36_ALA_shp.zip',
    ],
    ids=['url_zip', 'url_2_shp_zip'],
)
def test_regionmask(url):
    """Test regionmask with no regionmask_kwargs."""
    item = RegionmaskSource(url,)
    region = item.read()
    assert isinstance(region, regionmask.Regions), print(type(region))


@pytest.mark.skipif(not regionmask_installed, reason='regionmask needs to be installed')
@pytest.mark.parametrize(
    'key,value',
    [
        ('names', 'ECOREGION'),
        ('source', 'some url'),
        ('name', 'MEOW'),
        ('numbers', 'ECO_CODE_X'),
        ('abbrevs', 'ECO_CODE'),
    ],
)
def test_regionmask_kwargs(key, value):
    """Test regionmask_kwargs."""
    item = RegionmaskSource(
        urlpath='http://maps.tnc.org/files/shp/MEOW-TNC.zip',
        regionmask_kwargs={key: value},
    )
    region = item.read()
    assert isinstance(region, regionmask.Regions), print(type(region))

    if key == 'names':
        # no default region names
        assert 'Region0' not in getattr(region, key)
    elif key == 'numbers':
        # no default region numbering
        assert 0 not in getattr(region, key), print(region)
    elif key == 'abbrevs':
        # no default region abbrevs
        assert 'r0' not in getattr(region, key), print(region)
    else:  # source
        assert getattr(region, key) == value, print(region)


@pytest.mark.skipif(not regionmask_installed, reason='regionmask needs to be installed')
def test_regionmask_yaml():
    cat = intake.open_catalog('tests/data/shape.catalog.yaml')
    meow_regions = cat['MEOW_regionmask'].read()
    assert isinstance(meow_regions, regionmask.Regions), print(type(meow_regions))


@pytest.mark.skipif(
    not geopandas_version_allows_fsspec_caching,
    reason='requires geopandas release after 0.8.1',
)
@pytest.mark.skipif(not regionmask_installed, reason='regionmask needs to be installed')
def test_regionmask_yaml_cache():
    cat = intake.open_catalog('tests/data/shape.catalog.yaml')
    item = cat['MEOW_regionmask_cache']
    meow_regions = item.read()
    assert isinstance(meow_regions, regionmask.Regions), print(type(meow_regions))
    expected_location = item.storage_options['simplecache']['cache_storage']
    assert os.path.exists(expected_location)
    from .test_remote_cache import try_clean_cache

    try_clean_cache(item)
    assert not os.path.exists(expected_location)
    meow_regions = item.read()
    assert isinstance(meow_regions, regionmask.Regions), print(type(meow_regions))
