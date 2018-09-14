# -*- coding: utf-8 -*-
import os
import numpy as np
import pytest

import intake

here = os.path.dirname(__file__)

from .util import TEST_URLPATH, cdf_source, dataset  # noqa


@pytest.mark.parametrize('source', ['cdf', 'zarr'])
def test_discover(source, cdf_source, zarr_source, dataset):
    source = {'cdf': cdf_source, 'zarr': zarr_source}[source]
    r = source.discover()

    assert r['datashape'] is None
    assert r['dtype'] is None
    assert r['metadata'] is not None

    assert source.datashape is None
    assert source.metadata['dims'] == dict(dataset.dims)
    assert set(source.metadata['data_vars']) == set(dataset.data_vars.keys())
    assert set(source.metadata['coords']) == set(dataset.coords.keys())


@pytest.mark.parametrize('source', ['cdf', 'zarr'])
def test_read(source, cdf_source, zarr_source, dataset):
    source = {'cdf': cdf_source, 'zarr': zarr_source}[source]

    ds = source.read_chunked()
    assert ds.temp.chunks

    ds = source.read()
    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)


def test_read_partition_cdf(cdf_source):
    source = cdf_source
    with pytest.raises(TypeError):
        source.read_partition(None)
    out = source.read_partition(('temp', 0, 0, 0, 0))
    d = source.to_dask()['temp'].data
    expected = d[:1, :4, :5, :10].compute()
    assert np.all(out == expected)


@pytest.mark.parametrize('source', ['cdf', 'zarr'])
def test_to_dask(source, cdf_source, zarr_source, dataset):
    source = {'cdf': cdf_source, 'zarr': zarr_source}[source]
    ds = source.to_dask()

    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)


def test_grib_dask():
    pytest.importorskip('Nio')
    import dask.array as da
    cat = intake.open_catalog(os.path.join(here, 'data', 'catalog.yaml'))
    x = cat.grib.to_dask()
    assert len(x.fileno) == 2
    assert isinstance(x.APCP_P8_L1_GLL0_acc6h.data, da.Array)
    values = x.APCP_P8_L1_GLL0_acc6h.data.compute()
    x2 = cat.grib.read()
    assert (values == x2.APCP_P8_L1_GLL0_acc6h.values).all()
