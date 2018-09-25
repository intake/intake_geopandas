# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import os
import pytest
from unittest.mock import patch
from intake import open_catalog
import dask.dataframe as dd


@pytest.fixture
def local_prefix_cat():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, 'data', 'local.catalog.yaml'))


def test_local_prefix_catalog(local_prefix_cat):
    source = local_prefix_cat['mogreps_g_manifest'].get()
    ds = source.read()
    assert isinstance(ds, pd.DataFrame)
    assert len(ds) == 622007


def test_s3_prefix_catalog():

    orig_read_csv = dd.read_csv

    def read_csv(path, *args, **kwargs):
        return orig_read_csv(_s3_url_to_local(path), *args, **kwargs)

    def _s3_url_to_local(url):
        return url.replace('s3://', './tests/data/')

    def opener(url, mode='r'):
        return open(_s3_url_to_local(url), mode)

    with patch('s3fs.S3FileSystem') as mockopen, patch('dask.dataframe.read_csv') as mock_read_csv:
        # patch('intake_s3_manifests.s3_manifest') as s3_manifest,

        mockopen.return_value.open.side_effect = opener
        mock_read_csv.side_effect = read_csv
        path = os.path.dirname(__file__)
        s3_prefix_cat = open_catalog(os.path.join(path, 'data', 's3.catalog.yaml'))
        source = s3_prefix_cat['mogreps_g_manifest'].get()
        ds = source.read()
        assert isinstance(ds, pd.DataFrame)
        assert len(ds) == 622007
