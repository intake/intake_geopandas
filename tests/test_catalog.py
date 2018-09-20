# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import os
import pytest

from intake import open_catalog


@pytest.fixture
def local_prefix_cat():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, 'data', 'local.catalog.yaml'))


@pytest.fixture
def s3_prefix_cat():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, 'data', 's3.catalog.yaml'))


def test_local_prefix_catalog(local_prefix_cat):
    source = local_prefix_cat['mogreps_g_manifest'].get()
    ds = source.read()
    assert isinstance(ds, pd.DataFrame)
    assert len(ds) == 622007


def test_s3_prefix_catalog(s3_prefix_cat):
    source = s3_prefix_cat['mogreps_g_manifest'].get()
    ds = source.read()
    assert isinstance(ds, pd.DataFrame)
