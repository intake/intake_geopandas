# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import os
import pytest

from intake import open_catalog


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, 'data', 'catalog.yaml'))


def test_catalog(catalog1, dataset):
    source = catalog1['test_dataset'].get()
    ds = source.read()

    assert isinstance(ds, pd.DataFrame)
