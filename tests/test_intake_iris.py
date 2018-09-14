# -*- coding: utf-8 -*-
import os
import numpy as np
import pytest

import intake

here = os.path.dirname(__file__)

from .util import TEST_URLPATH, cdf_source, dataset  # noqa


@pytest.mark.parametrize('source', ['cdf'])
def test_discover(source, cdf_source, dataset):
    source = {'cdf': cdf_source}[source]
    r = source.discover()

    assert r['datashape'] is None
    assert r['dtype'] is None
    assert r['metadata'] is not None

    assert source.datashape is None
