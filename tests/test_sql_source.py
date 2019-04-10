"""
Tests for loading data from SQL data sources.
Modified from those in the GeoPandas test suite.

In order to run, SpatiaLite must be installed and configured.
"""
import pytest

import geopandas
from geopandas import read_file

from intake_geopandas import SpatiaLiteSource



@pytest.fixture
def df_nybb():
    nybb_path = geopandas.datasets.get_path('nybb')
    df = read_file(nybb_path)
    return df


# Expected to fail until there is a geopandas release
# with geopandas/geopandas#856
@pytest.mark.xfail
def test_read_spatialite_null_geom(df_nybb):
    """Tests that geometry with NULL is accepted."""
    try:
        from geopandas.tests.util import (
            connect_spatialite, create_spatialite, validate_boro_df
        )
        con = connect_spatialite()
    except Exception:
        raise pytest.skip()
    else:
        geom_col = df_nybb.geometry.name
        df_nybb.geometry.iat[0] = None
        create_spatialite(con, df_nybb)
        sql = ('SELECT ogc_fid, borocode, boroname, shape_leng, shape_area, '
               'AsEWKB("{0}") AS "{0}" FROM nybb').format(geom_col)
        df = SpatiaLiteSource(con, sql_expr=sql, geopandas_kwargs={
                              'geom_col': geom_col}).read()
        validate_boro_df(df)
    finally:
        if 'con' in locals():
            con.close()

# Expected to fail until there is a geopandas release
# with geopandas/geopandas#856
@pytest.mark.xfail
def test_read_spatialite_binary(df_nybb):
    """Tests that geometry read as binary is accepted."""
    try:
        from geopandas.tests.util import (
            connect_spatialite, create_spatialite, validate_boro_df
        )
        con = connect_spatialite()
    except Exception:
        raise pytest.skip()
    else:
        geom_col = df_nybb.geometry.name
        create_spatialite(con, df_nybb)
        sql = ('SELECT ogc_fid, borocode, boroname, shape_leng, shape_area, '
               'ST_AsBinary("{0}") AS "{0}" FROM nybb').format(geom_col)
        df = SpatiaLiteSource(con, sql_expr=sql, geopandas_kwargs={
                              'geom_col': geom_col}).read()
        validate_boro_df(df)
    finally:
        if 'con' in locals():
            con.close()
