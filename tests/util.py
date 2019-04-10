"""Modified from geopandas/tests/util.py"""

import sqlite3

from geopandas import GeoDataFrame
from pandas import Series


def validate_boro_df(df, case_sensitive=False):
    """Tests a GeoDataFrame that has been read in from the nybb dataset."""
    assert isinstance(df, GeoDataFrame)
    # Make sure all the columns are there and the geometries
    # were properly loaded as MultiPolygons
    assert len(df) == 5
    columns = ('BoroCode', 'BoroName', 'Shape_Leng', 'Shape_Area')
    if case_sensitive:
        for col in columns:
            assert col in df.columns
    else:
        for col in columns:
            assert col.lower() in (dfcol.lower() for dfcol in df.columns)
    assert Series(df.geometry.type).dropna().eq('MultiPolygon').all()


def get_srid(df):
    """Return srid from `df.crs`."""
    crs = df.crs
    return (int(crs['init'][5:]) if 'init' in crs
            and crs['init'].startswith('epsg:')
            else 0)


def connect_spatialite():
    """
    Return a memory-based SQLite3 connection with SpatiaLite
    enabled & initialized.

    `The sqlite3 module must be built with loadable extension support
    <https://docs.python.org/3/library/sqlite3.html#f1>`_ and
    `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`_
    must be available on the system as a SQLite module.
    Packages available on Anaconda meet requirements.

    Exceptions
    ----------
    ``AttributeError`` on missing support for loadable SQLite extensions
    ``sqlite3.OperationalError`` on missing SpatiaLite
    """
    try:
        with sqlite3.connect(':memory:') as con:
            con.enable_load_extension(True)
            con.load_extension('mod_spatialite')
            con.execute('SELECT InitSpatialMetaData(TRUE)')
    except Exception:
        con.close()
        raise
    return con


def create_spatialite(con, df):
    """
    Return a SpatiaLite connection containing the nybb table.

    Parameters
    ----------
    `con`: ``sqlite3.Connection``
    `df`: ``GeoDataFrame``
    """

    with con:
        geom_col = df.geometry.name
        srid = get_srid(df)
        con.execute('CREATE TABLE IF NOT EXISTS nybb '
                    '( ogc_fid INTEGER PRIMARY KEY'
                    ', borocode INTEGER'
                    ', boroname TEXT'
                    ', shape_leng REAL'
                    ', shape_area REAL'
                    ')')
        con.execute('SELECT AddGeometryColumn(?, ?, ?, ?)',
                    ('nybb',
                     geom_col, srid, df.geom_type.dropna().iat[0].upper())
                    )
        con.execute('SELECT CreateSpatialIndex(?, ?)', ('nybb', geom_col))
        sql_row = "INSERT INTO nybb VALUES(?, ?, ?, ?, ?, GeomFromText(?, ?))"
        con.executemany(sql_row,
                        ((None,
                          row.BoroCode,
                          row.BoroName,
                          row.Shape_Leng,
                          row.Shape_Area,
                          row.geometry.wkt if row.geometry
                          else None,
                          srid
                          ) for row in df.itertuples(index=False)))
    return con
