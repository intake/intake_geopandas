# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake_geopandas',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Geopandas plugin for Intake',
    url='https://github.com/intake/intake_geopandas',
    maintainer='Ian Rose',
    maintainer_email='ian.r.rose@gmail.com',
    license='BSD',
    py_modules=['intake_geopandas'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    entry_points={
        'intake.drivers': [
            'geojson = intake_geopandas.geopandas:GeoJSONSource',
            'geopandasfile = intake_geopandas.geopandas:GeoPandasFileSource',
            'geoparquet = intake_geopandas.geopandas:GeoParquetSource',
            'postgis = intake_geopandas.geopandas:PostGISSource',
            'shapefile = intake_geopandas.geopandas:ShapefileSource',
            'spatialite = intake_geopandas.geopandas:SpatiaLiteSource',
            'regionmask = intake_geopandas.regionmask:RegionmaskSource',
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    python_requires=">=3.6",
    include_package_data=True,
    install_requires=requires,
    extras_require={"arrow": ["pyarrow"], "regionmask":["regionmask"]},
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    zip_safe=False,
)
