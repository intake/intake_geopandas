# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake_geopandas',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Geopandas plugin for Intake',
    url='https://github.com/informatics-lab/intake_geopandas',
    maintainer='Jacob Tomlinson',
    maintainer_email='jacob.tomlinson@informaticslab.co.uk',
    license='BSD',
    py_modules=['intake_geopandas'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    entry_points={
        'intake.drivers': [
            'geojson = intake_geopandas.geopandas:GeoJSONSource',
            'postgis = intake_geopandas.geopandas:PostGISSource',
            'shapefile = intake_geopandas.geopandas:ShapefileSource',
            'spatialite = intake_geopandas.geopandas:SpatiaLiteSource',
            'regionmask = intake_geopandas.regionmask:RegionmaskSource',
        ]
    },
    include_package_data=True,
    install_requires=requires,
    extra_require=['regionmask'],
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    zip_safe=False,
)
