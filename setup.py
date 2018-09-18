# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake_s3_manifests',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='S3 manifests plugin for Intake',
    url='https://github.com/informatics-lab/intake-s3-manifests',
    maintainer='Jacob Tomlinson',
    maintainer_email='jacob.tomlinson@informaticslab.co.uk',
    license='BSD',
    py_modules=['intake_s3_manifests'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    zip_safe=False, )
