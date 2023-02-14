#!/usr/bin/env python
from setuptools import setup, find_packages

with open('README.rst') as readme:
    readme = readme.read()

setup(
    name="s3lib",
    version='0.4.0',
    author="Amogha hegde",
    author_email="amogha.hegde@affinsys.com",
    maintainer="Amogha Hegde",
    maintainer_email="amogha.hegde@affinsys.com",
    keywords="python, django, botominio",
    description="Test Lib",
    long_description=readme,
    url="https://github.com/Amogha-Affinsys/bud-lib-s3",
    download_url="https://github.com/Amogha-Affinsys/bud-lib-s3/releases",
    bugtrack_url="https://github.com/Amogha-Affinsys/bud-lib-s3/issues",
    package_dir={'': 'src'},
    packages=find_packages('src', exclude="tests"),
    package_data={'django_clamd': [
        'locale/*/LC_MESSAGES/*.po',
        'locale/*/LC_MESSAGES/*.mo',
    ]},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Framework :: Django",
        "Framework :: Django :: 3.2",
        "Framework :: Django :: 4.16",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.9",
    ],
    install_requires=(
        "boto3",
        "Django>=3",
    ),
    zip_safe=False,
    include_package_data=True,
)