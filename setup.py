import os

from setuptools import find_packages, setup

setup(
    install_requires=['boto3~=1.20.51',
                      'pystache~=0.6.0',
                      'setuptools~=60.8.1',
                      'shortuuid~=1.0.8',
                      'botocore~=1.23.51'],
    include_package_data=True
)
