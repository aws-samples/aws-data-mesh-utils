import os

from setuptools import find_packages, setup

setup(
    install_requires=['boto3~=1.17.34',
                      'pystache~=0.5.4',
                      'setuptools~=54.1.2',
                      'shortuuid~=0.5.0',
                      'botocore~=1.20.34'],
    include_package_data=True
)
