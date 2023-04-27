import os

from setuptools import find_packages, setup

setup(
    install_requires=['boto3~=1.26.121',
                      'pystache~=0.6.0',
                      'setuptools~=67.7.2',
                      'shortuuid~=1.0.11',
                      'botocore~=1.29.121'],
    include_package_data=True
)
