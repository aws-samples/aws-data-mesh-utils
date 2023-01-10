import os

from setuptools import find_packages, setup

setup(
    install_requires=['boto3~=1.26.46',
                      'pystache~=0.6.0',
                      'setuptools~=65.6.3',
                      'shortuuid~=1.0.11',
                      'botocore~=1.29.46'],
    include_package_data=True
)
