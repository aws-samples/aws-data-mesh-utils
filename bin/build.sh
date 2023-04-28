#!/bin/bash

if [ "$1" == "build" ]; then
  rm -Rf dist build && python3 setup.py install
  twine check dist/*
elif [ "$1" == "deploy-test" ]; then
  twine upload --repository-url https://test.pypi.org/legacy/ dist/*
elif [ "$1" == "deploy-prod" ]; then
  twine upload --repository pypi dist/*
else
  echo "Unknown Option or no input provided. Valid options are 'build', 'deploy-test', or 'deploy-prod'"
  exit -1
fi
