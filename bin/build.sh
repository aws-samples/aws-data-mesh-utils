#!/bin/bash

if [ "$1" == "build" ]; then
  rm -Rf dist build && python3 -m build
  twine check dist/*
elif [ "$1" == "deploy-test" ]; then
  twine upload --repository-url https://test.pypi.org/legacy/ dist/*
elif [ "$1" == "deploy-prod" ]; then
  twine upload --repository pypi dist/*
fi
