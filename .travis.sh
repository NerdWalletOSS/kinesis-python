#!/bin/sh

if [ ! -z "${TRAVIS_PULL_REQUEST}" ]; then
    if [ -z "$(git diff origin/master setup.py | grep '\+.*version=')" ]; then
        printf "\n\n\n\nBump version in setup.py!\n\n\n\n"
        exit 1
    fi
fi

pip install -e .
pip install codecov pytest pytest-mock
coverage run --source=src $(which py.test) test/
