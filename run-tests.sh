#!/bin/bash
set -e
echo "pychecker"
echo "---------"
echo
#pychecker --only --keepgoing --no-reimport --no-reimportself --no-import --no-pkgimport *.py
pychecker --only memon.py
pychecker --only --no-miximport memon.py

echo "pep8 style check"
echo "---------"
echo
pep8 *.py 

echo "Running unit tests"
echo "---------"
echo
python -m unittest discover -v
