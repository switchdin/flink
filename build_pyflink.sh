#!/bin/bash
cd flink-python
rm -r dist/*
python setup.py sdist bdist_wheel
cd apache-flink-libraries
rm -r dist/*
python setup.py sdist
cd ..
