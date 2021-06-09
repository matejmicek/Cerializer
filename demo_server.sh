#!/bin/sh

pip install -r requirements.txt
pip install -r dev_requirements.txt
python setup.py build_ext --inplace

clear

python -m cerializer_demo.demo_server "$1"