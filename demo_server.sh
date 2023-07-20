#!/bin/sh

poetry install

clear

python -m cerializer_demo.demo_server "$1"
