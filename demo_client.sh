#!/bin/sh

poetry install

sleep 2

clear

python -m cerializer_demo.demo_client "$1"
