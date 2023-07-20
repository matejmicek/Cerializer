#!/bin/sh
clear
echo 'Now, I will install all the necessary dependencies and then show a demo'
sleep 3

echo '------- INSTALLING DEPENDENCIES ---------'
sleep 3
poetry install
python setup.py build_ext --inplace


clear
echo '------- INSTALATION COMPLETE ---------'
echo 'showing demo in:'
echo '3'
sleep 1
echo '2'
sleep 1
echo '1'
sleep 1
clear
echo '----------- SHOWING DEMO -------------'
sleep 1
python -m cerializer_demo.demo_student_schema
