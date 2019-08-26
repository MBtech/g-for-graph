#!/bin/bash

SLAVES=$1

python mysar.py terminate $SLAVES
sleep 5
python mysar.py process $SLAVES
sleep 20
python mysar.py collectcsv $SLAVES $2
