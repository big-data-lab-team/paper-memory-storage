#!/bin/bash

fs=$1
cond=$2
delay=$3
app="python /home/users/vhayots/paper-memory-storage/experiments/bigbrain-incrementation/increment.py"
efout="experiment_files/$cond-ef.sh"

rm $efout

for f in $fs/*
do
    bn="$(basename -- $f)"
    echo $bn $cond
    echo "$app $f $cond --benchmark_file $cond/$cond-$bn.out --delay $delay" >> $efout
done 
