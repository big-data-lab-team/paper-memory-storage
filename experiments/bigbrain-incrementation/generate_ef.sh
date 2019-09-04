#!/bin/bash

fs=$1
cond=$2
delay=$3
bc="$(basename -- $cond)"
app="python /home/users/vhayots/paper-memory-storage/experiments/bigbrain-incrementation/increment.py"
efout="experiment_files/$bc-ef.sh"

rm $efout

for f in $fs/*
do
    bn="$(basename -- $f)"
    echo $bn $cond
    echo "$app $f $cond --benchmark_file $cond/$bc-$bn.out --delay $delay" >> $efout
done 
