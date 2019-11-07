#!/bin/bash

for f in ../results/20bb_redo/*
do
    fn=$(echo $(cut -d '/' -f4 <<< $f) | cut -d '.' -f 1)
    awk -v fn="$fn" '$0~fn,/sys/' ../../../mcgill_backup/paper-memory-storage/experiments/bigbrain-incrementation/nohup.out > nohup/$fn-nohup.out
done
