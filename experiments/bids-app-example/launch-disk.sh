#!/usr/bin/env bash

date
echo DISK-REP1
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < disk-1/bosh_commands.sh                                


sleep 100  # cool down a little

mv disk-1 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo DISK-REP4
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < disk-4/bosh_commands.sh  

mv disk-4 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
