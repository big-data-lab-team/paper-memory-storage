#!/usr/bin/env bash

date
echo DISK-REP2
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < disk-2/bosh_commands.sh                                


sleep 100  # cool down a little

mv disk-2 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo DISK-REP5
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < disk-5/bosh_commands.sh  

mv disk-5 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
sleep 100  # cool down a little

date
echo DISK-REP3
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < disk-3/bosh_commands.sh                                


sleep 100  # cool down a little

mv disk-3 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo DISK-REP6
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < disk-6/bosh_commands.sh  

mv disk-6 ~/tristan/paper-memory-storage/experiments/bids-app-example/
