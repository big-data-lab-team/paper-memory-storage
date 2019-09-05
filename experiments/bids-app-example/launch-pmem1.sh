#!/usr/bin/env bash

date
echo PMEM-REP1
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < pmem-1/bosh_commands.sh                                


sleep 100  # cool down a little

mv pmem-1 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo PMEM-REP4
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < pmem-4/bosh_commands.sh  

mv pmem-4 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
