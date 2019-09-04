#!/usr/bin/env bash

date
echo NVME-REP1
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < nvme-1/bosh_commands.sh                                


sleep 100  # cool down a little

mv nvme-1 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo NVME-REP4
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < nvme-4/bosh_commands.sh  

mv nvme-4 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
