#!/usr/bin/env bash

date
echo ISILON-REP1
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < isilon-1/bosh_commands.sh                                


sleep 100  # cool down a little

    
date
echo ISILON-REP4
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < isilon-4/bosh_commands.sh  

    
