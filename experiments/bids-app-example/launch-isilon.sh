#!/usr/bin/env bash

date
echo ISILON-REP2
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < isilon-2/bosh_commands.sh                                


sleep 100  # cool down a little

    
date
echo ISILON-REP5
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < isilon-5/bosh_commands.sh  

sleep 100  # cool down a little

date
echo ISILON-REP3
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < isilon-3/bosh_commands.sh                                


sleep 100  # cool down a little

date
echo ISILON-REP6
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < isilon-6/bosh_commands.sh  
