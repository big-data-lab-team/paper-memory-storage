#!/usr/bin/env bash

date
echo ISILON-REP3
echo 3 | sudo tee /proc/sys/vm/drop_caches
parallel --jobs 96 < isilon-rep3/bosh_commands.sh

sleep 1000  # cool down a little

date
echo ISILON-REP4
echo 3 | sudo tee /proc/sys/vm/drop_caches
parallel --jobs 96 < isilon-rep4/bosh_commands.sh

sleep 1000  # cool down a little
    
date
echo ISILON-REP5
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < isilon-rep5/bosh_commands.sh  

######################################

sleep 1000  # cool down a little

date
echo ISILON-REP6
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < isilon-rep6/bosh_commands.sh                                
                                                                                 
sleep 1000  # cool down a little                                                 
    
date
echo ISILON-REP7
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < isilon-rep7/bosh_commands.sh                                
                                                                                 
sleep 1000  # cool down a little                                                 
               
date
echo ISILON-REP8
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < isilon-rep8/bosh_commands.sh

