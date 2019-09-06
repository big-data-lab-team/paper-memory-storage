#!/usr/bin/env bash

#date
#echo IN-MEM-REP1
#echo 3 | sudo tee /proc/sys/vm/drop_caches
#parallel --jobs 96 < in-mem-1/bosh_commands.sh

#sleep 1000  # cool down a little

mv in-mem-1 ~/tristan/paper-memory-storage/experiments/bids-app-example/

date
echo IN-MEM-REP4
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < in-mem-4/bosh_commands.sh                                


sleep 100  # cool down a little

mv in-mem-4 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo IN-MEM-REP3
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < in-mem-3/bosh_commands.sh  

######################################
                                                                                 
sleep 100  # cool down a little                                                 

mv in-mem-3 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo IN-MEM-REP5
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < in-mem-5/bosh_commands.sh                                

sleep 100  # cool down a little

mv in-mem-5 ~/tristan/paper-memory-storage/experiments/bids-app-example/
    
date
echo IN-MEM-REP2
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 96 < in-mem-2/bosh_commands.sh  
                                                                                 
sleep 100  # cool down a little                                                 

mv in-mem-2 ~/tristan/paper-memory-storage/experiments/bids-app-example/
               
date
echo IN-MEM-REP6
echo 3 | sudo tee /proc/sys/vm/drop_caches                                       
parallel --jobs 25 < in-mem-6/bosh_commands.sh

mv in-mem-6 ~/tristan/paper-memory-storage/experiments/bids-app-example/
