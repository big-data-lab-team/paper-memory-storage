#!/bin/bash

curr_dir=`pwd`
cd ./experiments/bids-app-example
./plot.py
cd $curr_dir/experiments/bigbrain-incrementation/figures
./makespan.py '../results/40bb_redo/*25cpus*' 40bb_25cpus
./stacked_bar.py '../results/40bb_redo/*25cpus*' 40bb_25cpus
./makespan.py '../results/40bb_redo/*96cpus*' 40bb_96cpus
./stacked_bar.py '../results/40bb_redo/*96cpus*' 40bb_96cpus
./makespan.py '../results/20bb_redo/*25cpus*' 20bb_25cpus
./makespan.py '../results/20bb_redo/*25cpus*' spark_20bb_25cpus spark
./stacked_bar.py '../results/20bb_redo/*25cpus*' 20bb_25cpus
./stacked_bar.py '../results/20bb_redo/*25cpus*' spark_20bb_25cpus spark
./makespan.py '../results/20bb_redo/*96cpus*' 20bb_96cpus
./makespan.py '../results/20bb_redo/*96cpus*' spark_20bb_96cpus spark
./stacked_bar.py '../results/20bb_redo/*96cpus*' 20bb_96cpus
./stacked_bar.py '../results/20bb_redo/*96cpus*' spark_20bb_96cpus spark
cd $curr_dir
echo 'GNU Parallel: 75BB local App Direct 25 cpus - 1572382794-localAD_1it_25cpus_40bb-1.out'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/40bb_redo/1572382794-localAD_1it_25cpus_40bb-1.out --cpus 25
echo 'GNU Parallel: 75BB Optane App Direct 25 cpus - 1572382482-optaneAD_1it_25cpus_40bb-1.out'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/40bb_redo/1572382482-optaneAD_1it_25cpus_40bb-1.out --cpus 25
echo 'GNU Parallel: 75BB Isilon App Direct 25 cpus - 1572388118-isilonAD_1it_25cpus_40bb-2.out'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/40bb_redo/1572388118-isilonAD_1it_25cpus_40bb-2.out --cpus 25
echo 'GNU Parallel: 75BB tmpfs App Direct 25 cpus - 1572380543-tmpfsAD_1it_25cpus_40bb-1.out'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/40bb_redo/1572380543-tmpfsAD_1it_25cpus_40bb-1.out --cpus 25
echo 'GNU Parallel: 75BB Optane Memory Mode 96 cpus - 1572306855-tmpfs_1it_96cpus_40bb-2.out'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/40bb_redo/1572306855-tmpfs_1it_96cpus_40bb-2.out --cpus 96
echo 'GNU Parallel: 75BB Optane App Direct 96 cpus - 1572379747-optaneAD_1it_96cpus_40bb-1.out'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/40bb_redo/1572379747-optaneAD_1it_96cpus_40bb-1.out --cpus 96
mv ./gantt-*.pdf ./experiments/bigbrain-incrementation/figures

