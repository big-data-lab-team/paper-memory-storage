#!/bin/bash

curr_dir=`pwd`
cd ./experiments/bids-app-example
./plot.py
cd $curr_dir/experiments/bigbrain-incrementation/figures
./makespan.py '../results/*25cpus*' 25cpus-120delay-1it
./stacked_bar.py '../results/*25cpus*' 25cpus-120delay-1it
./makespan.py '../results/*96cpus*' 96cpus-0delay-1it
./stacked_bar.py '../results/*96cpus*' 96cpus-0delay-1it
./makespan.py '../results/20bb_experiments/*25cpus*' 20bb_25cpus spark
./stacked_bar.py '../results/20bb_experiments/*25cpus*' 20bb_25cpus spark
./makespan.py '../results/20bb_experiments/*96cpus*' 20bb_96cpus spark
./stacked_bar.py '../results/20bb_experiments/*96cpus*' 20bb_96cpus spark
cd $curr_dir
echo 'GNU Parallel: 75BB local App Direct 25 cpus - 1567704324-localAD_1it_25cpus_120delay_withpc-2.out:'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/1567704324-localAD_1it_25cpus_120delay_withpc-2.out --cpus 25
echo 'GNU Parallel: 75BB Optane App Direct 25 cpus - 1567705458-optaneAD_1it_25cpus_120delay_withpc-2.out:'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/1567705458-optaneAD_1it_25cpus_120delay_withpc-2.out --cpus 25
echo 'GNU Parallel: 75BB Isilon App Direct 25 cpus - 1567707833-isilonAD_1it_25cpus_120delay_withpc-2.out:'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/1567707833-isilonAD_1it_25cpus_120delay_withpc-2.out --cpus 25
echo 'GNU Parallel: 75BB tmpfs App Direct 25 cpus - 1567709282-tmpfsAD_1it_25cpus_120delay_withpc-2.out:'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/1567709282-tmpfsAD_1it_25cpus_120delay_withpc-2.out --cpus 25
echo 'GNU Parallel: 75BB local App Direct 96 cpus - 1567714252-localAD_1it_96cpus_0delay_npc-3.out:'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/1567714252-localAD_1it_96cpus_0delay_npc-3.out --cpus 96
echo 'GNU Parallel: 75BB local Memory mode 96 cpus - 1568150082-local_1it_96cpus_0delay_npc-3.out:'
./scripts/gantt.py --incrementation_tf ./experiments/bigbrain-incrementation/results/1568150082-local_1it_96cpus_0delay_npc-3.out --cpus 96
mv ./gantt-*.pdf ./experiments/bigbrain-incrementation/figures

