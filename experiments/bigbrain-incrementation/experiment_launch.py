#!/usr/bin/env python3

import json
from random import shuffle
from os import path as op, remove
import subprocess
import glob
import sys
from shutil import copytree

base_dir = '/home/users/vhayots' 
block_dir = sys.argv[1] #'1000_blocks'
input_dataset = op.join(base_dir, block_dir)
isilon = base_dir
optane = '/run/user/61218'
cmd_template = 'spark-submit --master local[{0}] --driver-memory {1} --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 spark_inc.py {2} {3} {4} --benchmark'
im_size_b = 646461552 

with open('conditions.json', 'r') as f:
    conditions = json.load(f)

shuffle(conditions)

def drop_caches():
    p = subprocess.Popen('echo 3 | sudo tee /proc/sys/vm/drop_caches',
                         shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    print(p.communicate())


def empty_dir(regex):
    files = glob.glob(regex)
    for f in files:
        remove(f)

def delete_output_files(mem_dir, out_dir):
    if mem_dir is not None:
        empty_dir(op.join(mem_dir, '*.nii'))

    empty_dir(op.join(out_dir, '*.nii'))


for i in range(int(sys.argv[2])):
    for c in conditions:

        parallel_writes = c['ncpus'] * im_size_b
        mem_in_dir = None
        in_dir = input_dataset
        out_dir = None
        mem_size = str(int((parallel_writes * 0.05 + parallel_writes) / 1024**3)) + 'G'
        print(mem_size)

        if c['storage'] == 'optane':
            in_dir = op.join(optane, block_dir) 
            mem_in_dir = in_dir
            out_dir = op.join(optane, '{0}-{1}'.format(c['id'], i))

            # Copy dataset to memory
            in_dir = copytree(input_dataset, in_dir)
            print(in_dir, 'created')
        else:
            out_dir = op.join(isilon, '{0}-{1}'.format(c['id'], i))

        drop_caches()
        cmd = cmd_template.format(c['ncpus'],
                                  mem_size,
                                  in_dir,
                                  out_dir,
                                  c['iterations'])

        if c['delay'] != 0 :
            delay_flag = '--delay {}'.format(c['delay'])
            cmd = '{0} {1}'.format(cmd, delay_flag)

        cmd = cmd.split(' ')
        print(cmd)

        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        print(p.communicate())

        delete_output_files(mem_in_dir, out_dir)



        




        


        


