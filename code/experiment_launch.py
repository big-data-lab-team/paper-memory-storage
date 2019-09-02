#!/usr/bin/env python3

import json
from random import shuffle
from os import path as op, remove
import subprocess
import glob
import sys

base_dir = '/home/users/vhayots' 
block_dir = sys.argv[1] #'1000_blocks'
input_dataset = op.join(base_dir, block_dir)
isilon = base_dir
optane = '/run/user/61218'
cmd_template = 'spark-submit --master local[{0}] --driver-memory {1} spark_inc.py {2} {3} {4} --benchmark'
im_size_b = 646461552 

with open('conditions.json', 'r') as f:
    conditions = json.load(f)

shuffle(conditions)

def drop_caches():
    p = subprocess.Popen('echo 3 | sudo tee /proc/sys/vm/drop_caches',
                         shell=True)
    print(p.communicate())


def empty_dir(regex):
    files = glob.glob(regex)
    for f in files:
        remove(f)

def delete_output_files(work_dir_flag, mem_dir, mem_dir_out, isilon_dir_out):
    if isilon_dir_out is not None:
        empty_dir(op.join(isilon_dir_out, '*.nii'))
        empty_dir(op.join(work_dir_flag.split(' ')[1], '*.nii'))

    else:
        empty_dir(op.join(mem_dir, '*.nii'))
        empty_dir(op.join(mem_dir_out, '*.nii'))


for i in range(int(sys.argv[2])):
    for c in conditions:

        parallel_writes = c['ncpus'] * im_size_b
        work_dir_flag = None
        mem_dir_out = None 
        mem_dir = None
        isilon_dir_out = None
        mem_size = str(int((parallel_writes * 0.05 + parallel_writes) / 1024**3)) + 'G'
        print(mem_size)

        if c['storage'] == 'optane':
            mem_dir = op.join(optane, block_dir) 
            mem_dir_out = op.join(optane, '{0}-{1}'.format(c['id'], i))
            p = subprocess.Popen(['cp', '-r', input_dataset, optane],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(p.communicate())

            drop_caches()


            cmd = cmd_template.format(c['ncpus'],
                                      mem_size,
                                      mem_dir,
                                      mem_dir_out,
                                      c['iterations'])

        else:
            isilon_dir_out = op.join(isilon, '{0}-{1}'.format(c['id'], i))
            drop_caches()
            cmd = cmd_template.format(c['ncpus'],
                                      mem_size,
                                      input_dataset,
                                      isilon_dir_out,
                                      c['iterations'])
            work_dir_flag = '--work_dir {}'.format(op.join(isilon, 'work_dir'))
            cmd = '{0} {1}'.format(cmd, work_dir_flag)


        if c['delay'] != 0 :
            delay_flag = '--delay {}'.format(c['delay'])
            cmd = '{0} {1}'.format(cmd, delay_flag)

        cmd = cmd.split(' ')
        print(cmd)

        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        print(p.communicate())

        delete_output_files(work_dir_flag, mem_dir, mem_dir_out, isilon_dir_out)



        




        


        


