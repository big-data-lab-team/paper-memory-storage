#!/usr/bin/env python3

import json
from random import shuffle
from os import path as op, remove
import subprocess
import glob
import sys
from shutil import copytree, rmtree

base_dir = '/home/users/vhayots' 
exp_dir = op.join(base_dir, 'paper-memory-storage/experiments/bigbrain-incrementation')
block_dir = sys.argv[1] #'1000_blocks'
input_dataset = op.join(base_dir, block_dir)
isilon = base_dir
optane = '/nvme-disk1'
local = '/home/val'
ef_dir = op.join(exp_dir, 'experiment_files')
cmd_template = 'time $(parallel --jobs {} < {})'
im_size_b = 646461552 

with open('conditions.json', 'r') as f:
    conditions = json.load(f)

shuffle(conditions)

def drop_caches():
    p = subprocess.Popen('echo 3 | sudo tee /proc/sys/vm/drop_caches',
                         shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(p.communicate())


def empty_dir(regex):
    files = glob.glob(regex)
    for f in files:
        remove(f)

def delete_output_files(mem_dir, out_dir):
    if mem_dir is not None:
        rmtree(mem_dir)

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

        ef_cmd = [op.join(exp_dir, 'generate_ef.sh'), in_dir, out_dir, str(c['delay'])]
        p = subprocess.Popen(ef_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(p.communicate())

        ef_exec = op.join(ef_dir, '{}-ef.sh'.format(op.basename(out_dir)))

        drop_caches()
        cmd = cmd_template.format(c['ncpus'], ef_exec)

        print(cmd)

        p = subprocess.Popen(cmd, shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(p.communicate())

        delete_output_files(mem_in_dir, out_dir)



        




        


        


