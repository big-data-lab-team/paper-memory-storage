#!/usr/bin/env python3

import json
from random import shuffle
from os import path as op, remove
import subprocess
import glob
import sys
from shutil import copytree, rmtree
from time import time
import fileinput
import glob


base_dir = '/home/users/vhayots' 
exp_dir = op.join(base_dir, 'paper-memory-storage/experiments/bigbrain-incrementation')
block_dir = sys.argv[1] #'1000_blocks'
input_dataset = op.join(base_dir, block_dir)
isilon = base_dir
optane = '/nvme-disk1'
local = '/home/val'
tmpfs = '/run/user/61218'
ef_dir = op.join(exp_dir, 'experiment_files')
results_dir = op.join(exp_dir, 'results')
cmd_template = 'time $(parallel --jobs {} < {})'
im_size_b = 646461552 

with open(sys.argv[3], 'r') as f:
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


def get_results(out_dir, out_name):
    with open(op.join(results_dir, '{}.out'.format(out_name)), 'w+') as f:
        for line in fileinput.input(glob.glob(op.join(out_dir, '*'))):
            f.write(line)
        fileinput.close()
    


for i in range(int(sys.argv[2])):
    for c in conditions:

        parallel_writes = c['ncpus'] * im_size_b
        mem_in_dir = None
        in_dir = input_dataset
        out_dir = None
        mem_size = str(int((parallel_writes * 0.05 + parallel_writes) / 1024**3)) + 'G'
        print(mem_size)
        start = int(time())

        out_name = '{0}-{1}-{2}'.format(start, c['id'], i)

        if c['storage'] == 'optane' or c['storage'] == 'local' or c['storage'] == 'tmpfs':
            disk = optane

            if c['storage'] == 'local':
                disk = local
            elif c['storage'] == 'tmpfs':
                disk = tmpfs
            in_dir = op.join(disk, block_dir)
            mem_in_dir = in_dir
            out_dir = op.join(disk, out_name)

            # Copy dataset to memory
            in_dir = copytree(input_dataset, in_dir)
            print(in_dir, 'created')
        else:
            out_dir = op.join(isilon, out_name)

        ef_cmd = [op.join(exp_dir, 'generate_ef.sh'), in_dir, out_dir, str(c['delay'])]
        p = subprocess.Popen(ef_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(p.communicate())

        ef_exec = op.join(ef_dir, '{}-ef.sh'.format(op.basename(out_dir)))

        drop_caches()
        cmd = cmd_template.format(c['ncpus'], ef_exec)

        print(cmd)

        p = subprocess.Popen(cmd, shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = p.communicate()
        print('COMMAND OUTPUT:\n', out.decode('utf-8'), '\n\nCOMMAND ERR:\n', err.decode('utf-8'))

        delete_output_files(mem_in_dir, out_dir)
        get_results(out_dir, out_name)



        




        


        


