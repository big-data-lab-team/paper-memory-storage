#!/usr/bin/env python3

import json
from random import shuffle
from os import path as op, remove
import subprocess
import glob
import sys
from shutil import copytree, rmtree
from time import time, sleep
import fileinput
import glob


print(sys.argv)
base_dir = '/home/users/vhayots' 
exp_dir = op.join(base_dir, 'paper-memory-storage/experiments/bigbrain-incrementation')
block_dir = sys.argv[1] #'1000_blocks'
input_dataset = op.join(base_dir, block_dir)
isilon = base_dir
optane = '/nvme-disk1'
optane_ad = '/pmem1-data/val'
local = '/home/val'
tmpfs = '/dev/shm/val' #'/run/user/61218'
tmpfs_ad = '/dev/shm/val'
ef_dir = op.join(exp_dir, 'experiment_files')
results_dir = op.join(exp_dir, 'results')
cmd_template = 'time $(parallel --jobs {} < {})'
cmd_template_spark = 'time $(spark-submit --conf spark.network.timeout=10000000 --master local[{}] --driver-memory 700G spark_inc.py {} {} 1 --benchmark)'
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

        if len(sys.argv) < 4:
            out_name = '{0}-{1}-{2}'.format(start, c['id'], i)
        else:
            out_name = '{0}-{1}-{2}-{3}'.format('spark', start, c['id'], i)


        if ('optane' in c['storage'] or c['storage'] == 'local' or 
            'tmpfs' in c['storage']):
            disk = optane

            if c['storage'] == 'local':
                disk = local
            elif c['storage'] == 'tmpfs':
                disk = tmpfs
            elif c['storage'] == 'tmpfsAD':
                disk = tmpfs_ad
            elif c['storage'] == 'optaneAD':
                disk = optane_ad
            in_dir = op.join(disk, block_dir)
            print(disk, block_dir, in_dir)
            mem_in_dir = in_dir

            out_dir = op.join(disk, out_name)

            # Copy dataset to memory
            copytree(input_dataset, in_dir)
            print(in_dir, 'created')
        else:
            out_dir = op.join(isilon, out_name)

        if len(sys.argv) < 4:

            ef_cmd = [op.join(exp_dir, 'generate_ef.sh'), in_dir, out_dir, str(c['delay'])]
            print(ef_cmd)
            p = subprocess.Popen(ef_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(p.communicate())

            ef_exec = op.join(ef_dir, '{}-ef.sh'.format(op.basename(out_dir)))

            drop_caches()
            cmd = cmd_template.format(c['ncpus'], ef_exec)

        else:
            drop_caches()
            cmd = cmd_template_spark.format(c['ncpus'], in_dir, out_dir)

        print(cmd)

        p = subprocess.Popen(cmd, shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = p.communicate()
        print('COMMAND OUTPUT:\n', out.decode('utf-8'), '\n\nCOMMAND ERR:\n', err.decode('utf-8'))

        try:
            delete_output_files(mem_in_dir, out_dir)
            get_results(out_dir, out_name)
        except Exception as e:
            pass

        sleep(10)
