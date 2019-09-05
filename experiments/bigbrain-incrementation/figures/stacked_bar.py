#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
from glob import glob
from os import path as op
import numpy as np


def stacked_bar(df):

    gk = df.groupby(["filename"]).groups.keys()
    
    get_disk = lambda x: x.split('_')[0].split('-')[-1]

    ind = [get_disk(g) for g in gk]

    dfr = df[df["Task"] == "read_file"].groupby(["filename"])["Duration"].sum()
    dfi = df[df["Task"] == "increment_file"].groupby(["filename"])["Duration"].sum()
    dfw = df[df["Task"] == "write_file"].groupby(["filename"])["Duration"].sum()

    width = 0.4
    print(dfr)
    print(dfi)
    print(dfw)

    p_read = plt.bar(ind, dfr, width, color='r', label="read")
    p2 = plt.bar(ind, dfi, width, bottom=np.array(dfr), color='b', label="increment")
    p3 = plt.bar(ind, dfw, width, 
             bottom=np.array(dfr)+np.array(dfi), color='g', label="write")

    plt.ylim(0, 36000)
    plt.legend()
    plt.savefig('{}.pdf'.format(sys.argv[2]))
    

all_files = glob(op.abspath(sys.argv[1]))
disks = ['tmpfs', 'tmpfsAD', 'optane', 'optaneAD', 'local', 'isilon']
all_files.sort(key=lambda x: disks.index(op.basename(x).split('_')[0].split('-')[1]))

print(all_files)

df = pd.concat((pd.read_csv(f, delim_whitespace=True, names=["Task", "Start", "End", "File", "ThreadId"]).assign(filename='{0}-{1}'.format(i, op.basename(f))) for i, f in enumerate(all_files)))

df["Duration"] = df.End - df.Start
stacked_bar(df)


