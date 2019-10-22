#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatch
from os import path as op
from json import load
from datetime import datetime
import time
import argparse
import numpy as np


def gantt_increment(df, data_file, ncpus, spark=False):
    color = {
        "read_file": "turquoise",
        "increment_file": "crimson",
        "write_file": "purple",
    }

    if spark:
        color = {
            "load_img": "turquoise",
            "increment_data": "crimson",
            "save_incremented": "purple",
        }

    fig, ax = plt.subplots(figsize=(6, 3))
    labels = []

    df = df[~df["Task"].str.contains("task_duration") & ~df["Task"].str.contains("driver_program")]


    program_start = df.Start.min()
    program_end = df.End.max() - program_start
    
    def count_overlaps(df):
        #df = df[df['Task'].str.contains("increment_file")]
        df['tmp'] = 1
        df1 = pd.merge(df,df.reset_index(),on=['tmp'])
        df = df.drop('tmp', axis=1)
        df1 = df1[(df1["Start_x"]<=df1["Start_y"])  & (df1["End_x"]> df1["Start_y"])]
        #df1 = df1[((df1["Start_x"]>=df1["Start_y"]) & (df1["Start_x"] <= df1["End_y"]))]
        df['overlaps'] = df1.groupby('index').size()
        return (df)

    '''
    for idx, rw in df.iterrows():
        overlaps = 0
        for index, row in df.iterrows():
            if not rw.equals(row):
                start_idx_row = row['Start']
                end_idx_row = row['Duration'] + start_idx_row
                start_idx_rw = rw['Start']
                end_idx_rw = rw['Duration'] + start_idx_rw

                if ((start_idx_rw >= start_idx_row and start_idx_rw <= end_idx_row) or
                    (end_idx_rw >= start_idx_row and end_idx_rw <= end_idx_row)):
                    overlaps += 1

        if overlaps > max_overlaps:
            max_overlaps = overlaps
    '''
    print('Avg number of overlaps:', count_overlaps(df)['overlaps'].mean())

    print('Number of threads:', len(df.groupby("ThreadId")))
    for i, task in enumerate(df.groupby("ThreadId")):
        labels.append(task[0])
        for r in task[1].groupby("Task"):
            data = r[1][["Start", "Duration"]]
            ax.broken_barh(data.values, (i - 0.4, 0.8), color=color[r[0]])

    r_read = mpatch.Patch(facecolor='turquoise', label='Read')
    r_inc = mpatch.Patch(facecolor='crimson', label='Increment')
    r_write = mpatch.Patch(facecolor='purple', label='Write')
    ax.legend(handles=[r_read, r_inc, r_write], loc='upper center',
              bbox_to_anchor=(0.5, 1.07), ncol=3, fancybox=True,
              fontsize='x-small', framealpha=1)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels([])
    ax.set_xlabel("time [s]")

    if not spark:
        if ncpus == 25:
            ax.set_xlim(0, 1200)
        else:
            pass #ax.set_xlim(0, 140)
    ax.tick_params(axis='both', which='major', labelsize=8)
    plt.tight_layout()
    plt.savefig("gantt-{}.pdf".format(op.basename(data_file).strip('.out')))


def gantt_bids(df, data_file):
    color = {
        "i/o": "blue",
        "cpu": "orange"
    }

    fig, ax = plt.subplots(figsize=(6, 3))
    labels = []

    for i, task in enumerate(df.groupby("ThreadId")):
        labels.append(task[0])
        for r in task[1].groupby("Name"):
            data = r[1][["Start", "io"]]
            ax.broken_barh(data.values, (i - 0.4, 0.8), color=color["i/o"])
            data = r[1][["Start_cpu", "cpu"]]
            ax.broken_barh(data.values, (i - 0.4, 0.8), color=color["cpu"])

    orangebar = mpatch.Rectangle((0, 0), 1, 1, fc="orange")
    bluebar = mpatch.Rectangle((0, 0), 1, 1, fc="b")
    ax.legend([orangebar, bluebar], ['CPU', 'I/O'])

    ax.set_xlabel("time [s]")
    plt.tight_layout()
    plt.savefig("gantt-{}.pdf".format(op.basename(data_file)))
    

def main():
    parser = argparse.ArgumentParser(description="Gantt chart generation")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--incrementation_tf", type=str, help="The incrementation csv"
    )
    input_group.add_argument("--bids_tf", type=str, help="The BidsApp generated JSON")
    parser.add_argument("--cpus", type=int, help="number of cpus used")
    args = parser.parse_args()

    df = None
    color = None
    data_file = None

    if args.incrementation_tf is not None:
        data_file = args.incrementation_tf
        spark = 'spark' in data_file
        df = pd.read_csv(
            data_file,
            delim_whitespace=not spark,
            names=["Task", "Start", "End", "File", "ThreadId"],
        )

        df["Duration"] = df["End"] - df["Start"]
        df["Start"] = df["Start"] - df["Start"].min()
        df["End"] = df["Start"] + df["Duration"]

        gantt_increment(df, data_file, args.cpus, spark)

    else:
        data_file = args.bids_tf

        with open(data_file, "r") as f:
            # date fmt: Tue Sep  3 17:21:44 EDT 2019
            get_tmsp = lambda a: datetime.timestamp(
                datetime.strptime(a, "%a %b  %w %H:%M:%S %Z %Y")
            )

            # time fmt 1m6.671s
            def get_seconds(t_string):
                components = t_string.rstrip("s").split("m")
                minutes = int(components[0])
                seconds = float(components[1])
                return minutes * 60 + seconds

            data = load(f)
            df = pd.DataFrame(data["tasks"])
            df = df.rename(columns={"Finish": "End", "real time": "real", "system time": "system", "user time": "user"})
            df.Start = df.Start.map(get_tmsp)
            df.real = df.real.map(get_seconds)
            df.system = df.system.map(get_seconds)
            df.user = df.user.map(get_seconds)

            df["cpu"] = df.user + df.system
            df["io"] = df.real - (df.user + df.system)

            df = df.sort_values(by=["Start"])

            df["ThreadId"] = df.loc[ : , "Start" ].map(lambda x: x % args.cpus)
            df["Start"] = df["Start"] - df["Start"].min()
            df["Start_cpu"] = df["Start"] + df["io"] 

            gantt_bids(df, data_file)


if __name__ == "__main__":
    main()
