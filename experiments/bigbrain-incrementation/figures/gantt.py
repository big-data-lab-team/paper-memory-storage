#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt


def gantt(df):
    color = {"read_file":"turquoise", "increment_file":"crimson",
             "write_file":"purple"}

    fig,ax=plt.subplots(figsize=(6,3))
    labels=[]

    df = df[df["Task"] != "task_duration"]
    for i, task in enumerate(df.groupby("ThreadId")):
        labels.append(task[0])
        for r in task[1].groupby("Task"):
            data = r[1][["Start", "Duration"]]
            print(data, r[0])
            ax.broken_barh(data.values, (i-0.4,0.8), color=color[r[0]] )

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels) 
    ax.set_xlabel("time [s]")
    plt.tight_layout() 
    plt.savefig('gantt.pdf')


data_file = sys.argv[1]

df = pd.read_csv(data_file, delim_whitespace=True,
                 names=["Task", "Start", "End", "File", "ThreadId"])
df["Duration"] = df.End - df.Start

gantt(df)


