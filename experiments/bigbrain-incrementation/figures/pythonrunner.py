import sys
from glob import glob
from os import path as op, linesep
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json


sl_fldr = '../home/val/spark-logs/'

def get_json(log):
    data = []
    with open(log, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return data


df = None
slogs = []
smakespans = []
for fn in glob(op.join(sys.argv[1], '*' + sys.argv[2] + '*')):
    with open(fn, 'r') as f:
        data = f.read().split(linesep)
        pr = [line.strip().split(':')[-1].split(',') for line in data if 'PythonRunner' in line]
        pr = [{ atr.split('=')[0].strip(' '): atr.split('=')[1].strip(' ') for atr in el } for el in pr]

        sl = [line.split('spark-logs/')[1].split(' ')[0] for line in data if 'local-' in line][0]
        slogs.append((fn, op.join(sl_fldr, sl)))

        smp = [line.strip(' s').split(' ')[-1] for line in data if 'Job 0 finished' in line][0]
        smakespans.append({ 'filename' : fn, 'makespan': float(smp), 'storage': op.basename(fn).split('_')[0].split('-')[-1] })

        for el in pr:
            el['filename'] = op.basename(fn)
            
        if df is None:
            df = pd.DataFrame(pr)
        else:
            df = pd.concat([df, pd.DataFrame(pr)])

df_sl = None

for fn, sl in slogs:
    data = get_json(sl)
    data = [{**ev['Task Info'], **ev['Task Metrics']} for ev in data if ev['Event'] == 'SparkListenerTaskEnd']
    for el in data:
        el['filename'] = op.basename(fn)

    if df_sl is None:
        df_sl = pd.DataFrame(data)
    else:
        df_sl = pd.concat([df_sl, pd.DataFrame(data)])

df_mksp = pd.DataFrame(smakespans)
df_mksp_mem = df_mksp[~df_mksp['filename'].str.contains("AD")].groupby(['storage'])
df_mksp_mem_mean = df_mksp_mem.mean()
df_mksp_mem_std = df_mksp_mem.std()
df_mksp_ad = df_mksp[df_mksp['filename'].str.contains("AD")].groupby(['storage'])
df_mksp_ad_mean = df_mksp_ad.mean()
df_mksp_ad_std = df_mksp_ad.std()

print(df_mksp_mem_mean)
print(df_mksp_mem_std)
print(df_mksp_ad_mean)
print(df_mksp_ad_std)

df_sl['Executor Run Time'] = df_sl['Executor Run Time'] / 1000
df_sl['Executor CPU Time'] = df_sl['Executor CPU Time'] / 10**9
df_sl['JVM GC Time'] = df_sl['JVM GC Time'] / 1000

dfsl_mem = df_sl[~df_sl["filename"].str.contains("AD")][['filename', 'Executor Run Time', 'Executor CPU Time', 'JVM GC Time']].groupby(['filename']).sum().reset_index()
dfsl_mem['storage'] = dfsl_mem['filename'].apply(lambda x: x.split('_')[0].split('-')[-1])
dfsl_mem_mean = dfsl_mem.groupby(['storage']).mean()
dfsl_mem_std = dfsl_mem.groupby(['storage']).std()
dfsl_ad = df_sl[df_sl["filename"].str.contains("AD")][['filename', 'Executor Run Time', 'Executor CPU Time', 'JVM GC Time']].groupby(['filename']).sum().reset_index()
dfsl_ad['storage'] = dfsl_ad['filename'].apply(lambda x: x.split('_')[0].split('-')[-1])
dfsl_ad_mean = dfsl_ad.groupby(['storage']).mean()
dfsl_ad_std = dfsl_ad.groupby(['storage']).std()

df['total'] = df['total'].astype(int) / 1000
df['boot'] = df['boot'].astype(int) / 1000
df['init'] = df['init'].astype(int) / 1000
df['finish'] = df['finish'].astype(int) / 1000


df_mem = df[~df["filename"].str.contains("AD")].groupby(['filename']).sum().reset_index()
df_mem['storage'] = df_mem['filename'].apply(lambda x: x.split('_')[0].split('-')[-1])
df_mem_mean = df_mem.groupby(['storage']).mean()
df_mem_std = df_mem.groupby(['storage']).std()
df_ad = df[df["filename"].str.contains("AD")].groupby(['filename']).sum().reset_index()
df_ad['storage'] = df_ad['filename'].apply(lambda x: x.split('_')[0].split('-')[-1])
df_ad_mean = df_ad.groupby(['storage']).mean()
df_ad_std = df_ad.groupby(['storage']).std()


print('***Python Runner***\n\n*Memory Mode*\n\nMean:')
print(df_mem_mean)
print('\nSTD:')
print(df_mem_std)
print('\n\n*App Direct Mode*\n\nMean:')
print(df_ad_mean)
print('\nSTD:')
print(df_ad_std)
print('\n\n***Spark Logs***\n\n*Memory Mode*\n\nMean:')
print(dfsl_mem_mean)
print('\nSTD:')
print(dfsl_mem_std)
print('\n*App Direct Mode*\n\nMean:')
print(dfsl_ad_mean)
print('\nSTD:')
print(dfsl_ad_std)


figure, ax = plt.subplots()
width = 0.4
ind = np.asarray([2, 1])

p_total = ax.bar(
    ind - width/2,
    df_mem_mean['total'],
    width,
    color="g",
    label="total",
    yerr=df_mem_std['total']
)
p_total_ad = ax.bar(
    ind + width/2,
    df_ad_mean['total'],
    width,
    color="g",
    label="total AD",
    alpha=0.4,
    yerr=df_ad_std['total']
)
p_init = ax.bar(
    ind - width/2,
    df_mem_mean['init'],
    width,
    color="r",
    label="init",
    yerr=df_mem_std['init']
)
p_init_ad = ax.bar(
    ind + width/2,
    df_ad_mean['init'],
    width,
    color="r",
    label="init AD",
    alpha=0.4,
    yerr=df_ad_std['init']
)
p_finish = ax.bar(
    ind - width/2,
    df_mem_mean['finish'],
    width,
    bottom=df_mem_mean['init'],
    color="b",
    label="finish",
    yerr=df_mem_std['finish']
)
p_finish_ad = ax.bar(
    ind + width/2,
    df_ad_mean['finish'],
    width,
    bottom=df_ad_mean['init'],
    color="b",
    label="finish AD",
    alpha=0.4,
    yerr=df_ad_std['finish']
)

ax.set_ylabel("Average total execution time (s)")
ax.set_xlabel("Storage type")
ax.set_xticks(ind)
ax.set_xticklabels(['Isilon', 'Optane'])
ax.legend()
plt.savefig('pythonrunner-{}.pdf'.format(sys.argv[2]))

figure, ax = plt.subplots()
width = 0.4
ind = np.asarray([2, 1])

p_total = ax.bar(
    ind - width/2,
    dfsl_mem_mean['Executor Run Time'],
    width,
    color="g",
    label="total",
    yerr=dfsl_mem_std['Executor Run Time']
)
p_total_ad = ax.bar(
    ind + width/2,
    dfsl_ad_mean['Executor Run Time'],
    width,
    color="g",
    label="total AD",
    alpha=0.4,
    yerr=dfsl_ad_std['Executor Run Time']
)
p_init = ax.bar(
    ind - width/2,
    dfsl_mem_mean['Executor CPU Time'],
    width,
    color="r",
    label="CPU",
    yerr=dfsl_mem_std['Executor CPU Time']
)
p_init_ad = ax.bar(
    ind + width/2,
    dfsl_ad_mean['Executor CPU Time'],
    width,
    color="r",
    label="CPU AD",
    alpha=0.4,
    yerr=dfsl_ad_std['Executor CPU Time']
)
p_finish = ax.bar(
    ind - width/2,
    dfsl_mem_mean['JVM GC Time'],
    width,
    bottom=dfsl_mem_mean['Executor CPU Time'],
    color="b",
    label="GC",
    yerr=dfsl_mem_std['JVM GC Time']
)
p_finish_ad = ax.bar(
    ind + width/2,
    dfsl_ad_mean['JVM GC Time'],
    width,
    bottom=dfsl_ad_mean['Executor CPU Time'],
    color="b",
    label="GC AD",
    alpha=0.4,
    yerr=dfsl_ad_std['JVM GC Time']
)

ax.set_ylabel("Average total execution time (s)")
ax.set_xlabel("Storage type")
ax.set_xticks(ind)
ax.set_xticklabels(['Isilon', 'Optane'])
ax.legend()
plt.savefig('sparklogs-{}.pdf'.format(sys.argv[2]))

figure, ax = plt.subplots()
width = 0.4
ind = np.asarray([2, 1])

p_total = ax.bar(
    ind - width/2,
    df_mksp_mem_mean['makespan'],
    width,
    color="g",
    label="Memory mode",
    yerr=df_mksp_mem_std['makespan']
)
p_total_ad = ax.bar(
    ind + width/2,
    df_mksp_ad_mean['makespan'],
    width,
    color="g",
    label="total AD",
    alpha=0.4,
    yerr=df_mksp_ad_std['makespan']
)

ax.set_ylabel("Average run time (s)")
ax.set_xlabel("Storage type")
ax.set_xticks(ind)
ax.set_xticklabels(['Isilon', 'Optane'])
ax.legend()
plt.savefig('spark-makespan-{}.pdf'.format(sys.argv[2]))
