# Launching experiments

1. Make sure that `bids-example.simg` is in this directory. To get it, 
run `bosh exec prepare bids-example/boutiques/bids-app-example.json`.
2. Run `./create_invocations.sh <experiment_name>`
3. Clear the page cache: `echo 3 | sudo tee /proc/sys/vm/drop_caches`
4. Run `nohup parallel --jobs <n_jobs> < <experiment_name>/bosh_commands.sh &`
5. Move the results to isilon before running the next repetition

Note: these steps are implemented in `launch-*.sh`

# Results

Results are in the following directories:
* in-mem-[1-6]: storage was `/run/user`. in-mem[1-3] are with 96 cores, in-mem[4-6] are with 25.
* disk-{1,4}: storage was `/home/val`. disk-1 is with 96 cores, disk-4 is with 25. 
* nvme-{1,4}: storage was `/nvme-disk1`. nvme-1 is with 96 cores, nvme-4 is with 25.

Directory contents:

* Useful things:
  - `makespan_seconds`: the makespan of the experiment, in seconds.
  - `tasks.json`: a JSON object containing start/end date, exit code and real/user/system time of each task

* Additional things:
  - `logs`: stdout/stderr of each task
  - `invocations`: Boutiques invocation of each task
  - `bosh_commands.sh`: command line of each task.