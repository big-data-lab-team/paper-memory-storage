1. Make sure that `bids-example.simg` is in this directory. To get it, 
run `bosh exec prepare bids-example/boutiques/bids-app-example.json`.
2. Run `./create_invocations.sh <experiment_name>`
3. Clear the page cache: `echo 3 | sudo tee /proc/sys/vm/drop_caches`
4. Run `nohup parallel --jobs <n_jobs> < <experiment_name>/bosh_commands.sh &`
5. Move the results to isilon before running the next repetition

Note: these steps are implemented in `launch-*.sh`
