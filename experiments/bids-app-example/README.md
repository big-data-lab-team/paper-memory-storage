1. Make sure that `bids-example.simg` is in this directory. To get it, 
run `bosh exec prepare bids-example/boutiques/bids-app-example.json`.
2. Run `./create_invocations.sh <experiment_name>`
3. Run `nohup parallel --jobs <n_jobs> < <experiment_name>/bosh_commands.sh &`