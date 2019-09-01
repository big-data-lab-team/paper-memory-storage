#!/usr/bin/env bash
echo "#!/usr/bin/env bash" > bosh-commands.sh  # cleans up previous commands
echo "mkdir -p logs" >> bosh-commands.sh
for i in  data/corr/RawDataBIDS/*/sub*
do
  dataset=$(echo $i | awk -F '/' '{print $4}')
  subject=$(echo $i | awk -F '/' '{print $5}' | awk -F '-' '{print $2}')
  exec_name="${dataset}_${subject}"
  invocation_file="invocations/${exec_name}.json"
  sed s,DATASET,data/corr/RawDataBIDS/${dataset},g invocations/template.json | sed s,PARTICIPANT,${subject},g | sed s,OUTPUT_DIR,outputs/${dataset}_${subject},g > ${invocation_file}
  echo "(echo +++++++++++++++++++++++ ; echo -n 'START DATE: date +%s' ; time bosh exec launch bids-example/boutiques/bids-app-example.json ${invocation_file}) &>logs/${exec_name}.log" >> bosh-commands.sh
  echo $i $dataset $subject 
done
