#!/usr/bin/env bash

set -e

if [ $# != 1 ]
then
  echo "usage: $0 <experiment_name>"
  exit 1
fi

exp_name=$1
test ${exp_name} || (echo "Directory ${exp_name} already exists, please remove it first." ; exit 1)
mkdir -p ${exp_name}/{logs,invocations}
bosh_file=${exp_name}/bosh_commands.sh

echo "#!/usr/bin/env bash" > ${bosh_file}  # cleans up previous commands

echo "mkdir -p ${exp_name}/logs" >> ${bosh_file}

for i in  data/corr/RawDataBIDS/*/sub*
do
  dataset=$(echo $i | awk -F '/' '{print $4}')
  subject=$(echo $i | awk -F '/' '{print $5}' | awk -F '-' '{print $2}')
  exec_name="${dataset}_${subject}"
  invocation_file="${exp_name}/invocations/${exec_name}.json"
  sed s,DATASET,data/corr/RawDataBIDS/${dataset},g invocation_template.json \
     | sed s,PARTICIPANT,${subject},g | sed s,OUTPUT_DIR,${exp_name}/outputs/${dataset}_${subject},g\
      > ${invocation_file}
  echo "(echo -n START DATE: ; date +%s ;\
         time bosh exec launch bids-example/boutiques/bids-app-example.json\
         ${invocation_file} ; echo -n END DATE: ; date +%s ) &>${exp_name}/logs/${exec_name}.log" >> ${bosh_file}
  echo -n .
done
echo
chmod 755 ${bosh_file}
