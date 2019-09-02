#!/usr/bin/env bash

set -e

if [ $# != 2 ]
then
  echo "usage: $0 <exp_name> <n_jobs>"
  exit 1
fi

exp_name=$1
n_jobs=$2
START=$(grep 'START DATE' ${exp_name}/logs/*.log | sort -g | head -1 | awk -F ':' '{print $3}' )
END=$(grep 'END DATE' ${exp_name}/logs/*.log | sort -g -r | head -1 | awk -F ':' '{print $3}' )
MAKESPAN=$(( ${END} - ${START} ))
echo "Makespan: ${MAKESPAN} seconds"

task_file="${exp_name}/tasks.json"
echo '{ "tasks": [' > ${task_file}
first=true
i=0
for log in $(ls -tra ${exp_name}/logs/*.log)
do
  if [ "${first}" = false ]
  then
    echo "," >> ${task_file}
  fi
  first=false
  task_number=$(( $i % ${n_jobs} ))
  i=$(( $i + 1 ))
  job_name=$(basename ${log} | awk -F '.' '{print $1}')
  start=$(grep 'START DATE' ${log} | awk -F ':' '{print $2}')
  start=$(date -d @${start})  # convert from unix timestamp to date
  end=$(grep 'END DATE' ${log} | awk -F ':' '{print $2}')
  end=$(date -d @${end})  # convert from unix timestamp to date
  echo "  { \"Task\": \"${task_number}\", \"Start\": \"${start}\", \"Finish\": \"${end}\" } " >> ${task_file}
done
echo '] }' >> ${task_file}