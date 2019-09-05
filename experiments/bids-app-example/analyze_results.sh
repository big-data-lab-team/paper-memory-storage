#!/usr/bin/env bash

set -e

if [ $# != 1 ]
then
  echo "usage: $0 <exp_name>"
  exit 1
fi

exp_name=$1
START=$(grep 'START DATE' ${exp_name}/logs/*.log | sort -g | head -1 | awk -F ':' '{print $3}' )
END=$(grep 'END DATE' ${exp_name}/logs/*.log | sort -g -r | head -1 | awk -F ':' '{print $3}' )
MAKESPAN=$(( ${END} - ${START} ))
echo "${MAKESPAN}" > ${exp_name}/makespan_seconds

task_file="${exp_name}/tasks.json"
echo '{ "tasks": [' > ${task_file}
first=true
i=0
for log in $(ls -tra ${exp_name}/logs/*.log)
do
  echo -n .
  if [ "${first}" = false ]
  then
    echo "," >> ${task_file}
  fi
  first=false
  i=$(( $i + 1 ))
  job_name=$(basename ${log} | awk -F '.' '{print $1}')
  start=$(grep 'START DATE' ${log} | awk -F ':' '{print $2}')
  start=$(date -d @${start})  # convert from unix timestamp to date
  end=$(grep 'END DATE' ${log} | awk -F ':' '{print $2}')
  end=$(date -d @${end})  # convert from unix timestamp to date
  code=$(grep -A 1 -i "exit code" ${log} | tail -1 | sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[mGK]//g")
  real=$(awk '$1=="real" {print $2}' ${log})
  user=$(awk '$1=="user" {print $2}' ${log})
  sys=$(awk '$1=="sys" {print $2}' ${log})
  echo "  { \"Start\": \"${start}\", \
            \"Finish\": \"${end}\", \"Name\": \"${job_name}\", \
            \"Exit code\": \"${code}\", \"user time\": \"${user}\",\
            \"real time\": \"${real}\", \"system time\": \"${sys}\" } " >> ${task_file}
done
echo '] }' >> ${task_file}
