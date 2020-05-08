#!/bin/bash

# Kills all the running jobs.

echo -ne "Killing all the running jobs\n"
count=0
flink list > /tmp/flink_running_jobs.txt
IFS=' '
cat /tmp/flink_running_jobs.txt | \
while read -r -a CMD; do
    if  [ "${#CMD[@]}" -gt 4 ];
    then
        if [[ ${CMD[5]} == ETL* ]] || [[ ${CMD[5]} == Pred* ]] || [[ ${CMD[5]} == Stats* ]] || [[ ${CMD[5]} == Train* ]];
        then
            echo "Killing ${CMD[5]} ..."
            flink cancel ${CMD[3]}
            let count++
            echo -ne "Killed $count jobs\n"
        fi
    fi
done