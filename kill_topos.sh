#!/bin/bash

# Kills all the running topologies on storm.
echo -ne "Killing all the running topologies\n"
count=0
$STORM list > /tmp/storm_running_topos.txt
IFS=' '
cat /tmp/storm_running_topos.txt | \
while read -r -a CMD; do 
	if  [[ $CMD == etl* ]] || [[ $CMD == pred* ]] || [[ $CMD == train* ]] || [[ $CMD == stat* ]];
	then	
   		echo "$Killing ${CMD[0]}"
		$STORM kill ${CMD[0]} -w 0
		$count=$count+1
	fi
done

echo -ne "Killed $count topologies\n"
