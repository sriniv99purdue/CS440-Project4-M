#!/bin/bash

declare -i i=0

sleep 1 # start streaming data from 2 second

while read line
do
		echo $line
		i+=1
		if [ $i -eq 1000 ];
		then 
				i=0
				sleep 3
		fi
done < "./input/input_file"
