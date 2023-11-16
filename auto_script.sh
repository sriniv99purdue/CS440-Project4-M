#!/bin/bash

rm -rf ./checkpoint_topk

rm ./result/task2.txt
touch ./result/task2.txt
rm ./result/task1.txt
touch ./result/task1.txt

rm ./result/windowDiff
rm ./result/globalDiff
touch ./result/windowDiff
touch ./result/globalDiff

python3 main.py &
sleep 6
serverPID=$!

if [ $# -eq 0 ];
then
	./input/read_file.sh | nc -l -p 9009 &
else
	./input/read_file.sh | nc -l -p $1 &
fi

ncPID=$!

declare -i timeCount=0
ln=`wc -l < './result/task1.txt'`
while [ $ln -lt 10 ]
do
	timeCount+=1
	if [ $timeCount -eq 40 ];
	then
		break
	fi

	ln=`wc -l < './result/task1.txt'`
	sleep 1
done


echo "task complete"
kill -s 9 $serverPID
kill -s 9 $ncPID

sleep 1
clear

diff <(head -n 10 ./result/task1.txt) <(head -n 10 ./result/expected1.txt) > ./result/windowDiff
diff <(head -n 10 ./result/task2.txt) <(head -n 10 ./result/expected2.txt) > ./result/globalDiff

windowDiffExist=`wc -l < './result/windowDiff'`
globalDiffExist=`wc -l < './result/globalDiff'`

echo "Result:"

if [ $windowDiffExist -eq 0 ] ;
then
		echo "Task 1: PASS!"
else
		echo "Task 1: Failed."
fi

if [ $globalDiffExist -eq 0 ] ;
then
		echo "Task 2: PASS!"
else
		echo "Task 2: Failed."
fi
