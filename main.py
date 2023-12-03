##############################################
##
##  CS440 Project 4
##  
##  Important: fill you name and PUID
##  
##  Name: Haris Hasan
##  PUID: 0032930768
#############################################

# to run: 
# chmod +x auto_script.sh
# export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-19.jdk/Contents/Home
# ./auto_script.sh 

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import sys
import heapq

def process_window(time, rdd):
    try:
        print("----------- %s -----------" % str(time))
        
        #extract the top 10 integers from the RDD for Task 1
        top10_current_interval = rdd.takeOrdered(10, key=lambda x: -x)

        #write the results for Task 1 to the file
        with open("./result/task1.txt", "a") as fd:
            fd.write(' '.join(str(num) for num in top10_current_interval) + '\n')
    except:
        e1 = sys.exc_info()[0]
        e2 = sys.exc_info()[1]
        print("Error: %s %s" % (e1, e2))

def update_global_topk(new_values, global_topk):
    if global_topk is None:
        global_topk = []
    for value in new_values:
        heapq.heappush(global_topk, value)
        if len(global_topk) > 10:
            heapq.heappop(global_topk)
    return global_topk

def process_global(time, rdd):
    try:
        #get the global top 10 integers
        global_top10 = rdd.collect()
        if global_top10:
            global_top10 = heapq.nlargest(10, global_top10[0])

            #write the results for Task 2 to the file
            with open("./result/task2.txt", "a") as fd:
                fd.write(' '.join(str(num) for num in global_top10) + '\n')
    except:
        e1 = sys.exc_info()[0]
        e2 = sys.exc_info()[1]
        print("Error: %s %s" % (e1, e2))

# Create Spark configuration
conf = SparkConf()
conf.setAppName("StreamApp")

# Create Spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Create the Streaming Context from the above spark context with interval size 3 seconds
ssc = StreamingContext(sc, 3)

# Setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_topk")

# Read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)

# Parse input from string to integer
dataStream = dataStream.map(lambda x: int(x))

# Process each RDD for Task 1
dataStream.foreachRDD(process_window)

# Update and process global state for Task 2
global_topk = dataStream.map(lambda x: (1, x)).updateStateByKey(update_global_topk)
global_topk.foreachRDD(process_global)

# Start the streaming computation
ssc.start()

# Wait for the streaming to finish
ssc.awaitTermination()
