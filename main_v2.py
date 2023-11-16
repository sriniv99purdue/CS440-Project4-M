##############################################
##
##  CS440 Project 4
##  
##  Important: fill you name and PUID
##  
##  Name:
##  PUID:
#############################################

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
import sys

# create spark configuration
conf = SparkConf()
conf.setAppName("StreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 3 seconds
ssc = StreamingContext(sc, 3)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_topk")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)

########### TODO Start #####################################



#Add you implementation here



########### TODO End ######################################

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
