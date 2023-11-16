# CS440-Project4

1. Check whether you have installed pyspark, if not, use command **pip3 install pyspark** to install PySpark library.

2. PySpark will depends on Java environment. You need to use command **export JAVA_HOME=$YOUR_PATH** (replace $YOUR_PATH with your own path) to setup java environment. When you start a new terminal window, make sure you have indicated the $JAVA_HOME in that terminal before you run your program. If you are using Purdue server pod0-0, you can use command **export JAVA_HOME="/usr/lib/jvm/java-1.17.0-openjdk-amd64"**.

3. Use command **./auto_script.sh** to test your program result. This script will automically stop and tell you your result's correctness.

4. PySpark uses checkpoint protocol to remember your global states. When you run your program in the next time, PySpark will automically load your global states that generated previously, which will cause mistakes. So, when you manually testing your program, use command **rm -rf checkpoint_topk/** to delete previous global state. If you use auto_script.sh, the script will do deletion automatically.
