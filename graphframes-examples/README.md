## Pre-reqs
You will need Apache spark and HDFS setup.
You can use [this repository](https://github.com/MBtech/ansible-spark) to setup cluster of Spark and HDFS

## How to run
Currently tab separated input graph files are accepted.
Make sure that your graph file is in HDFS

You might want to change the delimiter value in `config.py` to the delimiter for the input graph.

In your spark home directory executed the following command to run the GraphFrames spark job:
```
bin/spark-submit --master spark://<master_ip>:7077 --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 g-for-graph/graphframes-examples/pagerank.py <path of input graph>
```
I would also recommend passing appropriate values for the following spark configurations using command line `--executor-memory` and `--total-executor-cores`.
