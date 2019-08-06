# add GraphFrames package to spark-submit
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.7.0-spark2.4-s_2.11'
import sys
from functools import reduce
from pyspark.sql.functions import col, lit, when
import pyspark
from graphframes.examples import Graphs
from graphframes import GraphFrame

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)
inputFile = sys.argv[1]
# g = Graphs(sqlContext).friends()  # Get example graph
df = sqlContext.read.format("csv").option("delimiter", "\t").load(inputFile)
# Rename columns to something decent.
df = df.withColumnRenamed("_c0", "src")\
.withColumnRenamed("_c1", "dst")\
.withColumnRenamed("_c2", "weight")
df.show(5)

aggcodes = df.select("src","dst").rdd.flatMap(lambda x: x).distinct()
vertices = aggcodes.map(lambda x: (x, x)).toDF(["id","name"])

edges = df.select("src", "dst")
graph = GraphFrame(vertices, edges)

result = graph.stronglyConnectedComponents(maxIter=10)
result.select("id", "component").orderBy("component").show()
