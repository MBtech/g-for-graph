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
df = sqlContext.read.format("csv").option("delimiter", " ").load(inputFile)
# Rename columns to something decent.
df = df.withColumnRenamed("_c0", "src")\
.withColumnRenamed("_c1", "dst")\
.withColumnRenamed("_c2", "weight")
df.show(5)

aggcodes = df.select("src","dst").rdd.flatMap(lambda x: x).distinct()
vertices = aggcodes.map(lambda x: (x, x)).toDF(["id","name"])

edges = df.select("src", "dst")
g = GraphFrame(vertices, edges)

# Display the vertex and edge DataFrames
g.vertices.show(5)
# +--+-------+---+
# |id|   name|age|
# +--+-------+---+
# | a|  Alice| 34|
# | b|    Bob| 36|
# | c|Charlie| 30|
# | d|  David| 29|
# | e| Esther| 32|
# | f|  Fanny| 36|
# | g|  Gabby| 60|
# +--+-------+---+

g.edges.show(5)
# +---+---+------------+
# |src|dst|relationship|
# +---+---+------------+
# |  a|  b|      friend|
# |  b|  c|      follow|
# |  c|  b|      follow|
# |  f|  c|      follow|
# |  e|  f|      follow|
# |  e|  d|      friend|
# |  d|  a|      friend|
# |  a|  e|      friend|
# +---+---+------------+

# Get a DataFrame with columns "id" and "inDegree" (in-degree)
vertexInDegrees = g.inDegrees
vertexInDegrees.show(5)


# Run LPA
communities = g.labelPropagation(maxIter=5)
communities.persist().show(10)
