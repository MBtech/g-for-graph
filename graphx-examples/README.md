example-graphx
====

This is a repository for [Spark GraphX](http://spark.apache.org/graphx/) code examples implemented in Scala.

### Getting Started

- [ ] Install [sbt](http://www.scala-sbt.org/)
- [ ] Clone this repository
- [ ] Run sbt

```
git clone git@github.com:mogproject/example-graphx.git
cd example-graphx
sbt run
```
### Algorithms
- Page Rank
- Betweeness Centrality

### Running an example locally
- `sbt "runMain <package name of the algorithm> <input graph path> <number of partitions>"`

### Running on spark
```
bin/spark-submit --master spark://<masterIP>:7077 --executor-memory <memory per executor> --class PageRank target/scala-2.11/graphx-examples_2.11-1.0.jar <input file path> <number of partitions>
```
