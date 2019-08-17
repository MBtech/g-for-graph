import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

import scala.io.Source

object PartitionAndPageRank {
  def main(args: Array[String]) {
    // Start Spark.
    println("\n### Starting Spark\n")
    val sparkConf = new SparkConf().setAppName("example-graphx")
    implicit val sc = new SparkContext(sparkConf)

    // Suppress unnecessary logging.
    Logger.getRootLogger.setLevel(Level.ERROR)

    // Load a graph.
    val path = args(0)
    // Number of partitions
    val numPartitions = args(1).toInt

    println(s"\n### Loading edge list: ${path}\n")
    // Source.fromFile(path).getLines().foreach(println)

    val g: Graph[Int, Int] = GraphLoader.edgeListFile(
      sc,
      path,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK
    )

    val gp = g.partitionBy(new DBH, numPartitions)

//    val gp = g.partitionBy(PartitionStrategy.fromString("RandomVertexCut"), numPartitions)

//    gp.edges.saveAsObjectFile("/Volumes/Tyr/results/intermediate")
    gp.vertices.count()
    val ranks_init = gp.pageRank(0.0001).vertices
    //
    //    val edges: RDD[Edge[Int]] = sc.objectFile("/Volumes/Tyr/results/intermediate")
    //
    //    val graph: Graph[Int, Int] = Graph.fromEdges(EdgeRDD.fromEdges(edges), defaultValue = 1)
    //    graph.vertices.count()
    //
    //    val ranks = graph.pageRank(0.0001).vertices
    ////    graph.degrees.sortByKey()
    //    graph.edges.saveAsTextFile("/Volumes/Tyr/results/output")
    sc.stop()
  }
}