import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import partitioning.CoordinatedPartitionState

object PageRank {
  def main(args: Array[String]) {
    // Start Spark.
    println("\n### Starting Spark\n")
    val sparkConf = new SparkConf().setAppName("PageRank")
    implicit val sc = new SparkContext(sparkConf)

    // Suppress unnecessary logging.
    Logger.getRootLogger.setLevel(Level.ERROR)

    // Load a graph.
    val path = args(0)
    // Number of partitions
    val numPartitions = args(1).toInt
    val strategy = if (args.size >= 3) args(2) else "hash"

    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Loading edge list: ${path}\n")
    // Source.fromFile(path).getLines().foreach(println)

    val g: Graph[Int, Int] = GraphLoader.edgeListFile(
      sc,
      path,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      numEdgePartitions = numPartitions
    )
    // g.edges.saveAsTextFile("file:/home/ubuntu/input")

    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Graph Loaded. Number of Vertices: ${g.vertices.count()}\n")

    // val  state = new CoordinatedPartitionState(numPartitions)
    var algorithm : PartitionStrategy = PartitionStrategy.fromString("RandomVertexCut")
    if (strategy.equals("dbh")){
      println("Partition by DBH")
      algorithm = DBH
    }else if (strategy.equals("hdrf")){
      println("Partition by HDRF")
      algorithm = HDRF
    }

    val gp = g.partitionBy(algorithm, numPartitions)
//    val gp = g.partitionBy(new ImblanacedPartitioner,3)
    // gp.edges.saveAsTextFile("file:/home/ubuntu/output")
    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Partitioning Done. Number of Vertices: ${gp.vertices.count()}\n")

    val countRDD = gp.edges.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Edge Distribution")
    countRDD.collect().foreach(println)


    val vRDD = gp.vertices.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Vertices Distribution")
    vRDD.collect().foreach(println)
    
    val ranks = gp.pageRank(0.0001).vertices

    ranks.take(10).foreach(println)
    // Stop Spark.
    sc.stop()
  }
}
