import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.io.Source

object TC {
  def main(args: Array[String]) {
    // Start Spark.
    println("\n### Starting Spark\n")
    val sparkConf = new SparkConf().setAppName("Triangle Counting")
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
//    val gp = g.partitionBy(new DBH, numPartitions)
  //  val gp = g.partitionBy(new HDRF(1.0f, numPartitions), numPartitions)
    val gp = g.partitionBy(PartitionStrategy.fromString("RandomVertexCut"), numPartitions)
//    val gp = g.partitionBy(new ImblanacedPartitioner,3)

    val triCounts = gp.triangleCount().vertices

    triCounts.take(10).foreach(println)
    // Stop Spark.
    sc.stop()
  }
}
