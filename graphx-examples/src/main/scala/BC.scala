import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object BC {
  def main(args: Array[String]) {
    // Start Spark.
    println("\n### Starting Spark\n")
    val sparkConf = new SparkConf().setAppName("Betweenness Centrality")
    implicit val sc = new SparkContext(sparkConf)

    // Suppress unnecessary logging.
    Logger.getRootLogger.setLevel(Level.ERROR)

    // Load a graph.
    val path = args(0)
    // Number of partitions
    val numPartitions = args(1).toInt

    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Loading edge list: ${path}\n")
   // Source.fromFile(path).getLines().foreach(println)

    val g: Graph[Int, Int] = GraphLoader.edgeListFile(
      sc,
      path,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      numEdgePartitions = numPartitions
    )
    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Graph Loaded. Number of Vertices: ${g.vertices.count()}\n")

    val gp = g.partitionBy(PartitionStrategy.fromString("RandomVertexCut"), numPartitions)

    println(s"${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss").format(LocalDateTime.now)} Partitioning Done. Number of Vertices: ${gp.vertices.count()}\n")

    // Calculate centralities.
    println("\n### Degree centrality\n")
    g.degrees.sortByKey()//.collect()//.foreach { case (n, v) => println(s"Node: ${n} -> Degree: ${v}") }

    println("\n### Betweenness centrality\n")
    val h: Graph[Double, Int] = Betweenness.run(g)
    // h.vertices.sortByKey()//.collect()//.foreach { case (n, v) => println(s"Node: ${n} -> Betweenness: ${v}") }

    h.vertices.count()

    // Stop Spark.
    sc.stop()
  }
}
