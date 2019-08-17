import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.hashing.Hashing
import scala.util.hashing.{MurmurHash3 => MH3}
import scala.io.Source

object ReadAndPartition {
  def extremeN[T](n: Int, li: List[T])
                 (comp1: ((T, T) => Boolean), comp2: ((T, T) => Boolean)):
  List[T] = {

    def sortedIns(el: T, list: List[T]): List[T] =
      if (list.isEmpty) List(el) else if (comp2(el, list.head)) el :: list else
        list.head :: sortedIns(el, list.tail)

    def updateSofar(sofar: List[T], el: T): List[T] =
      if (comp1(el, sofar.head))
        sortedIns(el, sofar.tail)
      else sofar

    (li.take(n).sortWith(comp2(_, _)) /: li.drop(n)) (updateSofar(_, _))
  }

  def top[T](n: Int, li: List[T])
            (implicit ord: Ordering[T]): Iterable[T] = {
    extremeN(n, li)(ord.lt(_, _), ord.gt(_, _))
  }

  def bottom[T](n: Int, li: List[T])
               (implicit ord: Ordering[T]): Iterable[T] = {
    extremeN(n, li)(ord.gt(_, _), ord.lt(_, _))
  }

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
    val k = 100

    println(s"\n### Loading edge list: ${path}\n")
    // Source.fromFile(path).getLines().foreach(println)

    val g: Graph[Int, Int] = GraphLoader.edgeListFile(
      sc,
      path,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK
    )

//    val gp = g.partitionBy(new ImblanacedPartitioner, numPartitions)
    val partitioningAlgorithm = List("RandomVertexCut", "CanonicalRandomVertexCut", "EdgePartition1D", "EdgePartition2D")
    val gp = g.partitionBy(PartitionStrategy.fromString(partitioningAlgorithm(2)), numPartitions)



    val signatures = gp.edges.mapPartitionsWithIndex((index,it) => {
//      val li = List[Int]()
      var minK = List.empty[Int]
      var vertices : Set[VertexId] = Set()

      while(it.hasNext){
        val s = it.next()
        if (!vertices.contains(s.srcId)) {
          val sig = MH3.stringHash(s.srcId.toString, 0)
          minK = minK:+sig
          vertices = vertices+s.srcId
        }
        if (!vertices.contains(s.dstId)) {
          val sig = MH3.stringHash(s.dstId.toString, 0)
          minK = minK:+sig
          vertices = vertices+s.dstId
        }

        minK = top(k, minK).toList
      }
      minK.toIterator
    }, preservesPartitioning = true)

//    val builder = List.newBuilder[Set[Int]]
//    signatures.getNumPartitions
//
//    signatures.foreachPartition(it => builder.+=(it.collect()))
//    val sigs = signatures.collect().toSet
    val sigswithIndex = signatures.mapPartitionsWithIndex((index, it) => {
      var sigWithIndex : List[(Int, Int)] = List()
      while(it.hasNext){
          sigWithIndex = sigWithIndex:+(index, it.next())
      }
      sigWithIndex.toIterator
    },preservesPartitioning = true)
    val arr = sigswithIndex.collect()
    var map: mutable.HashMap[Int, Set[Int]] = mutable.HashMap()
    arr.foreach{
      case (index: Int, sig:Int) => {
      if(!map.contains(index)){

        map.put(index, Set(sig))
      }else{
        var tmp: Set[Int] = map(index)
        tmp+=sig
        map.put(index, tmp)
      }
      }
    }
    map.foreach{
      case (index, sigs) => {

        map.foreach{
          case (index1, sigs1) =>{
            if (index != index1) {
              println(s"Between Index: ${index} and Index: ${index1}")
              val u = sigs.union(sigs1)
              val i = u.intersect(sigs).intersect(sigs1)
              println(i.size/k.floatValue() )
            }
          }
        }
      }
    }

    sc.stop()
  }
}