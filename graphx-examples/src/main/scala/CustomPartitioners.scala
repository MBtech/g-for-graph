import org.apache.spark.graphx.{PartitionID, PartitionStrategy, VertexId}

import scala.collection.mutable
import scala.util.Random

class ImblanacedPartitioner extends PartitionStrategy{
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    0
  }
}

class DBH extends PartitionStrategy{
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val dSrc = getDegree(src)
    val dDst = getDegree(dst)

    var pid = 0
    if (dSrc < dDst) {
      pid = math.abs(src.hashCode()) % numParts
    }else{
      pid = math.abs(dst.hashCode()) % numParts
    }
    pid
  }

  def getDegree(vertex: VertexId): Long = {
    var ret = 0L
    if (map.contains(vertex)){
      map.put(vertex, map(vertex)+1)
      ret = map(vertex)
    }else{
      map.put(vertex, 1L)
      ret = map(vertex)
    }
    ret
  }
  var map : mutable.HashMap[VertexId, Long] = mutable.HashMap()
}

class HDRF(lambda: Float, numParts: Int) extends PartitionStrategy{
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val minLoad = getMinLoad()
    val maxLoad = getMaxLoad()

    var maxScore = 0.0

    var candidates : List[PartitionID] = List()
    membership.foreach{
      case (pid, vertices) => {
        val dSrc = getDegree(src)
        val dDst = getDegree(dst)

        val dSum = dSrc + dDst

        var fu = 0.0
        var fv = 0.0
        if(vertices.contains(src)){
          fu = dSrc
          fu /= dSum
          fu = 1 + (1 - fu)
        }

        if(vertices.contains(dst)){
          fv = dDst
          fv /= dSum
          fv = 1 + (1 - fv)
        }

        val l = load(pid)
        var bal = (maxLoad - l)

        bal /= (epsilon + maxLoad - minLoad)
        if (bal < 0){
          bal = 0
        }

        val score = fu + fv + lambda * bal

        if(score > maxScore){
          maxScore = score
          candidates = List()
          candidates = candidates:+pid
        }else if (score == maxScore){
          candidates = candidates:+pid
        }

      }
    }
    // Instead of random selection, let's select a deterministic partition. It will make our life easier

//    val rand = new Random()
//    println(candidates.size)
//    candidates(rand.nextInt(candidates.size))
    val pid = candidates(0)
    var members = membership(pid)
    if(!members.contains(src)){
      load.put(pid, load(pid)+1)
    }
    if(!members.contains(dst)){
      load.put(pid, load(pid)+1)
    }
    membership.put(pid, membership(pid)+src+dst)

    pid
  }

  def getMinLoad(): Long = {
    var ret = 0L
    if(load.size > 0){
      ret = load.minBy(_._2)._2
    }
    ret
  }

  def getMaxLoad(): Long = {
    var ret = 0L
    if(load.size>0){
      ret = load.maxBy(_._2)._2
    }
    ret
  }

  def getDegree(vertex: VertexId): Long = {
    var ret = 0L
    if (degrees.contains(vertex)){
      degrees.put(vertex,degrees(vertex)+1)
      ret = degrees(vertex)
    }else{
      degrees.put(vertex, 1L)
      ret = degrees(vertex)
    }
    ret
  }

  var degrees : mutable.HashMap[VertexId, Long] = mutable.HashMap()
  val epsilon = 1
  var membership: mutable.HashMap[Int, Set[VertexId]] = {
    var tmp : mutable.HashMap[Int, Set[VertexId]] = mutable.HashMap()
    for(i <- 0 to numParts-1){
      tmp.put(i, Set[VertexId]())
    }
    tmp
  }

  var load: mutable.HashMap[Int, Long] = {
    var tmp : mutable.HashMap[Int, Long] = mutable.HashMap()
    for(i <- 0 to numParts-1){
      tmp.put(i, 0L)
    }
    tmp
  }
}