package partitioning

object Utils {
  type PartitionID = Int
  type VertexId = Long

}
class Edge(src: Long, dst: Long){

  def getSrc(): Long ={
    src
  }

  def getDst(): Long ={
    dst
  }

}
