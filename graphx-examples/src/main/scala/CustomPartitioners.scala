import org.apache.spark.graphx.{PartitionID, PartitionStrategy, VertexId}
import partitioning.{CoordinatedPartitionState, CoordinatedRecord, Record}
import scala.collection.mutable
import scala.util.Random

class ImblanacedPartitioner extends PartitionStrategy{
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    0
  }
}

object DBH extends PartitionStrategy {
  var state : CoordinatedPartitionState = null
  var numParts : Int = 1

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    if(state == null){
      state = new CoordinatedPartitionState(numParts)
    }

    val recordSrc: Record = state.getRecord(src.toInt)
    val recordDst: Record = state.getRecord(dst.toInt)

    var sleep = 1024
    while (!recordSrc.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
    }
    sleep = 2
    while (!recordDst.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
      if (sleep > 10) {
        recordSrc.releaseLock
        return getPartition(src, dst, numParts)
        //TO AVOID DEADLOCK}
      }
    }


    val dSrc = recordSrc.getDegree
    val dDst = recordDst.getDegree

    var pid = 0
    if (dSrc < dDst) pid = math.abs(src.hashCode()) % numParts else pid = math.abs(dst.hashCode()) % numParts

    state.incrementMachineLoad(pid)
    //UPDATE RECORDS
    if (state.getClass eq classOf[CoordinatedPartitionState]) {
      val cord_state: CoordinatedPartitionState = state.asInstanceOf[CoordinatedPartitionState]
      //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
    }
    else { //1-UPDATE RECORDS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
      }
    }

    //3-UPDATE DEGREES
    recordSrc.incrementDegree
    recordDst.incrementDegree

    //*** RELEASE LOCK
    recordSrc.releaseLock
    recordDst.releaseLock

    pid
  }

}

object HDRF extends PartitionStrategy {
  var state : CoordinatedPartitionState = null
  val epsilon = 1
  var lambda : Float = 1.0f

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    if(state == null){
      state = new CoordinatedPartitionState(numParts)
    }
    val recordSrc: Record = state.getRecord(src.toInt)
    val recordDst: Record = state.getRecord(dst.toInt)

    // Get the locks otherwise, sleepy time
    var sleep = 2
    while (!recordSrc.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
    }
    sleep = 2
    while (!recordDst.getLock) {
      try
        Thread.sleep(sleep)
      catch {
        case ex: Exception =>

      }
      sleep = Math.pow(sleep, 2).toInt
      if (sleep > 1024) {
        recordSrc.releaseLock
        return getPartition(src, dst, numParts)
        //TO AVOID DEADLOCK}
      }
    }
    // LOCK TAKEN


    val minLoad = state.getMinLoad
    val maxLoad = state.getMaxLoad

    var maxScore = 0.0

    var candidates: List[PartitionID] = List()
    val dSrc = recordSrc.getDegree + 1
    val dDst = recordDst.getDegree + 1

    val dSum = dSrc + dDst

    //    println("Membership information: " + membership.toList.toString())
    //    println("Load information: " + load.toList.toString())
    for (pid <- 0 to numParts - 1) {

      var fu = 0.0
      var fv = 0.0
      if (recordSrc.hasReplicaInPartition(pid)) {
        fu = dSrc
        fu /= dSum
        fu = 1 + (1 - fu)
      }

      if (recordDst.hasReplicaInPartition(pid)) {
        fv = dDst
        fv /= dSum
        fv = 1 + (1 - fv)
      }

      val l = state.getMachineLoad(pid)
      var bal = (maxLoad - l).toDouble
      //        println(s"Load of pid ${pid} is ${l}")
      //        println(s"Balance score: ${bal}")
      bal /= (epsilon + maxLoad - minLoad).toDouble
      if (bal < 0.0) {
        bal = 0.0
      }

      val score = fu + fv + lambda * bal
      //        println(s"Score for partition: ${pid} is ${score}")
      //        setScore(score)

      if (score > maxScore) {
        maxScore = score
        candidates = List()
        candidates = candidates :+ pid
      } else if (score == maxScore) {
        candidates = candidates :+ pid
      }

    }
    val rand = new Random()
    //    println(candidates.size)
    val pid = candidates(rand.nextInt(candidates.size))
    //    println("\n" + "Candidates are: " + candidates.toList.toString())
    //
    // Instead of random selection, let's select a deterministic partition. It will make our life easier
    //    val pid = candidates(0)

    state.incrementMachineLoad(pid)
    //UPDATE RECORDS
    if (state.getClass eq classOf[CoordinatedPartitionState]) {
      val cord_state: CoordinatedPartitionState = state.asInstanceOf[CoordinatedPartitionState]
      //NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
        cord_state.incrementMachineLoadVertices(pid)
      }
    }
    else { //1-UPDATE RECORDS
      if (!(recordSrc.hasReplicaInPartition(pid))) {
        recordSrc.addPartition(pid)
      }
      if (!(recordDst.hasReplicaInPartition(pid))) {
        recordDst.addPartition(pid)
      }
    }

    //3-UPDATE DEGREES
    recordSrc.incrementDegree
    recordDst.incrementDegree

    //*** RELEASE LOCK
    recordSrc.releaseLock
    recordDst.releaseLock

    pid
  }

}

class ParallelDBH extends PartitionStrategy{
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

class ParallelHDRF(lambda: Float, numParts: Int) extends PartitionStrategy{
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

    val rand = new Random()
//    println(candidates.size)
    val pid = candidates(rand.nextInt(candidates.size))
    // val pid = candidates(0)
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
