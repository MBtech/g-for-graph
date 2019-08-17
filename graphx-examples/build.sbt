name := "graphx-examples"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-graphx" % "2.4.0"
)

initialCommands  += """
  import org.apache.spark._
  import org.apache.spark.graphx._
  import org.apache.spark.storage._
  val conf = new SparkConf().setAppName("graphx-examples")
  implicit val sc = new SparkContext(conf)
"""
