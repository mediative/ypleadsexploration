import sbt.Keys._
import sbt._

object SparkSettings {

  private val deps = Seq(
    "org.apache.spark" %% "spark-core" % "1.0.0" % "provided", // spark-core
    "org.apache.spark" % "spark-assembly_2.10" % "1.0.0-cdh5.1.0" % "provided", // spark-assembly
    "org.apache.spark" % "spark-sql_2.10" % "1.0.2" % "provided" // spark-sql
  )

  val sparkSettings = Seq(
    libraryDependencies ++= deps
  )

}
