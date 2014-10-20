import sbt.Keys._
import sbt._

object LogSettings {

  private val logResolvers = Seq(
    "Maven.org" at "http://repo1.maven.org/maven2",
    "ScalaTools releases at Sonatype" at "https://oss.sonatype.org/content/repositories/releases/",
    "ScalaTools snapshots at Sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/"
  )

  private val deps = Seq(
    // https://github.com/typesafehub/scala-logging
    // http://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging-api_2.10/2.1.2
    "com.typesafe.scala-logging" % "scala-logging-api_2.10" % "2.1.2"
    , "ch.qos.logback" % "logback-classic" % "1.0.13"
    // Tree log dependencies: (https://github.com/lancewalton/treelog)
    , "com.casualmiracles" %% "treelog" % "1.2.2" // NB: 1.2.3, as recommended on the github page, fails.
    , "org.scalaz" %% "scalaz-core" % "7.0.6"
    // TODO: this following line SHOULDN'T BE NEEDED, but without it the logging does not work. Why??????
  ,"org.apache.spark" % "spark-assembly_2.10" % "1.0.0-cdh5.1.0" % "provided" // spark-assembly
  )

  val logSettings = Seq(
    resolvers ++= logResolvers,
    libraryDependencies ++= deps
  )

}
