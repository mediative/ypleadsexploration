import sbt._
import Keys._
import com.typesafe.sbt.SbtStartScript
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

object YPLeadsExplorationBuild extends Build {
  val PROJECT_NAME = "ypleadsexploration"
  val HADOOP_JOBRUNNER = "sss.scalding.JobRunner"

  var commonResolvers = Seq(
    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "spy" at "http://files.couchbase.com/maven2/",
    "Twitter" at "http://maven.twttr.com/"
  )

  var commonDeps = Seq(
    "org.scalaz" %% "scalaz-core" % "7.0.6",
    "com.chuusai" % "shapeless" % "2.0.0" cross CrossVersion.full,
    "org.scalatest" %% "scalatest" % "2.1.6" % "test,it",
    "junit" % "junit" % "4.10" % "test,it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test,it"
  )

  var hadoopResolvers = Seq( // scalding, cascading etc
    "Concurrent Maven Repo" at "http://conjars.org/repo",
    "clojars.org" at "http://clojars.org/repo")

  val hadoopDeps = Seq(
    "com.twitter" %% "scalding-core" % "0.10.0",
    "com.twitter" %% "scalding-avro" % "0.10.0",
    "com.twitter" %% "scalding-commons" % "0.10.0"
    // "org.apache.hadoop" % "hadoop-core" % "2.3.0-mr1-cdh5.0.1" % "provided",
  )

  def configureScalariform(pref: IFormattingPreferences): IFormattingPreferences = {
    pref
      .setPreference(AlignParameters, true)
  }

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ scalariformSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "sss",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.4",
    ScalariformKeys.preferences := configureScalariform(FormattingPreferences())
  )

  import sbtassembly.Plugin._
  import AssemblyKeys._
  import sbtavro.SbtAvro._

  lazy val hadoopSettings = defaultSettings ++ assemblySettings ++ avroSettings ++ Seq(
    resolvers ++= hadoopResolvers,
    libraryDependencies ++= hadoopDeps,

    version in avroConfig := "1.7.5", // remove this if you want cdh5 avro to be
                                      // pulled in
    stringType in avroConfig := "String",

    // Slightly cleaner jar name
    jarName in assembly := { name.value + "-" + version.value + ".jar" },
    test in assembly := {}, // ignore tests
    mainClass in assembly := Some(HADOOP_JOBRUNNER),
    // NOTE: specifying the main class for Compile,run is currently somewhat
    // buggy because of this
    // [[https://github.com/sbt/sbt/issues/850][bug]]. it's also not really
    // needed but adding here for completeness
    mainClass in (Compile, run) := Some(HADOOP_JOBRUNNER),
    // Drop these jars
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes: Set[String] = Set(
        "minlog-1.2.jar" // Otherwise causes conflicts with Kryo (which bundles it)
        , "commons-beanutils-core-1.8.0.jar" // Clash with each other and with commons-collections
        , "commons-beanutils-1.7.0.jar" // "
        , "asm-3.1.jar" // there's already asm-4.0
        , "jsp-2.1-6.1.14.jar"
      )
      cp filter { jar => excludes(jar.data.getName) }
    },

    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) =>
        {
          case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
          case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
          case "project.clj" => MergeStrategy.discard // Leiningen build files
          case x => old(x)
        }
    })

  lazy val root = Project(PROJECT_NAME, file("."))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .aggregate(core, scalding)

  lazy val core = Project(PROJECT_NAME+"-core", file(PROJECT_NAME+"-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)

  lazy val scalding = Project(s"${PROJECT_NAME}-scalding", file(s"${PROJECT_NAME}-scalding"))
    .configs(IntegrationTest)
    .settings(hadoopSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .dependsOn(core)
}
