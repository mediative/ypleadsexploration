import sbt.Keys._

object ScalacSettings {

  val scalacSettings = Seq(
    // cf higherKinds: http://stackoverflow.com/questions/6246719/what-is-a-higher-kinded-type-in-scala
    // postfixOps: to be able to do stuff like "Set(1,2,3) toSeq"
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:higherKinds", "-language:implicitConversions", "-language:postfixOps")
  )

}
