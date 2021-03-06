import sbtrelease._
import ReleaseStateTransformations._

lazy val commonSettings = releaseSettings ++ Seq(
  name <<= name("handy-dynamo-" + _),
  organization := "com.teambytes.handy",
  scalaVersion := "2.11.5",
  crossScalaVersions := Seq("2.11.4", "2.10.4"),
  ReleaseKeys.crossBuild := true,
  libraryDependencies ++= Seq(
    "org.mockito"  %  "mockito-all" % "1.9.5" % "test",
    "org.scalatest" %% "scalatest" % "2.2.2" % "test"
  ),
  publishArtifact in Test := false,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  licenses := Seq("Apache License 2.0" -> url("http://opensource.org/licenses/Apache-2.0")),
  homepage := Some(url("https://github.com/grahamar/handy-dynamo")),
  publishTo <<= version { (v: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  pomExtra := <scm>
    <url>git@github.com:grahamar/handy-dynamo.git</url>
    <connection>scm:git:git@github.com:grahamar/handy-dynamo.git</connection>
  </scm>
  <developers>
    <developer>
      <id>grhodes</id>
      <name>Graham Rhodes</name>
      <url>https://github.com/grahamar</url>
    </developer>
  </developers>,
  sbtrelease.ReleasePlugin.ReleaseKeys.releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts.copy(action = publishSignedAction),
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val publishSignedAction = { st: State =>
  val extracted = Project.extract(st)
  val ref = extracted.get(thisProjectRef)
  extracted.runAggregated(com.typesafe.sbt.pgp.PgpKeys.publishSigned in Global in ref, st)
}

lazy val root = (project in file(".")).
  aggregate(core, replication).
  settings(commonSettings: _*).
  settings(publish := {})

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    resolvers += Resolver.bintrayRepo("dwhjames", "maven"),
    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.27" % "provided",
      "com.github.dwhjames" %% "aws-wrap" % "0.7.2"
    )
  )

lazy val replication = (project in file("replication")).
  dependsOn(core).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "com.gilt" %% "gfc-logging" % "0.0.2",
      "com.gilt" %% "gfc-time" % "0.0.4",
      "com.gilt" %% "gfc-util" % "0.0.4",
      "org.squeryl" %% "squeryl" % "0.9.5-7",
      "com.teambytes" %% "aws-leader-election" % "1.0.0"
    )
  )
