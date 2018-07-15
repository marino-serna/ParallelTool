import sbt.Keys.organization

lazy val root = (project in file(".")).
  settings(
    name := "ParallelTool",
    version := "1.0.0-00",
    scalaVersion := "2.11.8",

    organization := "com.github.marino-serna",
    homepage := Some(url("https://github.com/marino-serna/ParallelTool")),
    scmInfo := Some(ScmInfo(url("https://github.com/marino-serna/ParallelTool"),
      "git@github.com:marino-serna/ParallelTool.git")),
    developers := List(Developer("marino-serna",
      "Marino Serna",
      "marinosersan@gmail.com",
      url("https://github.com/marino-serna"))),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
  )

val sparkVersion = "2.2.1"

// disable using the Scala version in output paths and artifacts
crossPaths := false
useGpg := true
publishMavenStyle := true

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

credentials += Credentials(Path.userHome / ".ivy2" / ".credentialsPublic")

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)