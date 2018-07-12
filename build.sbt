
lazy val root = (project in file(".")).
  settings(
    name := "parallel-tool",
    organization:= "com.tools.parallelTool",
    version := "1.0.0-00-SNAPSHOT",
    scalaVersion := "2.11.8"
  )

// disable using the Scala version in output paths and artifacts
crossPaths := false

val sparkVersion = "2.2.1"

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
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

sources in (Compile,doc) := Seq.empty
publishArtifact in (Compile, packageDoc) := false

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
