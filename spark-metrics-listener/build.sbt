name := "spark-metrics-listener"

version := "1.0.0"

scalaVersion := "2.12.17"

organization := "com.company"

// Spark 3.4.x dependencies - marked as "provided" since Databricks supplies them
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
  "io.delta" %% "delta-core" % "2.4.0" % "provided"
)

// Compiler options for Scala 2.12
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint:_"
)

// Assembly settings for building fat JAR (optional, for standalone deployment)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// Exclude Scala library from assembly since it's provided by Spark
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

// JAR name without Scala version suffix for cleaner deployment
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${artifact.name}-${module.revision}.jar"
}

// Test settings
Test / fork := true
Test / parallelExecution := false

// Publish settings (optional, for internal artifact repository)
publishMavenStyle := true
