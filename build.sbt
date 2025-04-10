name := """SparkCsv"""

version := "1.0"

Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-deprecation", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused" )

unmanagedBase := baseDirectory.value / "spark-csv/lib"

Test / parallelExecution := false

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "com.phasmidsoftware" %% "tableparser" % "1.1.1",
  "com.github.nscala-time" %% "nscala-time" % "2.32.0",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)