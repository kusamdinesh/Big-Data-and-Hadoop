name := "MergeSort"

version := "0.1"

scalaVersion := "2.13.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"