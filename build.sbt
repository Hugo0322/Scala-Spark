name := "pruebasScala"

scalaVersion := "2.12.12"

// Spark library dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.4",
  "org.apache.spark" %% "spark-sql" % "3.3.4"
)