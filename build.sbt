name := "spark_custom_connector"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"
val scalaTestVersion = "3.2.7"
val containersTestVersion = "0.39.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,

  "org.postgresql" % "postgresql" % "42.2.19",

  "org.scalatest" %% "scalatest" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % containersTestVersion % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % containersTestVersion % Test
)
