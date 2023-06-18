ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"


val flinkVersion = "1.16.1"
val slf4jVersion = "2.0.5"
val log4jVersion = "1.2.17"
val gsonVersion = "2.10.1"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" % "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-cep" % flinkVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
  "log4j" % "log4j" % log4jVersion,
  "com.google.code.gson" % "gson" % gsonVersion,
  "org.json4s" %% "json4s-jackson" % "4.0.6" % Test
)


lazy val root = (project in file("."))
  .settings(
    name := "Flink101",
    libraryDependencies ++= flinkDependencies
  )