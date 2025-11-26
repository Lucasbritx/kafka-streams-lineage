name := "kafka-streams-lineage"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.12"

val kafkaVersion = "3.6.0"
val logbackVersion = "1.4.11"
val scalaLoggingVersion = "3.9.5"
val circeVersion = "0.14.6"
val openLineageVersion = "0.22.0"
val marquezVersion = "0.25.0"

libraryDependencies ++= Seq(
  // Kafka Streams
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  
  // JSON serialization
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  
  // OpenLineage
  "io.openlineage" % "openlineage-java" % openLineageVersion,
  
  // HTTP client for API calls
  "org.apache.httpcomponents" % "httpclient" % "4.5.14",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  
  // Logging
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  
  // Testing
  "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.12" % Test
)

// Assembly settings for fat JAR
assembly / mainClass := Some("com.lineage.kafka.LineageTrackingApp")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"