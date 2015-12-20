name := "log-replication"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka"     % "0.9.0.0",
  "io.netty"          % "netty-all" % "4.0.33.Final"
)