name := "spark-consumer"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0"

