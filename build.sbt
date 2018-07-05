name := "spark-consumer"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

