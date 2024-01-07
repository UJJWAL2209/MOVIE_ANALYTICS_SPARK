name := "Movie_analytics"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

val hadoop_version = "3.3.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion


libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion


libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion


libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion


// Hadoop dependencies// Hadoop dependencies

libraryDependencies +="org.apache.hadoop" % "hadoop-common" % hadoop_version
libraryDependencies +="org.apache.hadoop" % "hadoop-client" % hadoop_version

libraryDependencies +="org.apache.hadoop" % "hadoop-hdfs" % hadoop_version

// https://mvnrepository.com/artifact/org.scalanlp/breeze
libraryDependencies += "org.scalanlp" %% "breeze" % "2.1.0"


