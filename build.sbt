organization := "com.github.catalystcode"
name := "streaming-rss-html"
description := "A library for reading public RSS feeds and public websites using Spark Streaming."

scalaVersion := "2.11.6"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature"
)

val sparkVersion = "2.3.2" //2.1.0

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.github.CatalystCode" %% "streaming-rss-html" % "1.0.2",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
).map(_ % "compile")

libraryDependencies ++= Seq(
  "com.rometools" % "rome" % "1.8.0",
  "org.jsoup" % "jsoup" % "1.10.3",
  "log4j" % "log4j" % "1.2.17"
)

libraryDependencies ++= Seq(
  "org.mockito" % "mockito-core" % "2.8.47",
  "org.mockito" % "mockito-inline" % "2.8.47",
  "org.scalatest" %% "scalatest" % "2.2.1"
).map(_ % "test")

assemblyMergeStrategy in assembly := {                                          
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard                    
 case x => MergeStrategy.first                                                  
}