import sbt.ExclusionRule

name := "jd-cloth"

version := "1.0"

scalaVersion := "2.10.5"

resolvers += "my-maven-proxy-releases" at "http://nexus.avcdata.com/nexus/content/groups/public/"

libraryDependencies ++= Seq(
  "com.sun.jersey" % "jersey-bundle" % "1.19",
  "com.typesafe" % "config" % "1.2.0",
  "org.apache.spark" % "spark-core_2.10" % "1.5.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.1",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",
  "org.apache.hive" % "hive-jdbc" % "1.1.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.1" excludeAll ExclusionRule(organization = "org.apache.hive")
)

unmanagedBase <<= baseDirectory { base => base / "lib" }