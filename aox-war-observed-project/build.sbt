name := "aox_war_observed_project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.12",
  "org.apache.spark" % "spark-core_2.10" % "1.5.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.1" excludeAll ExclusionRule(organization = "org.apache.hive")
)


unmanagedBase <<= baseDirectory { base => base / "lib" }
    