package com.avcdata.config

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by Stuart Alex on 2015/11/9.
 */
object config {
  val configuration: Config = ConfigFactory.load

  def getString(key: String): String = {
    configuration.getString(key)
  }

  /**
   * Use this function to get whole data of a table in mssqlserver as a DataFrame
   * @param sqlContext
   * @param urlAndPort
   * @param username
   * @param password
   * @param database
   * @param sourceTable
   * @return
   */
  def readFromMSSQLServer(sqlContext: SQLContext, urlAndPort: String, username: String, password: String, database: String, sourceTable: String): DataFrame = {
    val url = s"jdbc:sqlserver://$urlAndPort"
    val properties = new Properties
    properties.put("databasename", database)
    properties.put("username", username)
    properties.put("password", password)
    properties.put("useunicode", "true")
    properties.put("characterEncoding", "utf-8")
    return sqlContext.read.jdbc(url, sourceTable, properties)
  }

  /**
   * Use this function to query from hive, but only can use in spark-submit, that is to say, this function can not use in local-mode
   * @param hiveContext
   * @param statement
   * @return
   */
  def readFromHive(hiveContext: HiveContext, statement: String): DataFrame = {
    return hiveContext.sql(statement)
  }

  def printStage(sum: Int, present: Int, logger: Logger): Unit = {
    var stage = "["
    for (i <- 1 to present)
      stage = stage + "="
    stage = stage + ">"
    for (i <- present + 1 to sum)
      stage = stage + " "
    stage = stage + "("
    for (i <- 1 to sum.toString.length - present.toString.length)
      stage = stage + " "
    logger.warn(s"$stage$present/$sum)]")
  }
}