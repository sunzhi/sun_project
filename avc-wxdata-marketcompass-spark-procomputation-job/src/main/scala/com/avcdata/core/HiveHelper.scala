package com.avcdata.core

import java.sql.{Connection, DriverManager}
import java.util
import java.util.List
import com.avcdata.config.config
import com.avcdata.wxdata.marketcompass.JobStarter
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.http.{HttpResponse, NameValuePair}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Created by Stuart Alex on 2015/11/9.
 */
object HiveHelper {
  val logger = Logger.getLogger(HiveHelper.getClass)
  val env = JobStarter.env

  def postForCDT(url: String, statement: String): Boolean = {
    val params: List[NameValuePair] = new util.ArrayList[NameValuePair]
    params.add(new BasicNameValuePair("statement", statement))
    val smsPost: HttpPost = new HttpPost(url + "?" + URLEncodedUtils.format(params, "utf-8"))
    val response: HttpResponse = HttpClientBuilder.create.build.execute(smsPost)
    if (response.getStatusLine.getStatusCode == 200)
      return true
    return false
  }

  def dropTable(dbName: String, tableName: String): Boolean = {
    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    val url: String = config.getString("data.destination.hive.url") + s"/$dbName;useUnicode=true&characterEncoding=utf8;"
    Class.forName(driverName)
    val conn: Connection = DriverManager.getConnection(url, config.getString("data.destination.hive.username"),
      config.getString("data.destination.hive.password"))
    if (env == "dev")
      return conn.prepareStatement(s"drop table if exists $dbName.$tableName").execute
    return postForCDT(config.getString("services.queryservice") + s"/drop/hive/$dbName", s"drop table $dbName.$tableName")
  }

  def createTable(dbName: String, tableName: String, data: DataFrame, translate: Boolean): Boolean = {
    logger.info(s"create table $tableName on database $dbName")
    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    val url: String = config.getString("data.destination.hive.url") + s"/$dbName;useUnicode=true&characterEncoding=utf8;"
    Class.forName(driverName)
    val conn: Connection = DriverManager.getConnection(url, config.getString("data.destination.hive.username"),
      config.getString("data.destination.hive.password"))
    var statement: String = s"create table if not exists $dbName.$tableName ("
    data.dtypes.foreach(element => {
      var name = element._1
      if (translate)
        name = Transformer.hanyuToPinyin(name)
      var fType = element._2.toLowerCase
      fType match {
        case "integertype" => fType = "int"
        case "floattype" => fType = "decimal(10,2)"
        case "doubletype" => fType = "decimal(10,2)"
        case "stringtype" => fType = "string"
        case _ if (fType.startsWith("decimaltype")) => fType = fType.replace("decimaltype", "decimal")
        case _ => fType = "string"
      }
      statement += name + " " + fType + ","
    })
    if (statement.endsWith(","))
      statement = statement.substring(0, statement.length - 1)
    statement += ") row format delimited fields terminated by '\t' stored as textfile"
    logger.info(statement)
    if (env == "dev")
      return conn.prepareStatement(statement).execute
    return postForCDT(config.getString("services.queryservice") + s"/create/hive/$dbName", statement)
  }

  def truncate(dbName: String, tableName: String): Boolean = {
    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    val url: String = config.getString("data.destination.hive.url") + s"/$dbName;useUnicode=true&characterEncoding=utf8;"
    Class.forName(driverName)
    val conn: Connection = DriverManager.getConnection(url, config.getString("data.destination.hive.username"),
      config.getString("data.destination.hive.password"))
    if (env == "dev")
      return conn.prepareStatement(s"truncate table $dbName.$tableName").execute
    return postForCDT(config.getString("services.queryservice") + s"/truncate/hive/$dbName", s"truncate table $dbName.$tableName")
  }

  def execute(dbName: String, statement: String): Boolean = {
    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    val url: String = config.getString("data.destination.hive.url") + s"/$dbName;useUnicode=true&characterEncoding=utf8;"
    Class.forName(driverName)
    val conn: Connection = DriverManager.getConnection(url, config.getString("data.destination.hive.username"), config.getString("data.destination.hive.password"))
    if (env == "dev")
      return conn.prepareStatement(statement).execute
    return postForCDT(config.getString("services.queryservice") + s"/execute/hive/$dbName", statement)
  }

}