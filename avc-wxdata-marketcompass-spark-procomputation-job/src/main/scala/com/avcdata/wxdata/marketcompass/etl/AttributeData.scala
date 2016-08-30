package com.avcdata.wxdata.marketcompass.etl

import java.io.File
import java.util.UUID

import com.avcdata.config.config
import com.avcdata.core.{HDFSHelper, HiveHelper, Transformer, WritingMode}
import com.databricks.spark.csv._
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by Stuart Alex on 2015/11/10.
 */
object AttributeData {
  val logger = Logger.getLogger(this.getClass)
  val db = config.getString("data.destination.hive.dbname")
  val destination = "attributes"

  def start(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)
    //get table of categories then register it
    var categories = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
      config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
      config.getString("data.source.salesrecord.database"), "品类表").select("品类")
    //      .filter("品类='微波炉' OR 品类='壁挂炉'")
    categories = categories.withColumn("品类", Transformer.getATfromCategory(categories("品类"))).withColumnRenamed("品类", "属性表")
    var tables = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
      config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
      config.getString("data.source.salesrecord.database"), "SYSOBJECTS")
      .select("name").filter("name like 'att%属性表'")
    tables = tables.join(categories, tables("name") === categories("属性表")).drop("属性表")
    //    extract_bysingletempfile(sqlContext, tables)
    extract_bymultipletempfile(sqlContext, tables)
  }

  def extract_bysingletempfile(sqlContext: SQLContext, tables: DataFrame) = {
    logger.warn("task will be finished by function extract_bysingletempfile")
    //do extract to every element of tables
    val destination = s"output/destination.csv"
    val destinationFile = new File(destination)
    val temp = s"output/temp.csv"
    val tempFile = new File(temp)
    var first = true
    val sum = tables.collect.size
    var present = 1;
    tables.collect.foreach(table => {
      config.printStage(sum, present, logger)
      present = present + 1
      val name = table.getString(table.fieldIndex("name"))
      var attributeTable = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
        config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
        config.getString("data.source.salesrecord.database"), name)
      var stringBuilder = new StringBuilder
      stringBuilder.append(attributeTable.columns.mkString("\t").replace("、", "").replace("/", "").replace(".", "").replace("(", "").replace(")", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
      attributeTable.collect.foreach(row => {
        stringBuilder.append(row.mkString("\t").replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
      })
      Files.write(stringBuilder.toString.getBytes(Charsets.UTF_8), tempFile)
      //      attributeTable = sqlContext.csvFile(realPath(tempFile), true, '\t').drop("zzid")
      attributeTable = new CsvParser().withCharset("utf-8").withUseHeader(true).withDelimiter('\t').csvFile(sqlContext, realPath(tempFile)).drop("zzid")
      attributeTable = attributeTable
        .withColumn("brandmodel", Transformer.Brand_Model_Combination(attributeTable("品牌"), attributeTable("机型")))
        .withColumn("details", Transformer.Empty())
      for (i <- 1 to attributeTable.columns.length) {
        val columnname = attributeTable.columns(i - 1)
        columnname match {
          case "品牌" => attributeTable = attributeTable.withColumnRenamed("品牌", "brand")
          case "机型" => attributeTable = attributeTable.withColumnRenamed("机型", "model")
          case "机型编码" => attributeTable = attributeTable.withColumnRenamed("机型编码", "modelcode")
          case "品类" => attributeTable = attributeTable.withColumnRenamed("品类", "category")
          case "审核标记" =>
          case "brandmodel" =>
          case "details" =>
          case _ => attributeTable = attributeTable.withColumn("details", Transformer.map_add(attributeTable("details"), lit(":"), lit(","), lit(Transformer.hanyuToPinyin(columnname)), attributeTable(columnname)))
        }
      }
      attributeTable = attributeTable.select("brandmodel", "brand", "model", "modelcode", "category", "details")
      stringBuilder.clear
      if (first) {
        stringBuilder.append(attributeTable.columns.mkString("\t").replace("、", "").replace("/", "").replace(".", "").replace("(", "").replace(")", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
        attributeTable.collect.foreach(row => {
          stringBuilder.append(row.mkString("\t").replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
        })
        Files.write(stringBuilder.toString.getBytes(Charsets.UTF_8), destinationFile)
        first = false
      } else {
        attributeTable.collect.foreach(row => {
          stringBuilder.append(row.mkString("\t").replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
        })
        Files.append(stringBuilder.subSequence(0, stringBuilder.length), destinationFile, Charsets.UTF_8)
      }
    })
    val newAttributes = transform(sqlContext, sqlContext.csvFile(realPath(destinationFile), true, '\t'))
    load(newAttributes)
    tempFile.delete
    destinationFile.delete
  }

  def extract_bymultipletempfile(sqlContext: SQLContext, tables: DataFrame): Unit = {
    logger.warn("task will be finished by function extract_bymultipletempfile")
    //do extract to every element of tables
    var newAttributes: DataFrame = null
    val sum = tables.collect.size
    var present = 1;
    tables.collect.foreach(table => {
      config.printStage(sum, present, logger)
      present = present + 1
      val name = table.getString(table.fieldIndex("name"))
      var attributeTable = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
        config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
        config.getString("data.source.salesrecord.database"), name)
      var stringBuilder = new StringBuilder
      stringBuilder.append(attributeTable.columns.mkString("\t").replace("、", "").replace("/", "").replace(".", "").replace("(", "").replace(")", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
      attributeTable.collect.foreach(row => {
        stringBuilder.append(row.mkString("\t").replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
      })
      val temp = s"output/temp-${UUID.randomUUID()}.csv"
      val tempFile = new File(temp)
      Files.write(stringBuilder.toString.getBytes(Charsets.UTF_8), tempFile)
      //      var cleanedAT = sqlContext.csvFile(realPath(tempFile), true, '\t').drop("zzid")
      var cleanedAT = new CsvParser().withCharset("utf-8").withUseHeader(true).withDelimiter('\t').csvFile(sqlContext, realPath(tempFile)).drop("zzid")
      cleanedAT = cleanedAT
        .withColumn("brandmodel", Transformer.Brand_Model_Combination(cleanedAT("品牌"), cleanedAT("机型")))
        .withColumn("details", Transformer.Empty())
      for (i <- 1 to cleanedAT.columns.length) {
        val columnname = cleanedAT.columns(i - 1)
        columnname match {
          case "品牌" => cleanedAT = cleanedAT.withColumn("品牌", Transformer.toUpper(cleanedAT("品牌"))).withColumnRenamed("品牌", "brand")
          case "机型" => cleanedAT = cleanedAT.withColumn("机型", Transformer.toUpper(cleanedAT("机型"))).withColumnRenamed("机型", "model")
          case "机型编码" => cleanedAT = cleanedAT.withColumnRenamed("机型编码", "modelcode")
          case "品类" => cleanedAT = cleanedAT.withColumnRenamed("品类", "category")
          case "审核标记" =>
          case "brandmodel" =>
          case "details" =>
          case _ => cleanedAT = cleanedAT.withColumn("details", Transformer.map_add(cleanedAT("details"), lit(":"), lit(","), lit(Transformer.hanyuToPinyin(columnname)), cleanedAT(columnname)))
        }
      }
      cleanedAT = cleanedAT.select("brandmodel", "brand", "model", "modelcode", "category", "details")
      if (newAttributes == null) {
        newAttributes = cleanedAT
      }
      else {
        newAttributes = newAttributes.unionAll(cleanedAT)
      }
    })
    newAttributes = transform(sqlContext, newAttributes)
    load(newAttributes)
    val files: Array[File] = new File("output").listFiles()
    for (i <- 1 to files.length) {
      logger.debug(files(i - 1).getAbsolutePath)
      if (files(i - 1).getAbsolutePath.contains("temp"))
        files(i - 1).delete
    }
  }

  def transform(sqlContext: SQLContext, newAttributes: DataFrame): DataFrame = {
    val model = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
      config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
      config.getString("data.source.salesrecord.database"), "型号表")
      .select("机型编码", "上市月度", "上市周度", "子品牌")
      .withColumnRenamed("机型编码", "机型编码待删除")
      .withColumnRenamed("上市月度", "shangshiyuedu")
      .withColumnRenamed("上市周度", "shangshizhoudu")
      .withColumnRenamed("子品牌", "subbrand")
    var brandinfo = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
      config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
      config.getString("data.source.salesrecord.database"), "品牌表")
      .select("品牌", "品牌类型", "国别")
      .withColumnRenamed("品牌", "品牌待删除")
      .withColumnRenamed("品牌类型", "brandtype")
      .withColumnRenamed("国别", "brandnation")
    brandinfo = brandinfo.withColumn("品牌待删除", Transformer.toUpper(brandinfo("品牌待删除")))
    return newAttributes
      .join(model, newAttributes("modelcode") === model("机型编码待删除"))
      .join(brandinfo, newAttributes("brand") === brandinfo("品牌待删除"))
      .drop("机型编码待删除").drop("品牌待删除")
      .select("brandmodel", "brand", "subbrand", "brandtype", "brandnation", "model", "modelcode", "category", "shangshiyuedu", "shangshizhoudu", "details")
  }

  def load(newAttributes: DataFrame): Unit = {
    val fileName = s"output/__user__hive__warehouse__${db}.db__${destination}__${UUID.randomUUID()}.csv"
    val file: File = new File(fileName)
    if (HDFSHelper.writeToFile(newAttributes, file, true, WritingMode.ONCE_FOR_ALL, 0)) {
      HiveHelper.truncate(db, destination)
      HDFSHelper.upload(db, destination, file, true)
    }
  }

  def realPath(file: File): String = {
    return s"file://${file.getAbsolutePath}"
  }

}