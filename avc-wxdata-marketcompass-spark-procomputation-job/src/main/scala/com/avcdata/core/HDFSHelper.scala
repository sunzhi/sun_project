package com.avcdata.core

import java.io.File
import java.net.URI
import java.sql.ResultSet
import java.util.Date
import com.avcdata.config.config
import com.avcdata.wxdata.marketcompass.JobStarter
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path, FileSystem}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Created by Stuart Alex on 2015/12/24.
 */
object HDFSHelper {
  val logger = Logger.getLogger(this.getClass)
  val env = JobStarter.env

  def writeToFile(dataFrame: DataFrame, file: File, overwrite: Boolean, writingMode: Int, numberPerBatch: Int): Boolean = {
    logger.warn(s"write data to file ${file.getAbsoluteFile}")
    val start = new Date
    if (file.exists && overwrite)
      file.delete
    if (writingMode == WritingMode.BATCH) {
      //version one
      var count = 0
      var total = 0
      var stringBuilder = new StringBuilder
      dataFrame.collect.foreach(row => {
        stringBuilder.append(row.mkString("\t").replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
        count += 1
        if (count % numberPerBatch == 0) {
          Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
          stringBuilder = new StringBuilder
          count = 0
          total += 1
          logger.warn(s"batch $total accomplished, total " + total * numberPerBatch + " rows")
        }
      })
      if (!stringBuilder.isEmpty) {
        total += 1
        logger.warn(s"batch $total accomplished, total " + (total * numberPerBatch - numberPerBatch + count) + " rows")
        Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
      }
    }
    else if (writingMode == WritingMode.ONCE_FOR_ALL) {
      //version two
      var stringBuilder = new StringBuilder
      dataFrame.collect.foreach(row => {
        stringBuilder.append(row.mkString("\t").replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
      })
      if (stringBuilder.isEmpty) {
        logger.warn("content is empty")
        return false
      }
      if (overwrite)
        Files.write(stringBuilder.toString.getBytes(Charsets.UTF_8), file)
      else
        Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
    } else {
      //version three
      dataFrame.collect.foreach(row => {
        var stringBuilder = new StringBuilder
        stringBuilder.append(row.mkString("\t").replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", "")).append(System.lineSeparator)
        Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
      })
    }
    if (file.exists) {
      logger.warn("file writing accomplished")
      logger.warn(s"consumed ${(new Date().getTime() - start.getTime) / 1000f} seconds to accomplish all mission")
      return true
    }
    return false
  }

  def writeToFile(resultSet: ResultSet, file: File, overwrite: Boolean, writingMode: Int, numberPerBatch: Int): Boolean = {
    logger.warn(s"write data to file ${file.getAbsoluteFile}")
    if (file.exists && overwrite)
      file.delete

    val start = new Date
    val columnCount = resultSet.getMetaData.getColumnCount

    if (writingMode == WritingMode.BATCH) {
      //version one
      var count = 0
      var batchCount = 0
      var stringBuilder = new StringBuilder
      while (resultSet.next) {
        for (i <- 1 to columnCount) {
          val obj = resultSet.getString(i)
          if (obj != null)
            stringBuilder.append(obj.replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", ""))
          stringBuilder.append("\t")
        }
        stringBuilder.append(System.lineSeparator)
        count += 1
        if (count % numberPerBatch == 0) {
          Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
          stringBuilder = new StringBuilder
          count = 0
          batchCount += 1
          logger.warn(s"batch $batchCount accomplished, total " + batchCount * numberPerBatch + " rows")
        }
      }
      if (!stringBuilder.isEmpty) {
        batchCount += 1
        logger.warn(s"batch $batchCount accomplished, total " + (batchCount * numberPerBatch - numberPerBatch + count) + " rows")
        Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
      }
    }
    else if (writingMode == WritingMode.ONCE_FOR_ALL) {
      //version two
      var stringBuilder = new StringBuilder
      while (resultSet.next) {
        for (i <- 1 to columnCount) {
          val obj = resultSet.getString(i)
          if (obj != null)
            stringBuilder.append(obj.replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", ""))
          stringBuilder.append("\t")
        }
        stringBuilder.append(System.lineSeparator)
      }
      if (stringBuilder.isEmpty) {
        logger.warn("content is empty")
        return false
      }
      if (overwrite)
        Files.write(stringBuilder.toString.getBytes(Charsets.UTF_8), file)
      else
        Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
    } else {
      //version three
      while (resultSet.next) {
        var stringBuilder = new StringBuilder
        for (i <- 1 to columnCount) {
          val obj = resultSet.getString(i)
          if (obj != null)
            stringBuilder.append(obj.replace("NULL", "").replace("null", "").replace("\r", "").replace("\n", ""))
          stringBuilder.append("\t")
        }
        stringBuilder.append(System.lineSeparator)
        Files.append(stringBuilder.subSequence(0, stringBuilder.length), file, Charsets.UTF_8)
      }
    }
    if (file.exists) {
      logger.warn("file writing accomplished")
      logger.warn(s"consumed ${(new Date().getTime() - start.getTime) / 1000f} seconds to accomplish all mission")
      return true
    }
    return false
  }

  def upload(dbName: String, tableName: String, file: File, deleteFile: Boolean): Boolean = {
    logger.warn(s"uploading file ${file.getAbsoluteFile} to $tableName")
    val conf: Configuration = new Configuration
    val hadoopFS: FileSystem = FileSystem.get(URI.create(config.getString("data.destination.hdfs.url")), conf)
    val hadoopPath: Path = new Path(file.getName.replace("__", "/"))
    val fsDataOutputStream: FSDataOutputStream = hadoopFS.create(hadoopPath, true)
    val fsDataInputStream: FSDataInputStream = FileSystem.getLocal(conf).open(new Path(file.getAbsolutePath
      .replace(s"_user_hive_warehouse_$dbName.db_${tableName}_", "")))
    val buffer: Array[Byte] = new Array[Byte](1024 * 1024 * 10)
    var readNumber: Int = fsDataInputStream.read(buffer)
    while (readNumber > 0) {
      fsDataOutputStream.write(buffer, 0, readNumber)
      readNumber = fsDataInputStream.read(buffer)
    }
    fsDataOutputStream.close
    fsDataInputStream.close
    logger.warn(s"file ${file.getAbsoluteFile} upload accomplished")
    if (env == "dev" && deleteFile)
      file.delete()
    return true
  }

  def mkhivedir(dbName: String, tableName: String, dir:String): Boolean = {
    val conf: Configuration = new Configuration
    val hadoopFS: FileSystem = FileSystem.get(URI.create(config.getString("data.destination.hdfs.url")), conf)
    val hadoopPath: Path = new Path(s"/usr/hive/warehouse/$dbName.db/$tableName/dir/")
    return hadoopFS.mkdirs(hadoopPath)
  }

  def mkdir(dir:String): Boolean = {
    val conf: Configuration = new Configuration
    val hadoopFS: FileSystem = FileSystem.get(URI.create(config.getString("data.destination.hdfs.url")), conf)
    val hadoopPath: Path = new Path(dir)
    return hadoopFS.mkdirs(hadoopPath)
  }

}