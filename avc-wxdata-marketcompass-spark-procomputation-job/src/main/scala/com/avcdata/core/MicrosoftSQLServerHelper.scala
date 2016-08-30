package com.avcdata.core

import java.sql.{DriverManager, PreparedStatement, ResultSet, Connection}

import org.apache.log4j.Logger

/**
 * Created by Stuart Alex on 2015/11/9.
 */
class MicrosoftSQLServerHelper {
  val logger = Logger.getLogger(this.getClass)
  private var conn: Connection = null
  private var ps: PreparedStatement = null

  def this(serverAddress: String, databaseName: String, userName: String, password: String) = {
    this
    val driverName: String = "net.sourceforge.jtds.jdbc.Driver"
    val dbUrl: String = s"jdbc:jtds:sqlserver://$serverAddress;databaseName=$databaseName;useUnicode=true&characterEncoding=utf8;"
    Class.forName(driverName)
    conn = DriverManager.getConnection(dbUrl, userName, password)
  }

  def getResultSet(statement: String, parameters: Array[String]): ResultSet = {
    ps = conn.prepareStatement(statement, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
    if (parameters != null) {
      var i: Int = 0
      parameters.foreach(param => {
        i += 1
        ps.setObject(i, param)
      })
    }
    return ps.executeQuery
  }

  def Close {
    try {
      conn.close
      conn = null
    }
    catch {
      case e: Exception => {
        logger.error(e.getMessage)
        conn = null
      }
    }
  }

  @throws(classOf[Throwable])
  protected override def finalize {
    Close
    super.finalize
  }

}