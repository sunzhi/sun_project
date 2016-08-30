package com.avcdata.wxdata.marketcompass

import com.avcdata.wxdata.marketcompass.etl.Jindong
import org.apache.log4j.Logger



/**
 * Created by Stuart Alex on 2015/11/9.
 */
object JobStarter {
  val logger = Logger.getLogger(this.getClass)
  var env: String = null

  def main(args: Array[String]): Unit = {

    Jindong.start(args)
  }

}
