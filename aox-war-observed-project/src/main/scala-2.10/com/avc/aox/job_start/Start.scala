package com.avc.aox.job_start

import com.avc.aox.service.Marketshare_channel

/**
 * Created by dev on 16-3-31.
 */
object Start {
  def main(args: Array[String]) {
    println("Aox Start--------------------->")
    Marketshare_channel.start()

    println("Aox end----------------------->")
  }

}
