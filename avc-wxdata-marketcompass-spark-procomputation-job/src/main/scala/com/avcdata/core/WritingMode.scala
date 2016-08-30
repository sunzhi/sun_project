package com.avcdata.core

/**
 * Created by Stuart Alex on 2015/11/13.
 */
object WritingMode extends Enumeration {
  private type Margin = Value
  val ONCE_FOR_ALL = 0
  val ROW_BY_ROW = 1
  val BATCH = 2
}