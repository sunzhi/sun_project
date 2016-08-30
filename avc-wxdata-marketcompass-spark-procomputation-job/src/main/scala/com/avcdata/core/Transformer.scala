package com.avcdata.core

import java.util.{Calendar, Date}

import net.sourceforge.pinyin4j.PinyinHelper
import net.sourceforge.pinyin4j.format.{HanyuPinyinCaseType, HanyuPinyinOutputFormat, HanyuPinyinToneType, HanyuPinyinVCharType}
import org.apache.spark.sql.functions._

/**
 * Created by Stuart Alex on 2015/11/11.
 */
object Transformer {

  def PublishMonth_LifeCycle = udf { publishMonth: String => PublishMonth_LifeCycle_Func(publishMonth) }

  private def PublishMonth_LifeCycle_Func(publishMonth: String): String = {
    //format of publish month is like [0-9][0-9].[0-9][0-9]
    val regex = "\\d{2}\\.\\d{2}"
    if (!publishMonth.matches(regex))
      return ""
    val now = new Date
    val calendar = Calendar.getInstance()
    val yearOfNow = calendar.get(Calendar.YEAR)
    val monthOfNow = calendar.get(Calendar.MONTH) + 1
    val yearOfPublish = 2000 + publishMonth.substring(0, 2).toInt
    val monthOfPublish = publishMonth.substring(3).toInt
    val value = (yearOfNow - yearOfPublish) * 12 + yearOfNow - yearOfPublish
    value match {
      case _ if (value < 6) => "半年内"
      case _ if (value < 12) => "一年内"
      case _ if (value < 18) => "一年半内"
      case _ if (value < 24) => "两年内"
      case _ => "两年以前"
    }
  }

  def Volume_VolumeSection = udf { volume: String => Volume_VolumeSection_Func(volume) }

  private def Volume_VolumeSection_Func(volume: String): String = {
    if (volume == null)
      return "a.-60L"
    val vol = volume.toInt
    vol match {
      case _ if (vol >= 0 && vol < 60) => "a.-60L"
      case _ if (vol >= 60 && vol < 100) => "b.61-100L"
      case _ if (vol >= 100 && vol < 140) => "c.101-140L"
      case _ if (vol >= 140 && vol < 160) => "d.141-160L"
      case _ if (vol >= 160 && vol < 180) => "e.161-180L"
      case _ if (vol >= 180 && vol < 200) => "f.181-200L"
      case _ if (vol >= 200 && vol < 220) => "g.201-220L"
      case _ if (vol >= 220 && vol < 240) => "h.221-240L"
      case _ if (vol >= 240 && vol < 270) => "i.241-270L"
      case _ if (vol >= 270 && vol < 300) => "j.271-300L"
      case _ if (vol >= 300 && vol < 350) => "k.301-350L"
      case _ if (vol >= 350 && vol < 400) => "l.351-400L"
      case _ if (vol >= 400 && vol < 450) => "m.401-450L"
      case _ if (vol >= 450 && vol < 500) => "n.451-500L"
      case _ if (vol >= 500 && vol < 550) => "o.501-550L"
      case _ if (vol >= 550 && vol < 600) => "p.551-600L"
      case _ if (vol >= 600 && vol < 700) => "q.601-700L"
      case _ => "r.700L+"
    }
  }

  def Brand_Model_Combination = udf { (brand: String, model: String) => Brand_Model_Combination_Func(brand, model) }

  private def Brand_Model_Combination_Func(brand: String, model: String): String = {
    return brand + "_" + model
  }

  def Empty = udf { () => "" }

  def Dash = udf { () => "-" }

  def Zero = udf { () => 0 }

  def Xtype_Type = udf { xtype: Int => Xtype_Type_Func(xtype) }

  private def Xtype_Type_Func(xtype: Int): String = {
    xtype match {
      case _ if (xtype == 56) => "int"
      case _ if (xtype == 61 || xtype == 62) => "float"
      case _ => "string"
    }
  }

  def toUpper = udf { letter: String => letter.toUpperCase }

  def Score_Rating = udf { score: Float => Score_Rating_Func(score) }

  private def Score_Rating_Func(score: Float): Int = {
    score match {
      case _ if score >= 1 => 5
      case _ if score > 0 => 4
      case _ if score == 0 => 3
      case _ if score > -1 => 2
      case _ => 1
    }
  }

  def hanyuToPinyin(hanyu: String): String = {
    var stringBuilder = new StringBuilder
    val defaultFormat = new HanyuPinyinOutputFormat()
    defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE)
    defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE)
    defaultFormat.setVCharType(HanyuPinyinVCharType.WITH_V)
    hanyu match {
      case "周度" => stringBuilder.append("week")
      case "品类" => stringBuilder.append("category")
      case "品牌" => stringBuilder.append("brand")
      case "机型" => stringBuilder.append("model")
      case "机型编码" => stringBuilder.append("modelcode")
      case "子品牌" => stringBuilder.append("subbrand")
      case "品牌机型" => stringBuilder.append("branmodel")
      case "品牌类型" => stringBuilder.append("brandtype")
      case _ =>
        for (i <- 1 to hanyu.length) {
          val ch = hanyu(i - 1)
          if (ch >= 48 && ch <= 57) stringBuilder.append(ch)
          else if (ch >= 65 && ch <= 90) stringBuilder.append(ch)
          else if (ch >= 97 && ch <= 122) stringBuilder.append(ch)
          else if (ch == '、' || ch == '（' || ch == '）' || ch == '。') stringBuilder.append("")
          else if (ch > 128) stringBuilder.append(PinyinHelper.toHanyuPinyinStringArray(ch, defaultFormat)(0))
        }
    }
    return stringBuilder.toString
  }

  def getATfromCategory = udf { category: String => s"att${category}属性表" }

  def map_add = udf { (details: String, keyTerminator: String, elementTerminator: String, key: String, value: String) =>
    if (details != "")
      s"$details$elementTerminator$key$keyTerminator$value"
    else
      s"$key$keyTerminator$value"
  }

}
