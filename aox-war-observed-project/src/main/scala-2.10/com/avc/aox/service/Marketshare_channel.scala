package com.avc.aox.service

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dev on 16-4-28.
 */
object Marketshare_channel {


  def start():Unit= {
    val conf = new SparkConf().setAppName("Aox_War")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    def str_deal(str:String): String={
      val stra = str.substring(0,2).toInt - 1
      val strb = stra.toString + str.substring(2,5)
      strb
    }


//    hiveContext.udf.register("aoto_number",()=>aoto_number())

//    val test = "select * from wanxiangdata.salesrecords_online_weekly limit 10"

//    val sql_jd_cat = "select a.category,a.brand,a.ec,a.sum_cat_jd/b.sum_all age_cat_jd from " +
//      " (select k.category,k.brand,k.ec,sum(k.volume) sum_cat_jd from wanxiangdata.salesrecords_online_weekly k where " +
//      " k.category = '空调' and k.ec in ('京东商城','天猫商城')  and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
//      " k.week in (select b.week from salesrecords_online_weekly b order by int(substring(b.week,0,2)) desc,int(substring(b.week,4,6)) desc limit 1) " +
//      " group by k.category,k.brand,k.ec) a " +
//      " left join " +
//      " (select m.category,m.ec,sum(m.volume) sum_all from wanxiangdata.salesrecords_online_weekly m where " +
//      " m.category = '空调' " +
//      " m.week in (select b.week from salesrecords_online_weekly b order by int(substring(b.week,0,2)) desc,int(substring(b.week,4,6)) desc limit 1) " +
//      " group by m.category,m.ec) b " +
//      " on a.category = b.category and a.ec = b.ec"

//    val date_now: Date = new Date()
    val week_benzhou = "select b.week from wanxiangdata.salesrecords_online_weekly b order by int(substring(b.week,0,2)) desc,int(substring(b.week,4,6)) desc limit 1"
    val str_benzhou: String = hiveContext.sql(week_benzhou).collect()(0)(0).toString()
    val str_last_benzhou: String = str_deal(str_benzhou)



//    val testc = "select k.category,k.brand,k.ec,sum(k.volume) sum_cat_jd from wanxiangdata.salesrecords_online_weekly k where " +
//      " k.category = '空调' and k.ec in ('京东商城','天猫商城')  and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')  " +
//      " and k.week in ('"+str_benzhou+"')  " +
//      " group by k.category,k.brand,k.ec"

    // 空调各品牌本周在京东\天猫的市占
    val sql_jd_cat_shizhan = "select aaa.category,aaa.brand,aaa.ec,aaa.shizhan_all,aaa.shizhan_all-bbb.shizhan_all tongbi from ( " +
      " select a.category,a.brand,a.ec,a.sum_cat_jd/b.sum_all shizhan_all from " +
      "(select k.category,k.brand,k.ec,sum(k.volume) sum_cat_jd from wanxiangdata.salesrecords_online_weekly k where " +
      "k.category = '空调' and k.ec in ('京东商城','天猫商城')  and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')  " +
      " and k.week in ('"+str_benzhou+"')  " +
      " group by k.category,k.brand,k.ec) a " +
      " left join  " +
      " (select m.category,m.ec,sum(m.volume) sum_all from wanxiangdata.salesrecords_online_weekly m where " +
      " m.category = '空调'   " +
      " and m.week in ('"+str_benzhou+"') group by m.category,m.ec) b " +
      " on a.category = b.category and a.ec = b.ec) aaa " +
      " join ( " +
      " select a.category,a.brand,a.ec,a.sum_cat_jd/b.sum_all shizhan_all from " +
      "(select k.category,k.brand,k.ec,sum(k.volume) sum_cat_jd from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.ec in ('京东商城','天猫商城')  and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')  " +
      " and k.week in ('"+str_last_benzhou+"')  " +
      " group by k.category,k.brand,k.ec) a " +
      " left join  " +
      " (select m.category,m.ec,sum(m.volume) sum_all from wanxiangdata.salesrecords_online_weekly m where " +
      " m.category = '空调'   " +
      " and m.week in ('"+str_last_benzhou+"')  group by m.category,m.ec) b " +
      " on a.category = b.category and a.ec = b.ec) bbb " +
      " where aaa.category = bbb.category and aaa.brand = bbb.brand and aaa.ec = bbb.ec"


    //空调各品牌在线上其他网站的市占
    val sql_up_other_shizhan = "select aaa.category,aaa.brand,aaa.ec,aaa.shizhan_all,aaa.shizhan_all-bbb.shizhan_all tongbi from ( " +
      " select bb.category,bb.brand,bb.ec,bb.other_all/ab.sum_all shizhan_all from ( " +
      " select aa.category,aa.brand, 'up_other' ec,sum(aa.sum_all) other_all from ( " +
      " select m.category,m.brand,m.ec,sum(m.volume) sum_all from wanxiangdata.salesrecords_online_weekly m where " +
      " m.category = '空调' and m.ec not in ('京东商城','天猫商城') and  m.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')" +
      " and m.week in ('"+str_benzhou+"') " +
      " group by m.category,m.brand,m.ec) aa group by aa.category,aa.brand) bb " +
      " left join  " +
      " (select xx.category, 'up_other' ec, sum(xx.sum_all) sum_all from ( " +
      " select m.category,m.ec,sum(m.volume) sum_all from wanxiangdata.salesrecords_online_weekly m where " +
      " m.category = '空调'" +
      " and m.week in ('"+str_benzhou+"')" +
      " group by m.category,m.ec) xx group by xx.category) ab " +
      " on bb.category = ab.category and bb.ec = ab.ec) aaa  " +
      " join ( " +
      " select bb.category,bb.brand,bb.ec,bb.other_all/ab.sum_all shizhan_all from ( " +
      " select aa.category,aa.brand, 'up_other' ec,sum(aa.sum_all) other_all from ( " +
      " select m.category,m.brand,m.ec,sum(m.volume) sum_all from wanxiangdata.salesrecords_online_weekly m where " +
      " m.category = '空调' and m.ec not in ('京东商城','天猫商城') and  m.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')" +
      " and m.week in ('"+str_last_benzhou+"')" +
      " group by m.category,m.brand,m.ec) aa group by aa.category,aa.brand) bb " +
      " left join " +
      " (select xx.category, 'up_other' ec, sum(xx.sum_all) sum_all from ( " +
      " select m.category,m.ec,sum(m.volume) sum_all from wanxiangdata.salesrecords_online_weekly m where " +
      " m.category = '空调' " +
      " and m.week in ('"+str_benzhou+"')" +
      " group by m.category,m.ec) xx group by xx.category) ab " +
      " on bb.category = ab.category and bb.ec = ab.ec) bbb " +
      " where aaa.category = bbb.category and aaa.brand = bbb.brand and aaa.ec = bbb.ec"

    //空调线上的市占
    val sql_up_shizhan = "select aaa.category,aaa.brand,aaa.ec,aaa.shizhan_all,aaa.shizhan_all-bbb.shizhan_all tongbi from ( " +
      " select kkk.category,kkk.brand,'up' ec,kkk.up_unit_sum/kkkk.up_sum shizhan_all from  " +
      " (select k.category,k.brand,sum(k.volume) up_unit_sum from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')" +
      " and k.week in ('"+str_benzhou+"') " +
      " group by k.category,k.brand) kkk " +
      " left join " +
      " (select kk.category,sum(kk.volume) up_sum from wanxiangdata.salesrecords_online_weekly kk where " +
      " kk.category = '空调'" +
      " and kk.week in ('"+str_benzhou+"')" +
      " group by kk.category) kkkk " +
      " on kkk.category = kkkk.category) aaa" +
      " join ( " +
      " select kkk.category,kkk.brand,'up' ec,kkk.up_unit_sum/kkkk.up_sum shizhan_all from " +
      " (select k.category,k.brand,sum(k.volume) up_unit_sum from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')" +
      " and k.week in ('"+str_last_benzhou+"')" +
      " group by k.category,k.brand) kkk " +
      " left join " +
      " (select kk.category,sum(kk.volume) up_sum from wanxiangdata.salesrecords_online_weekly kk where " +
      " kk.category = '空调'" +
      " and kk.week in ('"+str_last_benzhou+"')" +
      " group by kk.category) kkkk" +
      " on kkk.category = kkkk.category) bbb " +
      " where aaa.category = bbb.category and aaa.brand = bbb.brand and aaa.ec = bbb.ec"
    //空调线下的市占
    val sql_off_shizhan = "select aaa.category,aaa.brand,aaa.ec,aaa.shizhan_all,aaa.shizhan_all-bbb.shizhan_all tongbi from ( " +
      " select kkk.category,kkk.brand,'off' ec,kkk.up_unit_sum/kkkk.up_sum shizhan_all from  " +
      " (select k.category,k.brand,sum(k.volume) up_unit_sum from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
      " and k.week in ('"+str_benzhou+"') " +
      " group by k.category,k.brand) kkk " +
      " left join  " +
      " (select kk.category,sum(kk.volume) up_sum from wanxiangdata.salesrecords_offline_weekly kk where " +
      " kk.category = '空调' " +
      " and kk.week in ('"+str_benzhou+"') " +
      " group by kk.category) kkkk " +
      " on kkk.category = kkkk.category) aaa " +
      " join ( " +
      " select kkk.category,kkk.brand,'off' ec,kkk.up_unit_sum/kkkk.up_sum shizhan_all from " +
      " (select k.category,k.brand,sum(k.volume) up_unit_sum from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信')" +
      " and k.week in ('"+str_last_benzhou+"')" +
      " group by k.category,k.brand) kkk " +
      " left join  " +
      " (select kk.category,sum(kk.volume) up_sum from wanxiangdata.salesrecords_offline_weekly kk where  " +
      " kk.category = '空调'" +
      " and kk.week in ('"+str_last_benzhou+"')" +
      " group by kk.category) kkkk " +
      " on kkk.category = kkkk.category) bbb " +
      " where aaa.category = bbb.category and aaa.brand = bbb.brand and aaa.ec = bbb.ec "

    //空调整体市占
    var sql_all_shizhan = "select aaa.category,aaa.brand,aaa.ec,aaa.shizhan_all,aaa.shizhan_all-bbb.shizhan_all tongbi from ( " +
      " select e.category,e.brand,'all' ec,sum(e.off_unit_sum)/sum(e.off_sum) shizhan_all from  " +
      " (select kkk.category,kkk.brand,'off' ec,kkk.off_unit_sum,kkkk.off_sum from  " +
      " (select k.category,k.brand,sum(k.volume) off_unit_sum from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
      " and k.week in ('"+str_benzhou+"') " +
      " group by k.category,k.brand) kkk " +
      " left join  " +
      " (select kk.category,sum(kk.volume) off_sum from wanxiangdata.salesrecords_offline_weekly kk where  " +
      " kk.category = '空调' " +
      " and kk.week in ('"+str_benzhou+"') " +
      " group by kk.category) kkkk " +
      " on kkk.category = kkkk.category" +
      " UNION ALL " +
      " select kkk.category,kkk.brand,'on' ec,kkk.off_unit_sum,kkkk.off_sum from  " +
      " (select k.category,k.brand,sum(k.volume) off_unit_sum from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
      " and k.week in ('"+str_benzhou+"') " +
      " group by k.category,k.brand) kkk " +
      " left join " +
      " (select kk.category,sum(kk.volume) off_sum from wanxiangdata.salesrecords_online_weekly kk where  " +
      " kk.category = '空调'" +
      " and kk.week in ('"+str_benzhou+"') " +
      " group by kk.category) kkkk " +
      " on kkk.category = kkkk.category) e group by e.category,e.brand) aaa " +
      " join (  " +
      " select e.category,e.brand,'all' ec,sum(e.off_unit_sum)/sum(e.off_sum) shizhan_all from  " +
      " (select kkk.category,kkk.brand,'off' ec,kkk.off_unit_sum,kkkk.off_sum from  " +
      "(select k.category,k.brand,sum(k.volume) off_unit_sum from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
      " and k.week in ('"+str_last_benzhou+"') " +
      " group by k.category,k.brand) kkk " +
      " left join  " +
      " (select kk.category,sum(kk.volume) off_sum from wanxiangdata.salesrecords_offline_weekly kk where " +
      " kk.category = '空调' " +
      " and kk.week in ('"+str_last_benzhou+"') " +
      " group by kk.category) kkkk " +
      " on kkk.category = kkkk.category " +
      " UNION ALL " +
      " select kkk.category,kkk.brand,'on' ec,kkk.off_unit_sum,kkkk.off_sum from  " +
      " (select k.category,k.brand,sum(k.volume) off_unit_sum from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and  k.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
      " and k.week in ('"+str_last_benzhou+"') " +
      " group by k.category,k.brand) kkk " +
      " left join  " +
      " (select kk.category,sum(kk.volume) off_sum from wanxiangdata.salesrecords_online_weekly kk where " +
      " kk.category = '空调' " +
      " and kk.week in ('"+str_last_benzhou+"') " +
      " group by kk.category) kkkk " +
      " on kkk.category = kkkk.category) e group by e.category,e.brand) bbb " +
      " where aaa.category = bbb.category and aaa.brand = bbb.brand and aaa.ec = bbb.ec "

    //空调线上机型数据
    val sql_up_model = "select aaa.modelRanking,aaa.category,aaa.brand,aaa.model,aaa.sum_up_unit/bbb.sum_up model_shizhan,ccc.avg_amount,ddd.ranking from  " +
      " (select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from ( " +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('TCL') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('志高') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('奥克斯') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('格力') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('美的') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('海尔') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_online_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('海信') group by k.category,k.model,k.brand limit 10 ) x ) aaa " +
      " left join ( " +
      "  select q.category,sum(q.volume) sum_up from wanxiangdata.salesrecords_online_weekly q where " +
      " q.category = '空调' and q.week in ('"+str_benzhou+"') group by q.category) bbb " +
      " on aaa.category = bbb.category " +
      " left join (" +
      "  select m.category,m.brand,m.model,sum(m.amount)/sum(m.volume) avg_amount from wanxiangdata.salesrecords_online_weekly m where " +
      " m.category = '空调' and m.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
      " and m.week in ('"+str_benzhou+"')  " +
      " group by m.category,m.brand,m.model) ccc " +
      " on aaa.category = ccc.category and aaa.brand = ccc.brand and aaa.model = ccc.model " +
      " left join (" +
      "select ROW_NUMBER() OVER(ORDER BY sum(q.volume) DESC) ranking,q.category,q.model,sum(q.volume) sum_up from wanxiangdata.salesrecords_online_weekly q where " +
      " q.category = '空调' and q.week in ('16W17') group by q.category,q.model) ddd " +
      " on aaa.category = ddd.category and aaa.model = ddd.model "


    //空调线下机型数据
    val sql_off_model = "select aaa.modelRanking,aaa.category,aaa.brand,aaa.model,aaa.sum_up_unit/bbb.sum_up model_shizhan,ccc.avg_amount from  " +
      " (select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from ( " +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('TCL') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('志高') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('奥克斯') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('格力') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('美的') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('海尔') group by k.category,k.model,k.brand limit 10) x " +
      " UNION ALL " +
      " select x.modelRanking,x.category,x.brand,x.model,x.sum_up_unit from (" +
      " select ROW_NUMBER() OVER (ORDER BY sum(k.volume) DESC ) as modelRanking, k.category,k.brand,k.model,sum(k.volume) sum_up_unit from wanxiangdata.salesrecords_offline_weekly k where " +
      " k.category = '空调' and k.week in ('"+str_benzhou+"') and k.brand in ('海信') group by k.category,k.model,k.brand limit 10 ) x ) aaa " +
      " left join ( " +
      "  select q.category,sum(q.volume) sum_up from wanxiangdata.salesrecords_offline_weekly q where " +
      " q.category = '空调' and q.week in ('"+str_benzhou+"') group by q.category) bbb " +
      " on aaa.category = bbb.category " +
      " left join (" +
      "  select m.category,m.brand,m.model,sum(m.amount)/sum(m.volume) avg_amount from wanxiangdata.salesrecords_offline_weekly m where " +
      " m.category = '空调' and m.brand in ('志高','奥克斯','TCL','格力','美的','海尔','海信') " +
      " and m.week in ('"+str_benzhou+"')  " +
      " group by m.category,m.brand,m.model) ccc " +
      " on aaa.category = ccc.category and aaa.brand = ccc.brand and aaa.model = ccc.model "









    // 空调各品牌本周在京东\天猫的市占
//    val sql_jd_cat_shizhan_df = hiveContext.sql(sql_jd_cat_shizhan)
//    val sql_up_other_shizhan_df = hiveContext.sql(sql_up_other_shizhan)
//    val sql_up_shizhan_df = hiveContext.sql(sql_up_shizhan)
//    val sql_off_shizhan_df = hiveContext.sql(sql_off_shizhan)
//    val sql_all_shizhan_df = hiveContext.sql(sql_all_shizhan)
//
//    val sql_shizhan = sql_jd_cat_shizhan_df.unionAll(sql_up_other_shizhan_df).unionAll(sql_up_shizhan_df)
//      .unionAll(sql_off_shizhan_df).unionAll(sql_all_shizhan_df)
//    sql_shizhan.registerTempTable("shizhan")







//    val shizhan_result = hiveContext.sql(
//      "select 0 zzid," +
//        "t.category,t.brand,'0' dateType,'"+ str_benzhou +"' date, " +"t.ec line,'' channelType,'' model,0 ranking,0 modelRanking, "+
//    "  0 modelPrice, t.shizhan_all marketshare,t.tongbi marketshareMom," +
//       " from_unixtime(1404816601,'yyyy-MM-dd HH:mm:ss') writeTime,0 sales from shizhan t")






    val sql_up_model_df = hiveContext.sql(sql_up_model)
    sql_up_model_df.registerTempTable("up_model")
    val off_model_result = hiveContext.sql("select 0 zzid," +
      "t.category,t.brand,'0' dateType,'"+ str_benzhou +"' date, " +" 'up_model' line,'' channelType,t.model model,0 ranking,0 modelRanking, "+
      "  0 modelPrice, t.shizhan_all marketshare,t.tongbi marketshareMom," +
      " from_unixtime(1404816601,'yyyy-MM-dd HH:mm:ss') writeTime,0 sales from up_model t")





    sql_up_model_df.collect().foreach(println)


    val tableName = "marketshare_channel"
//    Hive_to_Mysql.hive_to_mysql(shizhan_result,tableName)

//    sql_jd_cat_shizhan_df.collect().foreach(println)



//    Hive_to_Mysql.hive_to_mysql(data,tableName)


    sc.stop()
  }
}
