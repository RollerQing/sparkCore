package exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据格式: timestamp   province   city	    userid	   adid
 *            时间点 	    省份	     城市	     用户     广告
 *
 * 用户ID范围:0-99
 * 省份,城市,ID相同:0-9
 * adid:0-19
 *
 * 统计每个省份的每个广告点击量，并取每个省份点击量的topn
 *
 *
 */
object Peo_AD_TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val sc = new SparkContext(conf)

    //获取数据并切分
    val splitedLogs: RDD[((String, String), Int)] = sc.textFile("D:\\Program Files\\feiq\\Recv Files\\sparkcoursesinfo\\spark\\data\\advert\\Advert.log")
      .map(line => {
        val arr = line.split("\t")
        ((arr(1), arr(4)), 1) //返回省份和广告id
      })


      //按照省份和广告id进行分组
      //    val grouped: RDD[((String, String), Iterable[((String, String), Int)])] = splitedLogs.groupBy(_._1)
          val sumed: RDD[((String, String), Int)] = splitedLogs.reduceByKey(_+_)
//      val count: collection.Map[(String, String), Long] = splitedLogs.countByKey() 不能用 这个返回值不是RDD

    //整合数据粒度
    val pro_ad_count: RDD[(String, (String, Int))] = sumed.map(tup => {
      val province = tup._1._1 //省份
      val adid = tup._1._2 //广告id
      val adCount = tup._2 //广告点击量
      (province, (adid, adCount))
    })

    //按照省份分组
    val grouped: RDD[(String, Iterable[(String, Int)])] = pro_ad_count.groupByKey

    //组内降序排序
    val sorted: RDD[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortWith(_._2 > _._2))

    //取每个省份广告点击量的top3
//    println(sorted.collect.toBuffer)

    val taked: RDD[(String, List[(String, Int)])] = sorted.mapValues(_.take(3))

    println(taked.collect.toBuffer)

    sc.stop()
  }
}
