package exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
 * 数据格式: timestamp   province   city	    userid	   adid
 *            时间点 	    省份	     城市	     用户     广告
 *
 * 用户ID范围:0-99
 * 省份,城市,ID相同:0-9
 * adid:0-19
 *
 *
 * //需求:统计每一个省份每一个小时的TOP3广告ID
 */
object Peo_Hour_AD_TopN {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val sc = new SparkContext(conf)

    val splited: RDD[(String, Int)] = sc.textFile("D:\\Program Files\\feiq\\Recv Files\\sparkcoursesinfo\\spark\\data\\advert\\Advert.log")
      .map(line => {
        val arr = line.split("\t")
        val timestamp = arr(0)
        val hour = new DateTime(timestamp.toLong).getHourOfDay.toString
        val province = arr(1)
        val adid = arr(4)

        ((hour + "_" + province + "_" + adid), 1)
      })

    //聚合计算点击次数
    val reduced: RDD[(String, Int)] = splited.reduceByKey(_+_)

    //整合数据粒度
    val pro_hour_adid_count: RDD[(String, (String, Int))] = reduced.map(tup => {
      val arr: Array[String] = tup._1.split("_")

      (arr(1) + "_" + arr(0), (arr(0), tup._2)) //(pro_hour,(adid,count))
    })
    //按照省份和小时分组
    val grouped: RDD[(String, Iterable[(String, Int)])] = pro_hour_adid_count.groupByKey()

    //组内降序排序 并取前3
    val top3: RDD[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortWith(_._2 > _._2).take(3))

    val res: RDD[(String, String, List[(String, Int)])] = top3.map(tup => {
      val array: Array[String] = tup._1.split("_")

      val pro = array(0)
      val hour = array(1)
      val adid_count = tup._2
      (pro, hour, adid_count)
    })
    println(res.collect.toBuffer)

  }
}
