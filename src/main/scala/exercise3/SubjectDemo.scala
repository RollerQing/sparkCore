package exercise3

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
用户点击产生日志信息,有时间戳和对应URL
URL中会有不用学科,统计学科的访问量
需求:根据用访问数据进行统计用户对各个学科的各个模块的访问量Top3
思路:1.统计每个模块的访问量
2. 按照学科进行分组
3. 学科进行排序
4. 取top3
*/
object SubjectDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //获取数据并切分 生成元组
    val tupLogss: RDD[(String, Int)] = sc.textFile("D:\\Program Files\\feiq\\Recv Files\\sparkcoursesinfo\\spark\\data\\subjectaccess\\access.txt")
      .map(line => {
        val url = line.split("\t")(1)
        (url, 1)
      })
    //聚合，得到各学科的每个模块的访问量
    val aggred: RDD[(String, Int)] = tupLogss.reduceByKey(_+_)

    //生成便于学科分组的数据
    aggred.map(tup => {
      val url = tup._1
      val count = tup._2
      val subject = new URL(url).getHost
      (subject, (url, count))
    })

    //


  }

}
