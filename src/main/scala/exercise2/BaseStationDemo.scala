package exercise2

/**
 * 根据用户产生日志的信息,在那个基站停留时间最长
 * 19735E1C66.log  这个文件中存储着日志信息
 * 文件组成:手机号,时间戳,基站ID 连接状态(1连接 0断开)
 * lac_info.txt 这个文件中存储基站信息
 * 文件组成  基站ID, 经,纬度
 * 在一定时间范围内,求所用户经过的所有基站所停留时间最长的Top2
 * 思路:
 * 1.获取用户产生的日志信息并切分
 * 2.用户在基站停留的总时长
 * 3.获取基站的基础信息
 * 4.把经纬度的信息join到用户数据中
 * 5.求出用户在某些基站停留的时间top2
 *
 * longitude经度  long
 * latitude纬度  lat
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BaseStationDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val userInfo: RDD[((String, String), Long)] = sc.textFile("D:\\Program Files\\feiq\\Recv Files\\sparkcoursesinfo\\spark\\data\\lacduration\\log")
      .map(line => {
        val arr = line.split(",")
        val phone = arr(0) //用户手机号
        val time = arr(1).toLong //时间戳(数字)
        val lac = arr(2) //基站ID
        val eventType = arr(3)
        //时间类型(连接后断开)
        //连接时长需要进行一个区分,因为进入基站范围内有两种状态,这个状态决定时间的开始于结束
        val time_long = if (eventType.equals("1")) -time else time
        //元组 手机号和基站作为key  时间作为value
        ((phone, lac), time_long)
      })
    userInfo

  }
}
