package exercise

import org.apache.spark.{SparkConf, SparkContext}

//D:\Program Files\feiq\Recv Files\sparkcoursesinfo\spark\pvuv\access.log
object PVUVTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val sc = new SparkContext(conf)

    val logs = sc.textFile("D:\\Program Files\\feiq\\Recv Files\\sparkcoursesinfo\\spark\\pvuv\\access.log")

    val lines = logs.map(_.split(" ")(0))

    val pvRDD = lines.count()

    val uvRDD = lines.distinct.count()

    println(s"pv:$pvRDD")
    println(s"uv:$uvRDD")

    sc.stop()



  }
}
