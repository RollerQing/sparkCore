package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransmitTest {
  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf = new SparkConf().setAppName("TransmitTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop","spark","hive","ss"))

    //3.创建一个Search对象
//    val search = new Search()

    //4.运用第一个过滤函数并打印结果
//    val match1: RDD[String] = search
  }

}
