package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1WordCount {
  def main(args: Array[String]): Unit = {

    //local 模式
    //创建sparkconf对象
    //设定Spark计算框架的运行（部署）环境
    //app id
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //读取文件 将文件内容一行一行读取出来
    val lines: RDD[String] = sc.textFile("in")

    //将一行一行的数据分解一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //为了方便统计 将单词数据进行数据结构的转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    //对转换结构后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    //将统计结果采集后打印到控制台
    val result: Array[(String, Int)] = wordToSum.collect()

    result.foreach(println)

  }
}
