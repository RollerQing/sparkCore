package practice

import org.apache.spark.{SparkConf, SparkContext}

object WordCount_test_10_14 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val sc = new SparkContext(config)

    val lines = sc.textFile("in")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))

    val wordToSum = wordToOne.reduceByKey(_ + _)

    val result = wordToSum.collect()

    result.foreach(println)


  }
}
