package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val wordAndOne: RDD[(String, Int)] = sc.textFile("in/word.txt").flatMap(_.split(" ")).map((_,1))

    val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    val res: Array[Array[(String, Int)]] = wordAndOne.glom().collect()


//    res.fl

    res.foreach(x => println(x.toArray.toBuffer))
    //ArrayBuffer((Hello,1), (Scala,1), (Hello,1), (Spark,1), (Hello,1), (World,1))
    //ArrayBuffer((Hello,1), (Hadoop,1), (Hello,1), (Scala,1))
    sc.stop()


//    wordAndCount.partitionBy()
//     wordAndCount.join()
//    wordAndCount.reduceByKey()
//    sc.parallelize()
  }

}
