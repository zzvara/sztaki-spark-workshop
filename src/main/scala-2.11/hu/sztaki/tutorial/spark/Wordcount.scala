package hu.sztaki.tutorial.spark

import org.apache.spark._

object Wordcount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[10]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    val counts =  sc
      .textFile(inputFile, 10)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 20)
      .sortBy(_._2, ascending = false)

    val result = counts.take(10)

    result foreach println

    Thread.sleep(54738926544l)
  }
}