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
      .textFile(inputFile)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
  }
}