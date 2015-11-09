package hu.sztaki.tutorial.spark

import breeze.linalg.{Vector, DenseVector, squaredDistance}

import org.apache.spark.{SparkConf, SparkContext}

object KMeans {
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  def generateData(): List[String] = {
    (1 to 1000).map( i =>
      (1 to 5).map(j => Math.random()).mkString(" ")
    ).toList
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: KMeans <k> <convergeDist>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(sparkConf)
    val lines = sc.parallelize(generateData())
    val K = args(0).toInt
    val convergeDist = args(1).toDouble
    var tempDist = 1.0

    /**
     * @todo Parse vectors.
     */
    // val data = _

    /**
     * @todo Set K points.
     */
    // val kPoints = _

    while(tempDist > convergeDist) {
      /**
       * @todo Implement iteration step.
       */

      println("Finished iteration (delta = " + tempDist + ")")
    }

    println("Final centers:")
    sc.stop()
  }
}