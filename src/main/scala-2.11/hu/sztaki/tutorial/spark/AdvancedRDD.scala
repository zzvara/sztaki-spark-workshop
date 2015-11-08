package hu.sztaki.tutorial.spark

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object AdvancedRDD {
  implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {
    /**
     * @todo Encapsulate the logic of wordcount.
     */
    def countEachElement = ???

    def countWhere(f: T => Boolean): Long = {
      rdd.filter(f).count()
    }

    def sortByDesc[K : Ordering: ClassTag](f: T => K): RDD[T] = {
      val isAscending = false
      rdd.sortBy(f, isAscending)
    }

    def explode[U](f: T => TraversableOnce[U]): RDD[(U, T)] = {
      rdd
        .map(element => (f(element), element))
        .flatMap(a => a._1.map(b => (b, a._2)))
    }
  }
}