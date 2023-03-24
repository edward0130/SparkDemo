package com.edward.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memory1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd : RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4, 5))

    rdd.collect().foreach(println)

    sc.stop()
  }

}
