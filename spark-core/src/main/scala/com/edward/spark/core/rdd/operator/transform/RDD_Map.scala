package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Map {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Map")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6))


    val mapRdd = rdd.map(_ * 2)

    mapRdd.collect().foreach(println)

    sc.stop()
  }

}
