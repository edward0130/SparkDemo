package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MapPartition")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5),2)

    // 分区内的数据统一进行计算，返回的结果要求也是迭代器
    val mapRdd = rdd.mapPartitions(a => {
      println("------")
      a.map(_ * 2)
    })

    // 统计每个分区内最大的数据
    val maxRdd = mapRdd.mapPartitions(a => {
      List(a.max).iterator
    })

    mapRdd.collect().foreach(println)
    maxRdd.collect().foreach(println)

    sc.stop()
  }

}
