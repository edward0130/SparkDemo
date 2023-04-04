package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Filter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Filter")
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6))

    //获取符合条件的过滤条件
    val filterRdd = rdd.filter(data => data % 2 == 0)

    filterRdd.collect().foreach(println)

    sc.stop()
  }

}
