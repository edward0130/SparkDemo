package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Glom")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    //每个分区的数据合并成数组
    val glomRdd = rdd.glom()

    glomRdd.collect().foreach(data=>println(data.mkString(",")))

    sc.stop()
  }

}
