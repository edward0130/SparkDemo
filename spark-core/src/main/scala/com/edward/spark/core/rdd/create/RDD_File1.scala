package com.edward.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_File1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd : RDD[(String,String)] = sc.wholeTextFiles("datas/data1.txt")

    rdd.collect().foreach(println)

    sc.stop()
  }

}
