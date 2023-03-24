package com.edward.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memory_Partition {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RDD")

    val sc = new SparkContext(conf)


    //获取默认配置 conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
    //           scheduler.conf.getInt("spark.default.parallelism", totalCores)
    val rdd : RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5),2)


    rdd.collect().foreach(println)

    sc.stop()
  }

}
