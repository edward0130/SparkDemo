package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Repartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Repartition")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    //重新分区，底层调用的方式是coalesce(numPartitions, shuffle = true)
    val repRdd = rdd.repartition(3)

    repRdd.glom().collect().foreach(data => println(data.mkString(",")))

    sc.stop()

  }

}
