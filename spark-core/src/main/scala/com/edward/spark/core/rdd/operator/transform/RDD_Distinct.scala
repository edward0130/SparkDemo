package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Distinct {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Repartition")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 2, 3), 3)

    //map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    val disRdd = rdd.distinct()

    disRdd.collect().foreach(println)

    sc.stop()

  }

}
