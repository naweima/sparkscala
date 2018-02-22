package com.weina.sparkcase.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object cases {
   
  def main(args: Array[String]) {
    first()
  }
    def first() {
    val conf = new SparkConf()
        .setAppName("map")
        .setMaster("local")  
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List("Spark is awesome","it is fun"))
    val fm = rdd.flatMap(str=>str.split(" "))
    fm.collect()
    
      
  }
   
}
  