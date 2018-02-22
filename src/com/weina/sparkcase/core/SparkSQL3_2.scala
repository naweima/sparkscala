//package com.weina.sparkcase.core
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
//
//class SparkSQL3_2 {
//  
//  case class Record(key:Int, Value: String)
//  val sparkConf = new SparkConf().setAppName("RDDRelation")
//  val sc = new SparkContext(sparkConf)
//  val sqlContext = new SQLContext(sc)
//  val rdd = sc.parallelize(1 to 100).map(i => Record(i,s"val_$i"))
//  rdd.re
//  
//}