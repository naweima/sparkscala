//package com.weina.sparkcase.core
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.SparkContext
////import org.apache.spark.sql.SparkSession
//import scala.util.Random
//
//object GroupByText {
//  def main(args: Array[String]): Unit = {
//     val sparkConf = new SparkConf().setAppName("SparkAccumulatorBroadcastJoin").setMaster("local[*]")
//  
////  def main(args: Array[String]) {
////   
////    val sparkConf =  new SparkConf().setAppName("GroupBy Test").setMaster("local")
//    
//    val sc = new SparkContext(sparkConf)
////    val sqlContext = new SQLContext(sc)
//    var numMappers = 2
//    var numKVPairs = 1000
//    var valSize =1000
//    var numReducers = numMappers
//    
//    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap{p => 
//    val ranGen = new Random
//    var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
////    for (i <- 0 until numKVPairs){
//    val byteArr = new Array[Byte](valSize)
//    ranGen.nextBytes(byteArr)
//      arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
//    }
//    arr1
//    }.cache()
//    pairs1.count()
//   }
//  
//}
//  
//  