//package com.weina.sparkcase.core
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//
//object Exam {
//  
//  def main(args: Array[String]) {
//    val conf = new SparkConf()
//        .setAppName("LocalFile") 
//        .setMaster("local");  
//    val sc = new SparkContext(conf)
//    
//    val f1 = sc.textFile("C://files//file1.txt").flatMap(l=>l.split(",")).map(word=>(word,1))
//    
//    val f2 = sc.textFile("C://files//file2.txt").flatMap(l=>l.split(",")).map(word=>(word,1))
//    
//    f1.join(f2).collect();
//  }
////  
//}