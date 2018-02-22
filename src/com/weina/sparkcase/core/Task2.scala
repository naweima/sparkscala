package com.weina.sparkcase.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Task2 {
    def main(args: Array[String]) {
    join()  
    }
    
    def join() {
    val conf = new SparkConf()
        .setAppName("join")  
        .setMaster("local")  
    val sc = new SparkContext(conf)
    
    val list1 = Array(
        Tuple2("A1", "KRISH"),
        Tuple2("A2", "kRISh"),
        Tuple2("A3", "MaDhU"),
        Tuple2("A4", "venkaT"));
    
    val list2 = Array(
        Tuple2("B1", "krish"),
        Tuple2("B2", "KRISH"),
        Tuple2("B3", "PrasAD"),
        Tuple2("B4", "VENKAT"));
    
    val listA = sc.parallelize(list1);
    val listB = sc.parallelize(list2);
    
    val upperA = listA.map(x=>(x._1,x._2.toUpperCase))
    val upperB = listB.map(x=>(x._1,x._2.toUpperCase))
    
    listA.foreach(println);
    listB.foreach(println);
    
    val reverseA = upperA.map({case(k,v) => (v,k)})
    val reverseB = upperB.map({case(k,v) => (v,k)})
    
    val results = reverseA.join(reverseB)
    results.foreach(println);
    
    results.foreach(result => { 
    println("name: " + result._1);
    println("id: " + result._2)
    println("=======================================")  
    }) 
    
    /*
 
    val upperA=listA.
    val upperB=listB.map(_.toUpperCase)
    
    val reverse = upperA.map ({case (k,v) => (v,k)})
    val reverse2 = upperA.map {case (x,y) => (y,x)}
    reverse2.take(5)
    val results = listA.join(listB)  
    
    results.foreach(result => { 
      println("student id: " + result._1);
      println("student name: " + result._2._1)
      println("student socre: " + result._2._2)  
      println("=======================================")  
    }) 
    
    */
  }
  
}


//
//package com.weina.sparkcase.core
//
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import scala.swing.Window
//import scala.swing.Window
//import scala.swing.Window
//import java.awt.Window
//
//object Task2 {
//    def main(args: Array[String]) {
//    join()  
//    }
//    
//    def join() {
//    val conf = new SparkConf()
//        .setAppName("join")  
//        .setMaster("local")  
//    val sc = new SparkContext(conf)
//    
//    val studentList = Array(
//        Tuple2(1, "leo"),
//        Tuple2(2, "jack"),
//        Tuple2(3, "tom"));
//    
//    val scoreList = Array(
//        Tuple2(1, 100),
//        Tuple2(2, 90),
//        Tuple2(3, 60));
//    
//    val students = sc.parallelize(studentList);
//    val scores = sc.parallelize(scoreList);
//    
//    val studentScores = students.join(scores)  
//    
//    studentScores.foreach(studentScore => { 
//      println("student id: " + studentScore._1);
//      println("student name: " + studentScore._2._1)
//      println("student socre: " + studentScore._2._2)  
//      println("=======================================")  
//    })  
//  }
//  
//}