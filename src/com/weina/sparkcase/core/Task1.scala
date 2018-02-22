package com.weina.sparkcase.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Weina
 */
object Task1 {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("ParallelizeCollection")
        .setMaster("local")
    val sc = new SparkContext(conf)
    
    //Step 1. Prepare data
    
    //Step 1. Prepare data

val input="""user date      item1 item2
1    2015-12-01 14  5.6
1    2015-12-01 10  0.6
1    2015-12-02 8   9.4
1    2015-12-02 90  1.3
2    2015-12-01 30  0.3
2    2015-12-01 89  1.2
2    2015-12-30 70  1.9
2    2015-12-31 20  2.5
3    2015-12-01 19  9.3
3    2015-12-01 40  2.3
3    2015-12-02 13  1.4
3    2015-12-02 50  1.0
3    2015-12-02 19  7.8
"""
val inputLines=sc.parallelize(input.split("\\r?\\n"))
//filter the header row
val data=inputLines.filter(l=> !l.startsWith("user") )
data.foreach(println)

//Step 2. Find the latest date of each user

val keyByUser=data.map(line => { val a = line.split("\\s+"); ( a(0), line ) })
//For each user, find his latest date
val latestByUser = keyByUser.reduceByKey( (x,y) => if(x.split("\\s+")(1) > y.split("\\s+")(1)) x else y )
latestByUser.foreach(println)

//Step 3. Join the original data with the latest date to get the result

val latestKeyedByUserAndDate = latestByUser.map( x => (x._1 + ":"+x._2.split("\\s+")(1), x._2))
val originalKeyedByUserAndDate = data.map(line => { val a = line.split("\\s+"); ( a(0) +":"+a(1), line ) })
val result=latestKeyedByUserAndDate.join(originalKeyedByUserAndDate)
result.foreach(println)

//Step 4. Transform the result into the format you desire

def createCombiner(v:(String,String)):List[(String,String)] = List[(String,String)](v)
def mergeValue(acc:List[(String,String)], value:(String,String)) : List[(String,String)] = value :: acc
def mergeCombiners(acc1:List[(String,String)], acc2:List[(String,String)]) : List[(String,String)] = acc2 ::: acc1
//use combineByKey
val transformedResult=result.mapValues(l=> { val a=l._2.split(" +"); (a(2),a(3)) } ).combineByKey(createCombiner,mergeValue,mergeCombiners)
transformedResult.foreach(println)
        
    //    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    //    val numberRDD = sc.parallelize(numbers, 5)  
    //    val sum = numberRDD.reduce(_ + _)  
    //    
    //    println("1到10的累加和：" + sum)  
  }
  
}