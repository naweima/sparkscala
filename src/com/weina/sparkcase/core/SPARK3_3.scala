package com.weina.sparkcase.core



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Administrator
 */
object SPARK3_3  {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("SPARK3_3")
        .setMaster("local")
    val sc = new SparkContext(conf)
    
    val rdd = sc.parallelize(List("Spark is awesome","it is fun"))
    val fm = rdd.flatMap(str=>str.split(" "))
    val word1 = fm.map(word=>(word,1))
    val wrdCnt = word1.reduceByKey(_+_)

    wrdCnt.collect()
    }
  
}


