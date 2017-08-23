package com.bluedon

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import redis.clients.jedis.Jedis

/**
  * Created by huoguang on 2017/8/14.
  */
object test1 extends App{

//    val host = s"172.16.10.70"
    //    val port = 6379
    //    val jedis = new Jedis(host, port)
    //    jedis.auth("123456")
    //    val regexSystem = jedis.get("logRegex")
    //    println(regexSystem)

  val spark: Builder = SparkSession
    .builder()
    .master("local")
    .appName("test")
  val sc = spark.getOrCreate().sparkContext
  val rdd = List(1,2,3,4)
  val res = sc.parallelize(rdd).aggregate((0,0))(
    (acc,value) => (acc._1 + value, acc._2 + 1),
    (acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )
  print(res)



}
