package com.bluedon.kafka

import java.io.InputStream
import java.util.Properties

import com.bluedon.dataMatch._
import org.apache.spark.sql._
import org.apache.spark.streaming._


/**
  * Created by Administrator on 2016/12/14.
  */
object SyslogUserAnalyzeServer {

  def main(args: Array[String]) {

    try{
      val properties:Properties = new Properties();
      val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
      properties.load(ipstream);

      val url:String = properties.getProperty("aplication.sql.url")
      val username:String = properties.getProperty("aplication.sql.username")
      val password:String = properties.getProperty("aplication.sql.password")
      val redisHost:String = properties.getProperty("redis.host")

      val masterUrl = properties.getProperty("spark.master.url")
      val appName = properties.getProperty("spark.analyze.app.name")
      val spark = SparkSession
        .builder()
        .master(masterUrl)
        .appName(appName+"_sysloguser")
        .config("spark.some.config.option", "some-value")
        .config("spark.streaming.unpersist",true)  // 智能地去持久化
        .config("spark.streaming.stopGracefullyOnShutdown","true")  // 优雅的停止服务
        .config("spark.streaming.backpressure.enabled","true")      //开启后spark自动根据系统负载选择最优消费速率
        .config("spark.streaming.backpressure.initialRate",10000)      //限制第一次批处理应该消费的数据，因为程序冷启动队列里面有大量积压，防止第一次全部读取，造成系统阻塞
        .config("spark.streaming.kafka.maxRatePerPartition",3000)      //限制每秒每个消费线程读取每个kafka分区最大的数据量
        .config("spark.streaming.receiver.maxRate",10000)      //设置每次接收的最大数量
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")   //使用Kryo序列化
        /*
        .config("spark.cleaner.ttl",300000)   //Spark系统内部的元信息的超时时间
        .config("spark.network.timeout",300000)    //设置网络通信时间5分钟
        .config("spark.rpc.askTimeout",300000)    //设置网络通信时间5分钟
        .config("spark.executor.heartbeatInterval",300000)    //设置心跳通信时间5分钟
        */
        .getOrCreate()
      val sparkConf = spark.sparkContext
      //原始日志处理
      val ssc_syslog = new StreamingContext(sparkConf, Seconds(1))

      //日志处理
      val dataProcess = new DataProcess
      dataProcess.logUserProcess(spark,ssc_syslog,properties)
      ssc_syslog.start()
      ssc_syslog.awaitTermination()
    }catch {
      case e:Exception=>{
        e.printStackTrace()
      }
    }

  }

}
