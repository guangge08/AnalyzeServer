package com.bluedon.kafka

import java.io.InputStream
import java.util.{Date, Properties}

import com.bluedon.dataMatch.DataProcess
import com.bluedon.listener.KafkaStreamListener
import com.bluedon.utils.{BCVarUtil, RuleUtils}
import net.sf.json.JSONArray
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Author: Dlin
  * Date:2017/8/16 17:06
  * Descripe:上网行为数据入库
  */
object NetFlowServer {
  def main(args: Array[String]): Unit = {
    try{
      val properties:Properties = new Properties();
      val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
      properties.load(ipstream);

      val masterUrl = properties.getProperty("spark.master.url")
      val appName = properties.getProperty("spark.analyze.app.name")
      val spark = SparkSession
        .builder()
       .master(masterUrl)
       //.master("local[2]")
        .appName("AnalyzeServer_netflow")
        .config("spark.some.config.option", "some-value")
        .config("spark.streaming.unpersist",true)  // 智能地去持久化
        .config("spark.streaming.stopGracefullyOnShutdown","true")  // 优雅的停止服务
        .config("spark.streaming.backpressure.enabled","true")      //开启后spark自动根据系统负载选择最优消费速率
        .config("spark.streaming.backpressure.initialRate",10000)      //限制第一次批处理应该消费的数据，因为程序冷启动队列里面有大量积压，防止第一次全部读取，造成系统阻塞
        .config("spark.streaming.kafka.maxRatePerPartition",3000)      //限制每秒每个消费线程读取每个kafka分区最大的数据量
        .config("spark.streaming.receiver.maxRate",10000)      //设置每次接收的最大数量
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")   //使用Kryo序列化
        .getOrCreate()
      val sparkConf:SparkContext = spark.sparkContext
      //上网行为处理
      val ssc_netflow = new StreamingContext(sparkConf, Seconds(5))
      val dataProcess = new DataProcess
      dataProcess.netflowProcess(spark,ssc_netflow,properties)
      ssc_netflow.start()
      ssc_netflow.awaitTermination()
    }catch {
      case e:Exception=>{
        e.printStackTrace()
      }
    }
  }
}
