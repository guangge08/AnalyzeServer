package com.bluedon.kafka
import java.util.{Date, Properties, UUID}

import com.bluedon.dataMatch._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{HashPartitioner, SparkConf}
import com.bluedon.initRule._
import com.bluedon.utils.{IpUtil, _}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import java.io.InputStream
import java.sql.Connection

import com.bluedon.asset.AssetAuto
import com.bluedon.esinterface.config.ESClient
import com.bluedon.listener.KafkaStreamListener
import net.sf.json.JSONArray
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.client.Client
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.kafka.clients.producer.KafkaProducer
import redis.clients.jedis.Jedis


/**
  * Created by Administrator on 2016/12/14.
  */
object AlarmSinalAnalyzeServer {

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
        .appName(appName+"_alarm_sinal")
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
      //获取规则
      val jedis:Jedis = new Jedis(redisHost,6379)
      jedis.auth("123456");
      val ruleUtils = new RuleUtils
      var allRuleMapBC:Map[String, JSONArray] = ruleUtils.getAllRuleMapByRedis(jedis,url,username,password)
      var dataCheckMapBC:Map[String, JSONArray] = ruleUtils.getCheckDataMapByRedis(jedis,url,username,password)
      //获取告警规则
      var alarmRuleMapBC:Map[String, JSONArray] = ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password)
      jedis.close()

      val ruleMap = Map[String,Map[String, JSONArray]]("allRuleMapBC" -> allRuleMapBC,"dataCheckMapBC" -> dataCheckMapBC,"alarmRuleMapBC" -> alarmRuleMapBC)
      var ruleMapBC:Broadcast[Map[String, Map[String, JSONArray]]] = spark.sparkContext.broadcast(ruleMap)
      var bcVarUtil:BCVarUtil = new BCVarUtil()
      val ruleVarBC = scala.collection.mutable.Map[String,Broadcast[Map[String, Map[String, JSONArray]]]] ("ruleBC"->ruleMapBC)
      bcVarUtil.setBCMap(ruleVarBC)
      val time = new Date()
      bcVarUtil.setRefreshTime(time.getTime)

      val ssc_alarm_sinal = new StreamingContext(sparkConf, Seconds(1))
      val kafkaStreamListener = new KafkaStreamListener()
      kafkaStreamListener.kafkaStreamListener(spark,bcVarUtil)
      //监听批处理
      ssc_alarm_sinal.addStreamingListener(kafkaStreamListener)

      //告警处理-单事件
      val dataProcess = new DataProcess
      dataProcess.alarmProcessForSinal(spark,ssc_alarm_sinal,properties,bcVarUtil)
      ssc_alarm_sinal.start()
      ssc_alarm_sinal.awaitTermination()
    }catch {
      case e:Exception=>{
        e.printStackTrace()
      }
    }

  }
}
