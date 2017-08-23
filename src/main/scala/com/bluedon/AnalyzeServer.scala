package com.bluedon
import java.util.UUID

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
import java.util.Properties
import java.io.InputStream
import java.sql.Connection

import com.bluedon.asset.AssetAuto
import com.bluedon.esinterface.config.ESClient
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.client.Client
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.kafka.clients.producer.KafkaProducer


/**
  * Created by Administrator on 2016/12/14.
  */
object AnalyzeServer {

  def main(args: Array[String]) {
    /*
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
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sparkConf = spark.sparkContext
    //原始日志处理
    new Thread {

      val ssc_syslog = new StreamingContext(sparkConf, Seconds(1))
      //日志处理
      logProcess(spark,ssc_syslog,properties)
      //流量处理暂时不作处理2017-4-11
      //flowProcess(spark,ssc,properties)
      ssc_syslog.start()
      ssc_syslog.awaitTermination()
    }.start()

    //范式化日志处理
    new Thread {
      val ssc_genlog = new StreamingContext(sparkConf, Seconds(1))
      //范式化日志处理
      genlogProcess(spark,ssc_genlog,properties)
      ssc_genlog.start()
      ssc_genlog.awaitTermination()
    }.start()

    //事件处理
    new Thread {
      val ssc_event = new StreamingContext(sparkConf, Seconds(1))
      //事件处理
      eventProcess(spark,ssc_event,properties)
      ssc_event.start()
      ssc_event.awaitTermination()
    }.start()

    //告警处理-单事件
    new Thread {
      val ssc_alarm_sinal = new StreamingContext(sparkConf, Seconds(1))
      //告警处理-单事件
      alarmProcessForSinal(spark,ssc_alarm_sinal,properties)
      ssc_alarm_sinal.start()
      ssc_alarm_sinal.awaitTermination()
    }.start()

    //告警处理-单事件
    new Thread {
      val ssc_alarm_gl = new StreamingContext(sparkConf, Seconds(1))
      //告警处理-联合事件
      alarmProcessForGL(spark,ssc_alarm_gl,properties)
      ssc_alarm_gl.start()
      ssc_alarm_gl.awaitTermination()
    }.start()
    */
  }

  /**
    * 原始日志处理，处理后把范式化日志转发到kafka的genlog-topic通道
    *
    * @param spark
    * @param properties
    */
  /*
  def logProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")
    val redisHostBD = spark.sparkContext.broadcast(redisHost)

    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")

    val kafkalist = properties.getProperty("kafka.host.list")
    val kafkalistURL = spark.sparkContext.broadcast(kafkalist)

    val numThreads = 1
    val group = "1"
    val topics = properties.getProperty("kafka.log.topic")

    //获取规则
    val ruleUtils = new RuleUtils
    var allRuleMap = ruleUtils.getAllRuleMap(spark,url,username,password)
    var dataCheckMap = ruleUtils.getCheckDataMap(spark,url,username,password)
    //获取告警规则
    var alarmRuleMap = ruleUtils.getAlarmRuleMap(spark,url,username,password)

    //val accumulator= spark.sparkContext.accumulator(0,"broadcasttest")

    //ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    println("#############################"+zkQuorumRoot + "###########" + topicMap.toString())
    //regexRule.regexMatch(x)
    //获取kafka日志流数据

    val syslogsCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    //日志范式化匹配与存储
    val genlogs = syslogsCache.repartition(20).mapPartitions(syslogsIt=>{
      var genlogs = List[String]()
      val matchLog:LogMatch = new LogMatch
      val logRule = allRuleMap.value("logRule")
      //数据库连接
      val zkQuorum = zkQuorumURL.value
      val dbUtils = new DBUtils
      var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
      var client = ESClient.esClient()
      var stmt = phoenixConn.createStatement()
      var producer:KafkaProducer[String, String] = dbUtils.getKafkaProducer(kafkalistURL.value)

      while(syslogsIt.hasNext) {
        var log = syslogsIt.next()
        val mlogs = matchLog.matchLog(log._2,logRule)
        genlogs = genlogs.::(mlogs)
        dbUtils.sendKafkaList("genlog-topic",mlogs,producer)
      }

      matchLog.batchSaveMatchLog(genlogs,stmt,client)
      stmt.executeBatch()
      phoenixConn.commit()
      //producer.flush()
      producer.close()

      genlogs.iterator
    })
    genlogs.print(1)

  }

  /**
    * 范式化日志处理，处理后把单事件转发到kafka的event-topic通道和allevent-topic通道
    *
    * @param spark
    * @param properties
    */
  def genlogProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")
    val redisHostBD = spark.sparkContext.broadcast(redisHost)

    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")

    val kafkalist = properties.getProperty("kafka.host.list")
    val kafkalistURL = spark.sparkContext.broadcast(kafkalist)

    val numThreads = 1
    val group = "1"
    val topics = properties.getProperty("kafka.genlog.topic")

    //获取规则
    val ruleUtils = new RuleUtils
    var allRuleMap = ruleUtils.getAllRuleMap(spark,url,username,password)
    var dataCheckMap = ruleUtils.getCheckDataMap(spark,url,username,password)
    //获取告警规则
    var alarmRuleMap = ruleUtils.getAlarmRuleMap(spark,url,username,password)

    //val accumulator= spark.sparkContext.accumulator(0,"broadcasttest")

    //ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //regexRule.regexMatch(x)
    //获取kafka日志流数据

    val genlogsCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    //事件匹配（内置）
    val eventBySystems = genlogsCache.repartition(20).mapPartitions(genlogsIt=>{
      var eventLists = List[String]()
      val eventMatch = new EventMatch
      val eventDefRuleBySystem:Array[Row] = allRuleMap.value("eventDefRuleBySystem")
      val dbUtils = new DBUtils
      val zkQuorum = zkQuorumURL.value
      var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
      var client = ESClient.esClient()
      var stmt = phoenixConn.createStatement()
      var producer:KafkaProducer[String, String] = dbUtils.getKafkaProducer(kafkalistURL.value)

      while(genlogsIt.hasNext) {
        var genlog = genlogsIt.next()
        val event = eventMatch.eventMatchBySystem(genlog._2,eventDefRuleBySystem)
        eventLists = eventLists.::(event)
        dbUtils.sendKafkaList("event-topic",event,producer)
        dbUtils.sendKafkaList("sinalevent-topic",event,producer)
      }
      eventMatch.batchSaveSinalMatchEvent(eventLists,stmt,client)
      stmt.executeBatch()
      phoenixConn.commit()
      //producer.flush()
      producer.close()

      eventLists.iterator
    })
    //缓存事件匹配（内置）
    eventBySystems.print(1)
    //事件匹配（自定义）
    val eventByUsers = genlogsCache.repartition(20).mapPartitions(genlogsIt=>{
      var eventLists = List[String]()
      val eventMatch = new EventMatch
      val eventField = allRuleMap.value("eventFieldRule")
      val eventDefRuleByUser:Array[Row] = allRuleMap.value("eventDefRuleByUser")
      val dbUtils = new DBUtils
      val zkQuorum = zkQuorumURL.value
      var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
      var client = ESClient.esClient()
      var stmt = phoenixConn.createStatement()
      var producer:KafkaProducer[String, String] = dbUtils.getKafkaProducer(kafkalistURL.value)

      while(genlogsIt.hasNext) {
        var genlog = genlogsIt.next()
        val event = eventMatch.eventMatchByUser(genlog._2,eventDefRuleByUser,eventField)
        eventLists = eventLists.::(event)
        dbUtils.sendKafkaList("event-topic",event,producer)
        dbUtils.sendKafkaList("sinalevent-topic",event,producer)
      }
      eventMatch.batchSaveSinalMatchEvent(eventLists,stmt,client)
      stmt.executeBatch()
      phoenixConn.commit()
      //producer.flush()
      producer.close()

      eventLists.iterator
    })
    eventByUsers.print(1)
  }

  /**
    * 事件处理，处理后把联合事件转发到kafka的allevent-topic通道
    *
    * @param spark
    * @param properties
    */
  def eventProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")
    val redisHostBD = spark.sparkContext.broadcast(redisHost)

    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")

    val kafkalist = properties.getProperty("kafka.host.list")
    val kafkalistURL = spark.sparkContext.broadcast(kafkalist)

    val numThreads = 1
    val group = "1"
    val topics = properties.getProperty("kafka.event.topic")

    //获取规则
    val ruleUtils = new RuleUtils
    var allRuleMap = ruleUtils.getAllRuleMap(spark,url,username,password)
    var dataCheckMap = ruleUtils.getCheckDataMap(spark,url,username,password)
    //获取告警规则
    var alarmRuleMap = ruleUtils.getAlarmRuleMap(spark,url,username,password)

    //val accumulator= spark.sparkContext.accumulator(0,"broadcasttest")

    //ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    println("#############################"+zkQuorumRoot + "###########" + topicMap.toString())
    //regexRule.regexMatch(x)
    //获取kafka日志流数据

    val sinalEventCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    //联合事件匹配与存储
    val relationEvents = sinalEventCache.repartition(5).mapPartitions(eventIt=>{
      var eventLists = List[String]()
      val zkQuorum = zkQuorumURL.value
      val relationEventMatch = new RelationEventMatch
      //数据库连接
      val dbUtils = new DBUtils
      var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
      var client = ESClient.esClient()
      var stmt = phoenixConn.createStatement()
      var producer:KafkaProducer[String, String] = dbUtils.getKafkaProducer(kafkalistURL.value)

      var tempgevents = List[String]()
      var tempgsevents = List[String]()
      var tempsqls = List[String]()

      while(eventIt.hasNext) {
        var event = eventIt.next()
        val relationEvents = relationEventMatch.relationEventMatch(event._2,allRuleMap.value,dataCheckMap.value,phoenixConn,redisHostBD.value)
        eventLists = eventLists.::(relationEvents._1)
        val geventMatchs = relationEvents._2
        if(geventMatchs != null && geventMatchs.size>0){
          geventMatchs.foreach(gevent=>{
            tempgevents = tempgevents.::(gevent)
            dbUtils.sendKafkaList("gevent-topic",gevent,producer)
          })
        }
        val gseventMatchs = relationEvents._3
        if(gseventMatchs != null && gseventMatchs.size>0){
          gseventMatchs.foreach(gsevent=>{
            tempgsevents = tempgsevents.::(gsevent)
          })
        }
        val sqlMatchs = relationEvents._4
        if(sqlMatchs != null && sqlMatchs.size>0){
          sqlMatchs.foreach(sql=>{
            tempsqls = tempsqls.::(sql)
          })
        }
      }
      relationEventMatch.batchSaveRelationEvent(tempsqls,tempgevents,tempgsevents,stmt,client)
      stmt.executeBatch()
      phoenixConn.commit()
      //producer.flush()
      producer.close()
      eventLists.iterator
    })

    relationEvents.print(1)

  }

  /**
    * 告警处理-单事件
    *
    * @param spark
    * @param properties
    */
  def alarmProcessForSinal(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")
    val redisHostBD = spark.sparkContext.broadcast(redisHost)

    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")

    val kafkalist = properties.getProperty("kafka.host.list")
    val kafkalistURL = spark.sparkContext.broadcast(kafkalist)

    val numThreads = 1
    val group = "1"
    val topics = properties.getProperty("kafka.sinalevent.topic")

    //获取规则
    val ruleUtils = new RuleUtils
    var allRuleMap = ruleUtils.getAllRuleMap(spark,url,username,password)
    var dataCheckMap = ruleUtils.getCheckDataMap(spark,url,username,password)
    //获取告警规则
    var alarmRuleMap = ruleUtils.getAlarmRuleMap(spark,url,username,password)

    //val accumulator= spark.sparkContext.accumulator(0,"broadcasttest")

    //ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    println("#############################"+zkQuorumRoot + "###########" + topicMap.toString())
    //regexRule.regexMatch(x)
    //获取kafka日志流数据

    val sinalEventCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    //告警匹配(单事件)与存储
    val alarmEvents = sinalEventCache.repartition(5).mapPartitions(eventIt=>{
      var alarmLists = List[String]()
      val zkQuorum = zkQuorumURL.value
      val dbUtils = new DBUtils
      val phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
      val alarmMatch = new AlarmMatch
      val alarmPolicyWhole = alarmRuleMap.value("alarmPolicyWholeRule")
      val alarmPolicyEvent = alarmRuleMap.value("alarmPolicyEventRule")
      var client = ESClient.esClient()
      var stmt = phoenixConn.createStatement()
      while(eventIt.hasNext){
        val event = eventIt.next()
        val alarmStr = alarmMatch.matchSinalEventAlarm(event._2,alarmPolicyWhole,alarmPolicyEvent,phoenixConn,redisHostBD.value)
        alarmLists = alarmLists.::(alarmStr)
      }
      alarmMatch.batchSaveMatchEventAlarm(alarmLists,stmt,client)
      stmt.executeBatch()
      phoenixConn.commit()
      alarmLists.iterator
    })
    alarmEvents.print(1)

  }

  /**
    * 告警处理-联合事件
    *
    * @param spark
    * @param properties
    */
  def alarmProcessForGL(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")
    val redisHostBD = spark.sparkContext.broadcast(redisHost)

    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")

    val kafkalist = properties.getProperty("kafka.host.list")
    val kafkalistURL = spark.sparkContext.broadcast(kafkalist)

    val numThreads = 1
    val group = "1"
    val topics = properties.getProperty("kafka.gevent.topic")

    //获取规则
    val ruleUtils = new RuleUtils
    var allRuleMap = ruleUtils.getAllRuleMap(spark,url,username,password)
    var dataCheckMap = ruleUtils.getCheckDataMap(spark,url,username,password)
    //获取告警规则
    var alarmRuleMap = ruleUtils.getAlarmRuleMap(spark,url,username,password)

    //val accumulator= spark.sparkContext.accumulator(0,"broadcasttest")

    //ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    println("#############################"+zkQuorumRoot + "###########" + topicMap.toString())
    //regexRule.regexMatch(x)
    //获取kafka日志流数据

    val sinalEventCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    //告警匹配(单事件)与存储
    val alarmEvents = sinalEventCache.repartition(5).mapPartitions(eventIt=>{
      var alarmLists = List[String]()
      val zkQuorum = zkQuorumURL.value
      val dbUtils = new DBUtils
      val phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
      val alarmMatch = new AlarmMatch
      val alarmPolicyWhole = alarmRuleMap.value("alarmPolicyWholeRule")
      val alarmPolicyEvent = alarmRuleMap.value("alarmPolicyEventRule")
      var client = ESClient.esClient()
      var stmt = phoenixConn.createStatement()
      while(eventIt.hasNext){
        val event = eventIt.next()
        val alarmStr = alarmMatch.matchSinalEventAlarm(event._2,alarmPolicyWhole,alarmPolicyEvent,phoenixConn,redisHostBD.value)
        alarmLists = alarmLists.::(alarmStr)
      }
      alarmMatch.batchSaveMatchEventAlarm(alarmLists,stmt,client)
      stmt.executeBatch()
      phoenixConn.commit()
      alarmLists.iterator
    })
    alarmEvents.print(1)

  }

  */
}
