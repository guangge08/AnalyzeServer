package com.bluedon.dataMatch
import java.util
import java.util.{Properties, UUID}

import com.bluedon.asset.AssetAuto
import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import com.bluedon.utils._
import kafka.serializer.StringDecoder
import net.sf.json.{JSONArray, JSONObject}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.Consumer
import org.apache.phoenix.exception.PhoenixParserException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis

/**
  * Created by dengxiwen on 2017/5/10.
  */
class DataProcess {

  /**
    * 原始日志处理(内置规则)，处理后把范式化日志转发到kafka的genlog-topic通道
    *
    * @param spark
    * @param properties
    */
  def logProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties,bcVarUtil:BCVarUtil): Unit ={

    val ruleVarBC = bcVarUtil.getBCMap()
    var ruleBC = spark.sparkContext.broadcast(ruleVarBC("ruleBC"))

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

    //获取kafka日志流数据

    //val syslogsCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER_2)
    val brokers = properties.getProperty("kafka.host.list")

    val topics = Set(properties.getProperty("kafka.log.topic")).toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "sysloggp",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    print(s"================ Load config finish ================= ")

    val syslogsCache: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )
    print(s"================ get from kafka finish ================= ")
    /*
    //获取规则
    val jedis:Jedis = new Jedis(redisHost,6379)
    jedis.auth("123456");
    val ruleUtils = new RuleUtils
    var allRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAllRuleMapByRedis(jedis,url,username,password))
    var dataCheckMapBC = spark.sparkContext.broadcast(ruleUtils.getCheckDataMapByRedis(jedis,url,username,password))
    //获取告警规则
    var alarmRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password))
    jedis.close()
    */
    //日志范式化匹配与存储
    val genlogs = syslogsCache.mapPartitions(syslogsIt=>{

      val allRuleMap = ruleBC.value.value("allRuleMapBC")
      val dataCheckMap = ruleBC.value.value("dataCheckMapBC")
      val alarmRuleMap = ruleBC.value.value("alarmRuleMapBC")


      var syslogLists = List[String]()
      var genlogLists = List[String]()
      var eventLists = List[String]()
      try{
        val matchLog:LogMatch = new LogMatch
        val logRule: JSONArray = allRuleMap("logRule")
        //获取内置事件规则
        val eventMatch = new EventMatch
        val eventDefRuleBySystem:JSONArray = allRuleMap("eventDefRuleBySystem")
        //数据库连接
        val zkQuorum = zkQuorumURL.value
        val dbUtils = new DBUtils
        var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
        //var phoenixConn = DBPool.getJdbcConn()
        var client = ESClient.esClient()
        var stmt = phoenixConn.createStatement()
        var producer:KafkaProducer[String, String] = dbUtils.getKafkaProducer(kafkalistURL.value)
//
        while(syslogsIt.hasNext) {
          var log: ConsumerRecord[String, String] = syslogsIt.next()
            if(log != null && log.value() != null && !log.value().trim.equals("")){
              var syslogs = log.value()
              if(syslogs != null && !syslogs.trim.equals("") && syslogs.trim.split("~")!=null && syslogs.trim.split("~").length<=6){
                val syslogsId = UUID.randomUUID().toString().replaceAll("-", "")
                syslogs = syslogsId + "~" + syslogs
              }

            val mlogs: String = matchLog.matchLog(syslogs,logRule)
            if(mlogs != null && !mlogs.trim.equals("")){
              genlogLists = genlogLists.::(mlogs)
              dbUtils.sendKafkaList("genlog-topic",mlogs,producer)
              syslogs = syslogs + "~0"  //原日志匹配
            }else{
              syslogs = syslogs + "~1"  //原日志不匹配
            }
//            syslogs = syslogs + "~2"
            syslogLists = syslogLists.::(syslogs)
            //内置事件匹配处理

            val event = eventMatch.eventMatchBySystem(mlogs,eventDefRuleBySystem)
            eventLists = eventLists.::(event)
            if(event != null && !event.trim.equals("")){
              dbUtils.sendKafkaList("event-topic",event,producer)
              dbUtils.sendKafkaList("sinalevent-topic",event,producer)
            }

          }
        }

        if(syslogLists.size>0){
          //保存原始日志
          matchLog.batchSaveSysLog(syslogLists,stmt,client)

          if(genlogLists.size>0){
            //保存范式化日志
            matchLog.batchSaveMatchLog(genlogLists,stmt,client)
          }

          if(eventLists.size>0){
            //保存内置事件
            eventMatch.batchSaveSinalMatchEvent(eventLists,stmt,client)
          }

          stmt.executeBatch()
          phoenixConn.commit()
        }

        phoenixConn.close()
        //DBPool.releaseConn(phoenixConn)
        producer.flush()
        producer.close()
      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
      genlogLists.iterator
    })
    genlogs.print(1)
  }

  /**
    * 原始日志处理(自定义规则)，处理后把范式化日志转发到kafka的genlog-topic通道
    *
    * @param spark
    * @param properties
    */
  def logUserProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={

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

    //获取kafka日志流数据

    //val syslogsCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER_2)
    val brokers = properties.getProperty("kafka.host.list")

    val topics = Set("topic-k1").toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "syslogurgp",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )

    val syslogsCache = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )

    syslogsCache.print()
    /*
    //获取规则
    val jedis:Jedis = new Jedis(redisHost,6379)
    jedis.auth("123456");
    val ruleUtils = new RuleUtils
    var allRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAllRuleMapByRedis(jedis,url,username,password))
    var dataCheckMapBC = spark.sparkContext.broadcast(ruleUtils.getCheckDataMapByRedis(jedis,url,username,password))
    //获取告警规则
    var alarmRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password))
    jedis.close()
    //日志范式化匹配与存储
    val genlogs = syslogsCache.repartition(5).mapPartitions(syslogsIt=>{

      var allRuleMap = allRuleMapBC.value
      var dataCheckMap = dataCheckMapBC.value
      //获取告警规则
      var alarmRuleMap = alarmRuleMapBC.value

      var genlogLists = List[String]()
      try{
        val matchLog:LogMatch = new LogMatch
        val logRule = allRuleMap("logUserRule")
        //数据库连接
        val zkQuorum = zkQuorumURL.value
        val dbUtils = new DBUtils
        var client = ESClient.esClient()

        while(syslogsIt.hasNext) {
          var log = syslogsIt.next()
          if(log != null && log.value() != null && !log.value().trim.equals("")){
            val mlogs = matchLog.matchLogUser(log.value(),logRule)
            genlogLists = genlogLists.::(mlogs)
          }
        }

        if(genlogLists.size>0){
          matchLog.batchSaveMatchLogUser(genlogLists,client)

        }

      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
      genlogLists.iterator
    })

    genlogs.print(1)
    */

  }

  /**
    * 范式化日志处理，处理后把单事件转发到kafka的event-topic通道和sinalevent-topic通道
    *
    * @param spark
    * @param properties
    */
  def genlogProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties,bcVarUtil:BCVarUtil): Unit ={

    val ruleVarBC = bcVarUtil.getBCMap()
    var ruleBC = spark.sparkContext.broadcast(ruleVarBC("ruleBC"))

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


    val brokers = properties.getProperty("kafka.host.list")
    val topics = Set(properties.getProperty("kafka.genlog.topic")).toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "genloggp",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    //获取kafka日志流数据

    val genlogsCache = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )

    /*
    //获取规则
    val jedis:Jedis = new Jedis(redisHost,6379)
    jedis.auth("123456");
    val ruleUtils = new RuleUtils
    var allRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAllRuleMapByRedis(jedis,url,username,password))
    var dataCheckMapBC = spark.sparkContext.broadcast(ruleUtils.getCheckDataMapByRedis(jedis,url,username,password))
    //获取告警规则
    var alarmRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password))
    jedis.close()
    */
    //事件匹配（自定义）
    val eventByUsers = genlogsCache.repartition(250).mapPartitions(genlogsIt=>{

      val allRuleMap = ruleBC.value.value("allRuleMapBC")
      val dataCheckMap = ruleBC.value.value("dataCheckMapBC")
      val alarmRuleMap = ruleBC.value.value("alarmRuleMapBC")

      var eventLists = List[String]()
      try{
        val eventMatch = new EventMatch
        val eventField = allRuleMap("eventFieldRule")
        val eventDefRuleByUser:JSONArray = allRuleMap("eventDefRuleByUser")
        val dbUtils = new DBUtils
        val zkQuorum = zkQuorumURL.value
        var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
        //var phoenixConn = DBPool.getJdbcConn()
        var client = ESClient.esClient()
        var stmt = phoenixConn.createStatement()
        var producer:KafkaProducer[String, String] = dbUtils.getKafkaProducer(kafkalistURL.value)

        while(genlogsIt.hasNext) {
          var genlog = genlogsIt.next()
          if(genlog != null && genlog.value() != null && !genlog.value().trim.equals("")){
            val event = eventMatch.eventMatchByUser(genlog.value(),eventDefRuleByUser,eventField)
            eventLists = eventLists.::(event)
            if(event != null && !event.trim.equals("")){
              dbUtils.sendKafkaList("event-topic",event,producer)
              dbUtils.sendKafkaList("sinalevent-topic",event,producer)
            }
          }
        }
        if(eventLists.size>0){
          eventMatch.batchSaveSinalMatchEvent(eventLists,stmt,client)
          stmt.executeBatch()
          phoenixConn.commit()
        }

        phoenixConn.close()
        //DBPool.releaseConn(phoenixConn)
        producer.flush()
        producer.close()
      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
      eventLists.iterator
    })
    eventByUsers.print(1)
  }

  /**
    * 事件处理，处理后把联合事件转发到kafka的gevent-topic通道
    *
    * @param spark
    * @param properties
    */
  def eventProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties,bcVarUtil:BCVarUtil): Unit ={

    val ruleVarBC = bcVarUtil.getBCMap()
    var ruleBC = spark.sparkContext.broadcast(ruleVarBC("ruleBC"))

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

    val brokers = properties.getProperty("kafka.host.list")
    val topics = Set(properties.getProperty("kafka.event.topic")).toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "eventgp",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    //获取kafka日志流数据

    val sinalEventCache = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )

    /*
    //获取规则
    val jedis:Jedis = new Jedis(redisHost,6379)
    jedis.auth("123456");
    val ruleUtils = new RuleUtils
    var allRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAllRuleMapByRedis(jedis,url,username,password))
    var dataCheckMapBC = spark.sparkContext.broadcast(ruleUtils.getCheckDataMapByRedis(jedis,url,username,password))
    //获取告警规则
    var alarmRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password))
    jedis.close()
    */
    //联合事件匹配与存储
    val relationEvents = sinalEventCache.repartition(200).mapPartitions(eventIt=>{

      val jedis:Jedis = new Jedis(redisHost,6379)
      jedis.auth("123456");

      val allRuleMap = ruleBC.value.value("allRuleMapBC")
      val dataCheckMap = ruleBC.value.value("dataCheckMapBC")
      val alarmRuleMap = ruleBC.value.value("alarmRuleMapBC")

      var eventLists = List[String]()
      try{
        val zkQuorum = zkQuorumURL.value
        val relationEventMatch = new RelationEventMatch
        //数据库连接
        val dbUtils = new DBUtils
        var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
        //var phoenixConn = DBPool.getJdbcConn()
        var client = ESClient.esClient()
        var stmt = phoenixConn.createStatement()
        var producer:KafkaProducer[String, String] = dbUtils.getKafkaProducer(kafkalistURL.value)

        var tempgevents = List[String]()
        var tempgsevents = List[String]()
        var tempsqls = List[String]()

        while(eventIt.hasNext) {
          var event = eventIt.next()
          if(event != null && event.value() != null && !event.value().trim.equals("")){
            val relationEvents = relationEventMatch.relationEventMatch(event.value(),allRuleMap,dataCheckMap,phoenixConn,jedis)
            if(relationEvents._1 != null && !relationEvents._1.trim.equals("")){
              dbUtils.sendKafkaList("gevent-topic",relationEvents._1,producer)
            }
            eventLists = eventLists.::(relationEvents._1)
            val geventMatchs = relationEvents._2
            if(geventMatchs != null && geventMatchs.size>0){
              geventMatchs.foreach(gevent=>{
                tempgevents = tempgevents.::(gevent)
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
        }
        relationEventMatch.batchSaveRelationEvent(tempsqls,tempgevents,tempgsevents,stmt,client)
        stmt.executeBatch()
        phoenixConn.commit()
        phoenixConn.close()
        //DBPool.releaseConn(phoenixConn)
        producer.flush()
        producer.close()
      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
      jedis.close()
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
  def alarmProcessForSinal(spark:SparkSession,ssc:StreamingContext,properties:Properties,bcVarUtil:BCVarUtil): Unit ={

    val ruleVarBC = bcVarUtil.getBCMap()
    var ruleBC = spark.sparkContext.broadcast(ruleVarBC("ruleBC"))

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

    val brokers = properties.getProperty("kafka.host.list")
    val topics = Set(properties.getProperty("kafka.sinalevent.topic")).toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "alarmslgp",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    //获取kafka日志流数据

    val sinalEventCache = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )

    /*
    //获取规则
    val jedis:Jedis = new Jedis(redisHost,6379)
    jedis.auth("123456");
    val ruleUtils = new RuleUtils
    var allRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAllRuleMapByRedis(jedis,url,username,password))
    var dataCheckMapBC = spark.sparkContext.broadcast(ruleUtils.getCheckDataMapByRedis(jedis,url,username,password))
    //获取告警规则
    var alarmRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password))
    jedis.close()
    */
    //告警匹配(单事件)与存储
    val alarmEvents = sinalEventCache.repartition(100).mapPartitions(eventIt=>{

      val allRuleMap = ruleBC.value.value("allRuleMapBC")
      val dataCheckMap = ruleBC.value.value("dataCheckMapBC")
      val alarmRuleMap = ruleBC.value.value("alarmRuleMapBC")

      var alarmLists = List[String]()
      try{
        val zkQuorum = zkQuorumURL.value
        val dbUtils = new DBUtils
        val phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
        //var phoenixConn = DBPool.getJdbcConn()
        val alarmMatch = new AlarmMatch
        val alarmPolicyWhole = alarmRuleMap("alarmPolicyWholeRule")
        val alarmPolicyEvent = alarmRuleMap("alarmPolicyEventRule")
        var client = ESClient.esClient()
        var stmt = phoenixConn.createStatement()
        while(eventIt.hasNext){
          val event = eventIt.next()
          if(event != null && event.value() != null && !event.value().trim.equals("")){
            val alarmStr = alarmMatch.matchSinalEventAlarm(event.value(),alarmPolicyWhole,alarmPolicyEvent,phoenixConn,redisHostBD.value)
            alarmLists = alarmLists.::(alarmStr)
          }
        }
        if(alarmLists.size>0){
          alarmMatch.batchSaveMatchEventAlarm(alarmLists,stmt,client)
          stmt.executeBatch()
          phoenixConn.commit()
        }
        phoenixConn.close()
        //DBPool.releaseConn(phoenixConn)
      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
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
  def alarmProcessForGL(spark:SparkSession,ssc:StreamingContext,properties:Properties,bcVarUtil:BCVarUtil): Unit ={

    val ruleVarBC = bcVarUtil.getBCMap()
    var ruleBC = spark.sparkContext.broadcast(ruleVarBC("ruleBC"))

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


    val brokers = properties.getProperty("kafka.host.list")
    val topics = Set(properties.getProperty("kafka.gevent.topic")).toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "alarmglgp",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    //获取kafka日志流数据

    val sinalEventCache = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )

    /*
    //获取规则
    val jedis:Jedis = new Jedis(redisHost,6379)
    jedis.auth("123456");
    val ruleUtils = new RuleUtils
    var allRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAllRuleMapByRedis(jedis,url,username,password))
    var dataCheckMapBC = spark.sparkContext.broadcast(ruleUtils.getCheckDataMapByRedis(jedis,url,username,password))
    //获取告警规则
    var alarmRuleMapBC = spark.sparkContext.broadcast(ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password))
    jedis.close()
    */
    //告警匹配(单事件)与存储
    val alarmEvents = sinalEventCache.mapPartitions(eventIt=>{

      val allRuleMap = ruleBC.value.value("allRuleMapBC")
      val dataCheckMap = ruleBC.value.value("dataCheckMapBC")
      val alarmRuleMap = ruleBC.value.value("alarmRuleMapBC")

      var alarmLists = List[String]()
      try{
        val zkQuorum = zkQuorumURL.value
        val dbUtils = new DBUtils
        val phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
        //var phoenixConn = DBPool.getJdbcConn()
        val alarmMatch = new AlarmMatch
        val alarmPolicyWhole = alarmRuleMap("alarmPolicyWholeRule")
        val alarmPolicyEvent = alarmRuleMap("alarmPolicyRelationEventRule")
        var client = ESClient.esClient()
        var stmt = phoenixConn.createStatement()
        while(eventIt.hasNext){
          val event = eventIt.next()
          if(event != null && event.value() != null && !event.value().trim.equals("")){
            val alarmStr = alarmMatch.matchRelationEventAlarm(event.value(),alarmPolicyWhole,alarmPolicyEvent,phoenixConn,redisHostBD.value)
            alarmLists = alarmLists.::(alarmStr)
          }
        }
        if(alarmLists.size>0){
          alarmMatch.batchSaveMatchEventAlarm(alarmLists,stmt,client)
          stmt.executeBatch()
          phoenixConn.commit()
        }
        phoenixConn.close()
        //DBPool.releaseConn(phoenixConn)

      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
      alarmLists.iterator
    })
    alarmEvents.print(1)

  }
  def netflowProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={


    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")

/*    val kafkalist = properties.getProperty("kafka.host.list")
    val kafkalistURL = spark.sparkContext.broadcast(kafkalist)*/

    //获取kafka日志流数据

    //val syslogsCache = KafkaUtils.createStream(ssc, zkQuorumKafka, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER_2)
    val brokers = properties.getProperty("kafka.host.list")

    val topics = Set(properties.getProperty("kafka.entflow.topic")).toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "netflowgroup",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    val netflowCache: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )

    //
    val indexName:String = properties.getProperty("es.netflow.index.name")
    val netflow = netflowCache.mapPartitions(netflowline=>{
      //var jsonUtils:JSONtoLowerTools = new JSONtoLowerTools()
      val client = ESClient.esClient()
      var lists= new util.ArrayList[String]()
      var netflowLists = List[String]()
      val matchLog:LogMatch = new LogMatch
      try{
          while(netflowline.hasNext) {
            var flow: ConsumerRecord[String, String] = netflowline.next()
            if(flow != null && flow.value() != null && !flow.value().trim.equals("")){
              var flows = flow.value()
              val array = JSONArray.fromObject(flows)
              for(i <- 0 to  array.size()-1){
                val jsonobject= array.getJSONObject(i)
                jsonobject.put("recordid",UUID.randomUUID().toString.replace("-",""))
                val jsonObject: JSONObject= JSONtoLowerTools.transObject(jsonobject)
                lists.add(jsonObject.toString())
              }
            }
          }
        if(!lists.isEmpty){
          IndexUtils.batchIndexData(client, indexName, indexName, lists)
        }
      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
      netflowLists.iterator
    })
    netflow.print(1)
  }
  def flowProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties):Unit={


    val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
    val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)
    val zkQuorumKafka = properties.getProperty("zookeeper.kafka.host")+":" + properties.getProperty("zookeeper.port")


    val brokers = properties.getProperty("kafka.host.list")

    val topics = Set(properties.getProperty("kafka.flow.topic")).toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list"        -> brokers,
      "bootstrap.servers"           -> brokers,
      "group.id"                    -> "flowgroup",
      "auto.commit.interval.ms"     -> "1000",
      "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"           -> "latest",
      "enable.auto.commit"          -> "true"
    )
    val netflowCache: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String]( ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams) )


    val indexName:String = properties.getProperty("es.flow.index.name")
    val netflow = netflowCache.mapPartitions(netflowline=>{
      val client = ESClient.esClient()
      var lists= new util.ArrayList[String]()
      var netflowLists = List[String]()

      try{
        while(netflowline.hasNext) {
          var flow: ConsumerRecord[String, String] = netflowline.next()
          if(flow != null && flow.value() != null && !flow.value().trim.equals("")){
            var flows = flow.value()
            val flowChar = flows.split(" ")
            var str:String =""
            for(i <- 1 to flowChar.length-1){
                str+=flowChar(i)
            }
            if(!str.trim.equals("")){
              var obj:JSONObject =JSONObject.fromObject(str)
              obj.put("recrodid",UUID.randomUUID().toString.replace("-",""))
              val lowerObj = JSONtoLowerTools.transObject(obj)
              var strs =lowerObj.toString()
              IndexUtils.addIndexData(client,indexName,indexName,strs)
            }
          }
        }
      }catch {
        case e:PhoenixParserException=>{
          println("#####phoenix发生异常!")
          e.printStackTrace()
        }
        case e:Exception=>{
          e.printStackTrace()
        }
      }
      netflowLists.iterator
    })
    netflow.print(1)
  }
}
