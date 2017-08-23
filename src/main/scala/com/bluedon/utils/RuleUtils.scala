package com.bluedon.utils
import com.bluedon.initRule._
import net.sf.json.JSONArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import redis.clients.jedis.Jedis
/**
  * Created by dengxiwen on 2017/1/23.
  */
class RuleUtils {

  /**
    * 获取联合事件规则
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getAllRuleMap(spark:SparkSession, url:String, username:String, password:String):Broadcast[ Map[String, Array[Row]]] ={

    val regexRule = new LogRegexRule
    val logDF = regexRule.getLogRegex(spark,url,username,password).collect()
    println("#############范式规则==="+logDF.length)
    //设置广播变量，可以在所有的节点访问
    //日志范式化规则
    val logRule = spark.sparkContext.broadcast(logDF)
    //事件规则
    val eventRegexRule = new EventRegexRule
    val eventDefDFByUser = eventRegexRule.getEventDefByUser(spark,url,username,password).collect()
    println("#############事件规则（自定义）==="+eventDefDFByUser.length)
    val eventDefDFBySystem = eventRegexRule.getEventDefBySystem(spark,url,username,password).collect()
    println("#############事件规则（内置）==="+eventDefDFBySystem.length)
    //广播变量--事件定义
    val eventDefRuleByUser = spark.sparkContext.broadcast(eventDefDFByUser)
    val eventDefRuleBySystem = spark.sparkContext.broadcast(eventDefDFBySystem)
    val eventFieldDF = eventRegexRule.getEventRule(spark,url,username,password).collect()
    //广播变量--事件规则
    val eventFieldRule = spark.sparkContext.broadcast(eventFieldDF)
    //联合事件规则
    val relationEventRegexRule = new RelationEventRegexRule
    val eventmedefineDF = relationEventRegexRule.getRelationEventMedefine(spark,url,username,password).collect()
    val eventmedefinesubDF = relationEventRegexRule.getRelationEventMedefineSub(spark,url,username,password).collect()
    val eventmedetailDF = relationEventRegexRule.getRelationEventMedetail(spark,url,username,password).collect()
    val leakmedetailDF = relationEventRegexRule.getRelationLeakMedetail(spark,url,username,password).collect()
    val netflowmedetailDF = relationEventRegexRule.getRelationNetflowMedetail(spark,url,username,password).collect()
    val nemedetailDF = relationEventRegexRule.getRelationNeMedetail(spark,url,username,password).collect()
    val intelligencemedetailDF = relationEventRegexRule.getRelationIntelligenceMedetail(spark,url,username,password).collect()
    val eventmedefineRule = spark.sparkContext.broadcast(eventmedefineDF)
    val eventmedefinesubRule = spark.sparkContext.broadcast(eventmedefinesubDF)
    val eventmedetailRule = spark.sparkContext.broadcast(eventmedetailDF)
    val leakmedetailRule = spark.sparkContext.broadcast(leakmedetailDF)
    val netflowMedetailRule = spark.sparkContext.broadcast(netflowmedetailDF)
    val neMedetailRule = spark.sparkContext.broadcast(nemedetailDF)
    val intelligenceMedetailRule = spark.sparkContext.broadcast(intelligencemedetailDF)
    var allRuleDF = Map[String,Array[Row]]()

    allRuleDF += ("logRule" -> logRule.value)
    allRuleDF += ("eventDefRuleByUser" -> eventDefRuleByUser.value)
    allRuleDF += ("eventDefRuleBySystem" -> eventDefRuleBySystem.value)
    allRuleDF += ("eventFieldRule" -> eventFieldRule.value)

    allRuleDF += ("eventmedefine" -> eventmedefineRule.value)
    allRuleDF += ("eventmedefinesub" -> eventmedefinesubRule.value)
    allRuleDF += ("eventmedetail" -> eventmedetailRule.value)
    allRuleDF += ("leakmedetail" -> leakmedetailRule.value)
    allRuleDF += ("netflowmedetail" -> netflowMedetailRule.value)
    allRuleDF += ("nemedetail" -> neMedetailRule.value)
    allRuleDF += ("intelligencemedetail" -> intelligenceMedetailRule.value)
    val allRuleMap = spark.sparkContext.broadcast(allRuleDF)

    allRuleMap
  }

  /**
    * 获取比对数据，比如漏洞数据比对
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getCheckDataMap(spark:SparkSession, url:String, username:String, password:String):Broadcast[ Map[String, Array[Row]]] ={
    val dataCheck = new DataCheck
    val leakscansDF = dataCheck.getLeakScans(spark,url,username,password).collect()
    val leakscans = spark.sparkContext.broadcast(leakscansDF)
    var dataMapDF = Map[String,Array[Row]]()
    dataMapDF += ("leakscans" -> leakscans.value)
    val dataMap = spark.sparkContext.broadcast(dataMapDF)
    dataMap
  }

  /**
    * 获取告警规则
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getAlarmRuleMap(spark:SparkSession, url:String, username:String, password:String):Broadcast[ Map[String, Array[Row]]] ={
    val alarmRegexRule = new AlarmRegexRule
    val alarmPolicyWholeDF = alarmRegexRule.getAlarmPolicyWhole(spark,url,username,password).collect()
    val alarmPolicyEventDF = alarmRegexRule.getAlarmPolicyEvent(spark,url,username,password).collect()
    val alarmPolicyRelationEventDF = alarmRegexRule.getAlarmPolicyRelationEvent(spark,url,username,password).collect()
    val alarmPolicyWholeRule = spark.sparkContext.broadcast(alarmPolicyWholeDF)
    val alarmPolicyEventRule = spark.sparkContext.broadcast(alarmPolicyEventDF)
    val alarmPolicyRelationEventRule = spark.sparkContext.broadcast(alarmPolicyRelationEventDF)
    var allRuleDF = Map[String,Array[Row]]()
    allRuleDF += ("alarmPolicyWholeRule" -> alarmPolicyWholeRule.value)
    allRuleDF += ("alarmPolicyEventRule" -> alarmPolicyEventRule.value)
    allRuleDF += ("alarmPolicyRelationEventRule" -> alarmPolicyRelationEventRule.value)
    val allRuleMap = spark.sparkContext.broadcast(allRuleDF)

    allRuleMap
  }

  /**
    * 获取联合事件规则-缓存到REDIS
    * @param jedis
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getAllRuleMapByRedis(jedis:Jedis, url:String, username:String, password:String): Map[String, JSONArray] ={

    val readDataToDataFrame:ReadDataToDataFrame = new ReadDataToDataFrame()
    val logDF: JSONArray = readDataToDataFrame.getDataFrameFromRedis(jedis,"logRegex")
    println("#############范式规则==="+logDF.size())


    val logDFUser = readDataToDataFrame.getDataFrameFromRedis(jedis,"logRegexByUser")

    //事件规则
    val eventDefDFByUser = readDataToDataFrame.getDataFrameFromRedis(jedis,"eventDefByUser")
    println("#############事件规则（自定义）==="+eventDefDFByUser.size)
    val eventDefDFBySystem = readDataToDataFrame.getDataFrameFromRedis(jedis,"eventDefBySystem")
    println("#############事件规则（内置）==="+eventDefDFBySystem.size)


    val eventFieldDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"eventRule")

    //联合事件规则
    val eventmedefineDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"relationEventMedefine")
    val eventmedefinesubDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"relationEventMedefineSub")
    val eventmedetailDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"relationEventMedetail")
    val leakmedetailDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"relationLeakMedetail")
    val netflowmedetailDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"relationNetflowMedetail")
    val nemedetailDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"relationNeMedetail")
    val intelligencemedetailDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"relationIntelligenceMedetail")

    var allRuleDF = Map[String,JSONArray]()

    allRuleDF += ("logRule" -> logDF)
    allRuleDF += ("logUserRule" -> logDFUser)
    allRuleDF += ("eventDefRuleByUser" -> eventDefDFByUser)
    allRuleDF += ("eventDefRuleBySystem" -> eventDefDFBySystem)
    allRuleDF += ("eventFieldRule" -> eventFieldDF)

    allRuleDF += ("eventmedefine" -> eventmedefineDF)
    allRuleDF += ("eventmedefinesub" -> eventmedefinesubDF)
    allRuleDF += ("eventmedetail" -> eventmedetailDF)
    allRuleDF += ("leakmedetail" -> leakmedetailDF)
    allRuleDF += ("netflowmedetail" -> netflowmedetailDF)
    allRuleDF += ("nemedetail" -> nemedetailDF)
    allRuleDF += ("intelligencemedetail" -> intelligencemedetailDF)

    allRuleDF
  }

  /**
    * 获取比对数据，比如漏洞数据比对-缓存到REDIS
    * @param jedis
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getCheckDataMapByRedis(jedis:Jedis, url:String, username:String, password:String): Map[String, JSONArray] ={
    val readDataToDataFrame:ReadDataToDataFrame = new ReadDataToDataFrame()
    val dataCheck = new DataCheck
    val leakscansDF =  readDataToDataFrame.getDataFrameFromRedis(jedis,"leakScans")
    var dataMapDF = Map[String,JSONArray]()
    dataMapDF += ("leakscans" -> leakscansDF)

    dataMapDF
  }

  /**
    * 获取告警规则-缓存到REDIS
    * @param jedis
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getAlarmRuleMapByRedis(jedis:Jedis, url:String, username:String, password:String): Map[String, JSONArray] ={
    val readDataToDataFrame:ReadDataToDataFrame = new ReadDataToDataFrame()
    val alarmRegexRule = new AlarmRegexRule
    val alarmPolicyWholeDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"alarmPolicyWhole")
    val alarmPolicyEventDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"alarmPolicyEvent")
    val alarmPolicyRelationEventDF = readDataToDataFrame.getDataFrameFromRedis(jedis,"alarmPolicyRelationEvent")

    var allRuleDF = Map[String,JSONArray]()
    allRuleDF += ("alarmPolicyWholeRule" -> alarmPolicyWholeDF)
    allRuleDF += ("alarmPolicyEventRule" -> alarmPolicyEventDF)
    allRuleDF += ("alarmPolicyRelationEventRule" -> alarmPolicyRelationEventDF)

    allRuleDF
  }
}
