package com.bluedon.initRule

import org.apache.spark.sql._

/**
  * Created by dengxiwen on 2016/12/27.
  */
class EventRegexRule {

  /**
    * 获取事件定义信息 --用户自定义
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getEventDefByUser(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    val sql:String = "(select EVENT_RULE_ID,EVENT_RULE_LIB_ID,EVENT_RULE_CODE," +
      "EVENT_RULE_NAME,EVENT_RULE_LEVEL,EVENT_BASIC_TYPE,event_sub_type,EVENT_RULE_DESC," +
      "EVENT_RULE,is_inner,IS_USE from T_SIEM_EVENT_RULE_DEF where is_inner=0 and IS_USE=1) as eventDef"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", sql)
      .option("user", username)
      .option("password", password)
      .load()
    jdbcDF
  }

  /**
    * 获取事件定义信息 --内置
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getEventDefBySystem(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    val sql:String = "(select event.event_rule_id,event.EVENT_RULE_NAME,event.EVENT_RULE_LEVEL,event.EVENT_BASIC_TYPE,event.event_sub_type,eventRule.FILED_BEGIN_VALUE" +
      ",eventRule.EVENT_RULE_FIELD,eventRule.MATCH_METHOD " +
      "from T_SIEM_EVENT_RULE_DEF event RIGHT JOIN T_SIEM_EVENT_RULE_FIELD eventRule on event.event_rule_id=eventRule.event_rule_id where event.is_inner=1 and event.IS_USE=1) as eventDef"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", sql)
      .option("user", username)
      .option("password", password)
      .load()
    jdbcDF
  }

  /**
    * 获取事件定义规则
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getEventRule(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    val sql:String = "(select EVENT_RULE_FIELD_ID,EVENT_RULE_ID," +
      "EVENT_RULE_FIELD,MATCH_METHOD,FILED_BEGIN_VALUE,FILED_END_VALUE from T_SIEM_EVENT_RULE_FIELD ) as eventRule"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", sql)
      .option("user", username)
      .option("password", password)
      .load()
    jdbcDF
  }
}
