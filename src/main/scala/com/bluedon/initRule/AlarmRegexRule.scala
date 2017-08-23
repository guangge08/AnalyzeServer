package com.bluedon.initRule

import org.apache.spark.sql._

/**
  * Created by dengxiwen on 2016/12/28.
  */
class AlarmRegexRule {

  /**
    * 获取告警策略整体表信息
    *
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getAlarmPolicyWhole(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    val sql:String = "(select RECORDID,ALARM_NAME,START_TIME,END_TIME,ALARM_LEVEL,ALERT_TYPE,OBJTABLE" +
      ",OBJID,ALARM_COMMENT,STATUS from T_SIEM_ALARM_POLICY_WHOLE where STATUS=1) as policyWhole"
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
    * 获取单事件告警策略信息 T_SIEM_ALARM_POLICY_EVENT
    *
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getAlarmPolicyEvent(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    //val sql:String = "(select RECORDID,ALARM_ID,EVENT_RULE_ID,CONTROL_TIME,COUNT from T_SIEM_ALARM_POLICY_EVENT where STATUS=1) as policyEvent"
    val sql:String = "(select RECORDID,ALARM_ID,EVENT_RULE_ID,CONTROL_TIME,COUNT from T_SIEM_ALARM_POLICY_EVENT) as policyEvent"
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
    * 获取联合事件告警策略信息 T_SIEM_ALARM_POLICY_EVENT
    *
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getAlarmPolicyRelationEvent(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    //val sql:String = "(select RECORDID,ALARM_ID,RELATION_EVENT_RULE_ID,CONTROL_TIME,COUNT from T_SIEM_ALARM_POLICY_RELATION_EV where STATUS=1) as policyEvent"
    val sql:String = "(select RECORDID,ALARM_ID,RELATION_EVENT_RULE_ID,CONTROL_TIME,COUNT from T_SIEM_ALARM_POLICY_RELATION_EV) as policyEvent"
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
