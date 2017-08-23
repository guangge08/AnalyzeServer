package com.bluedon.initRule

import org.apache.spark.sql._

/**
  * Created by dengxiwen on 2016/12/27.
  */
class RelationEventRegexRule {

  /**
    * 联合事件定义信息
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getRelationEventMedefine(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select RECORDID,MULTIEVENTNAME,EVENTDESC,MULTIEVENTTYPE,EVENTLEVEL,STATUS,ADVICE" +
      " from T_Siem_Medefine where STATUS=1) as tSiemMedefine"
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
    * 联合事件子集
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getRelationEventMedefineSub(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select RECORDID,MEDEFINEID,OP,OBJTABLE,STATUS" +
      " from T_SIEM_MEDEFINE_SUB where STATUS=1 order by OBJTABLE asc) as tSiemMedefine"
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
    * 联合事件-规则关联详情
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getRelationEventMedetail(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select RECORDID,MEDID,EVENTID,TIMELIMIT,TIMES,OP,FLAG,FIELDNAME,FIELDOP,FIELDVALUE  from T_SIEM_MEDETAIL) as tSiemMedetail"
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
    * 联合事件-漏洞关联详情
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getRelationLeakMedetail(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select RECORDID,MEDID,CVEMATCH,LEAKMATCH,PORTMATCH  from T_SIEM_MEDETAIL3) as tSiemMedetail"
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
    * 联合事件-流量关联详情
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getRelationNetflowMedetail(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select RECORDID,MEDID,BEGINTIME,ENDTIME,STATISPERIOD,FLOWLIMIT,DIOLOGLIMIT,CONDITIONEXP  from T_SIEM_MEDETAIL6) as tSiemMedetail"
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
    * 联合事件-资产关联详情
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getRelationNeMedetail(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select RECORDID,MEDID,IPTYPE,IPADDR,OS  from T_SIEM_MEDETAIL5) as tSiemMedetail"
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
    * 联合事件-情报关联详情
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getRelationIntelligenceMedetail(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select RECORDID,MEDID,IPMATCH  from T_SIEM_MEDETAIL4) as tSiemMedetail"
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
