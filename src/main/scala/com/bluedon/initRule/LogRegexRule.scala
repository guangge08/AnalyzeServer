package com.bluedon.initRule

import java.util.UUID

import org.apache.phoenix.schema.tuple.Tuple
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by dengxiwen on 2016/12/20.
  * 规则管理
  */
class LogRegexRule {

  /**
    * 获取日志正则库-内置
    * @param spark spark上下文
    * @param url 数据库连接
    * @param username 数据库用户名
    * @param password 数据库密码
    * @return
    */
  def getLogRegex(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    val sql:String = "(select logregex.LOG_RULE_ID,logregex.LOG_RULE_LIB_ID,logregex.LOG_RULE_CODE,logregex.ORIGINAL_LOG,logregex.LOG_REGEX,logregex.EVENTDEFID,logregex.EVENTLEVEL,logregex.EVENTSUBTYPE,logregex.STORAGETIME," +
      "logregex.ORILEVEL,logregex.PROTO,logregex.REPORTNETYPE,logregex.REPORTVENDOR,logregex.REPORTNEIP,logregex.REPORTIP,logregex.REPORTAPP,logregex.SOURCEIP,logregex.SOURCEPORT," +
      "logregex.DESTIP,logregex.DESTPORT,logregex.DESTNEID,logregex.EVENTNAME,logregex.EVENTDETAIL," +
      "logregex.APPPROTO,logregex.URL,logregex.GETPARAMETER," +
      "logregex.EVENTACTION,logregex.OPENTIME,logregex.ACTIONOPT,logregex.ACTIONRESULT,lib.device_ip,logregex.EVENTID," +
      //2017-08-11新增12个字段 霍广
      "logregex.INFOID, logregex.AFFECTEDSYSTEM, logregex.ATTACKMETHOD, logregex.APPID, logregex.VICTIMTYPE, logregex.ATTACKFLAG, logregex.ATTACKER," +
      "logregex.VICTIM, logregex.HOST, logregex.FILEMD5, logregex.FILEDIR, logregex.REFERER, logregex.REQUESTMETHOD " +
      "from T_SIEM_LOG_REGEX logregex " +
      "RIGHT JOIN ( SELECT regexlib.LOG_RULE_LIB_ID,moni_ne.device_ip,regexlib.HTYEID htypeid,regexlib.vendorid,regexlib.model " +
      "FROM T_SIEM_LOG_REGEX_LIB regexlib " +
      "INNER JOIN ( SELECT ne.HTYPEID,ne.VENDORID,ne.MODEL,ip.device_ip FROM T_SIEM_MONI_NE ne " +
      "LEFT JOIN t_moni_device_ip ip ON ip.ne_id = ne.ne_id) moni_ne ON regexlib.HTYEID = moni_ne.HTYPEID " +
      "AND regexlib.VENDORID = moni_ne.VENDORID AND regexlib.MODEL = moni_ne.MODEL " +
      ") lib ON lib.log_rule_lib_id = logregex.log_rule_lib_id " +
      "WHERE  lib.device_ip is not null and logregex.LOG_RULE_ID is not null" +
      ") as logregex"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", sql)
      .option("user", username)
      .option("password", password)
      .load()
//    jdbcDF.select("log_rule_code").show()
    jdbcDF.show()
    jdbcDF
  }

  /**
    * 获取日志正则库-自定义
    * @param spark spark上下文
    * @param url 数据库连接
    * @param username 数据库用户名
    * @param password 数据库密码
    * @return
    */
  def getLogRegexByUser(spark:SparkSession,url:String,username:String,password:String): DataFrame ={
    val sql:String = "(select logregex.LOG_RULE_ID,logregex.LOG_RULE_LIB_ID,logregex.LOG_RULE_CODE,logregex.LOG_REGEX,logregex.EVENTDEFID,logregex.EVENTLEVEL,logregex.EVENTSUBTYPE,logregex.STORAGETIME," +
      "logregex.ORILEVEL,logregex.PROTO,logregex.REPORTNETYPE,logregex.REPORTVENDOR,logregex.REPORTNEIP,logregex.REPORTIP,logregex.REPORTAPP,logregex.SOURCEIP,logregex.SOURCEPORT," +
      "logregex.DESTIP,logregex.DESTPORT,logregex.DESTNEID,logregex.EVENTNAME,logregex.EVENTDETAIL," +
      "logregex.APPPROTO,logregex.URL,logregex.GETPARAMETER," +
      "logregex.EVENTACTION,logregex.OPENTIME,logregex.ACTIONOPT,logregex.ACTIONRESULT,logregex.EVENTID from T_SIEM_LOG_REGEX logregex " +
      "WHERE  logregex.IS_INNER='0'" +
      ") as logregex"
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
