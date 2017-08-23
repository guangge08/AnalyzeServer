package com.bluedon.initRule

import org.apache.spark.sql._

/**
  * Created by dengxiwen on 2017/1/23.
  */
class DataCheck {

  /**
    * 获取漏洞扫描信息
    *
    * @param spark
    * @param url
    * @param username
    * @param password
    * @return
    */
  def getLeakScans(spark:SparkSession,url:String,username:String,password:String): DataFrame = {
    val sql:String = "(select recordid,neid,neip,leak_name,leak_port,leak_level,leak_type,leak_cve,leak_sid,leak_flag " +
      " from T_SIEM_LEAKSCAN) as tSiemLeakScan"
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
