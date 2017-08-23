package com.bluedon.asset

import java.sql.Connection
import java.sql.Statement
import java.sql.ResultSet
import java.util.{Calendar, Date, UUID}

import com.bluedon.flow
import com.bluedon.utils.{IpUtil, ROWUtils}
import org.apache.spark.sql._

/**
  * 资产自动发现
  * Created by dengxiwen on 2017/2/9.
  */
case class flowTransform(ip: String, port: Int, proto: String, packageNum: Int)

class AssetAuto {

  /**
    * 资产自动发现
    *
    * @param input
    * @param posgreConn
    */
  def assetAuto(input: flow, posgreConn: Connection, neIps: Array[Row]): Unit = {

    val time = input.recordTime

    if (IpUtil.ipExistsInRange(input.srcip, "221.176.64.00-221.176.64.255") || IpUtil.ipExistsInRange(input.srcip, "221.176.65.00-221.176.65.255")
      || IpUtil.ipExistsInRange(input.srcip, "221.176.68.00-221.176.68.255") || IpUtil.ipExistsInRange(input.srcip, "221.176.70.00-221.176.79.255") || IpUtil.ipExistsInRange(input.srcip, "221.176.69.1-221.176.69.11")) {
      //比较源IP

      val st: Statement = posgreConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY)

      val existSql = s"""select ip from T_SIEM_DEV_FIND where ip = '${input.srcip}'"""
      val rs: ResultSet = st.executeQuery(existSql)
      rs.last()
      // ip 不存在于资产发现表
      if (rs.getRow == 0) {
        if (!(input.proto == "ICMP")) {
          var recordid = ""
          //非新
          var isnew = 0
          val port = input.srcport
          if (!exitAsset(input.srcip, neIps)) {
            //新
            isnew = 1

            recordid = UUID.randomUUID().toString

            val insertSql = s"""INSERT INTO T_SIEM_DEV_FIND (RECORDID,IP,ISNEW,FLG,STORAGETIME,PROTOCOL, PORT, PACKAGENUM) values ('$recordid','${input.srcip}', $isnew,0,'$time','${input.proto}', '$port', '${input.packageNum}')"""
            st.execute(insertSql)
          }
        }
      }
      st.close()
    } else {
      if (IpUtil.ipExistsInRange(input.dstip, "221.176.64.00-221.176.64.255") || IpUtil.ipExistsInRange(input.dstip, "221.176.65.00-221.176.65.255")
        || IpUtil.ipExistsInRange(input.dstip, "221.176.68.00-221.176.68.255") || IpUtil.ipExistsInRange(input.dstip, "221.176.70.00-221.176.79.255") || IpUtil.ipExistsInRange(input.dstip, "221.176.69.1-221.176.69.11")) {

        val st: Statement = posgreConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
          ResultSet.CONCUR_READ_ONLY)

        val existSql = s"""select ip from T_SIEM_DEV_FIND where ip = '${input.dstip}'"""
        val rs: ResultSet = st.executeQuery(existSql)
        rs.last()
        if (rs.getRow == 0) {
          if (input.proto == "TCP" && input.packageNum >= 3) {

            val st: Statement = posgreConn.createStatement()
            var recordid = ""
            //不在
            var isnew = 0
            val port = input.dstport
            if (!exitAsset(input.dstip, neIps)) {
              //在
              isnew = 1
              recordid = UUID.randomUUID().toString
              val insertSql = s"""INSERT INTO T_SIEM_DEV_FIND (RECORDID,IP,ISNEW,FLG,STORAGETIME,PROTOCOL, PORT, PACKAGENUM) values ('$recordid','${input.dstip}', $isnew,0,'$time','${input.proto}', '$port','${input.packageNum}')"""
              st.execute(insertSql)
            }
          }
        }
        st.close()
      }
    }
  }

  /**
    * 判断ip是否存在资产库中
    *
    * @param ip
    * @param neIps
    * @return
    */
  def exitAsset(ip: String, neIps: Array[Row]): Boolean = {
    var isExit = false
    neIps.foreach(neip => {
      val matchip = neip.getString(0)
      if (ip != null && ip.equalsIgnoreCase(matchip)) {
        isExit = true
      }
    })
    isExit
  }

  /**
    * 获取资产IP
    *
    * @param spark    spark上下文
    * @param url      数据库连接
    * @param username 数据库用户名
    * @param password 数据库密码
    * @return
    */
  def getNeIp(spark: SparkSession, url: String, username: String, password: String): DataFrame = {
    val sql: String = "(select DEVICE_IP from T_MONI_DEVICE_IP) as neIp"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", sql)
      .option("user", username)
      .option("password", password)
      .load()
    jdbcDF
  }

}
