package com.bluedon.dataMatch

import java.sql.{Connection, Statement}
import java.util
import java.util.UUID

import com.bluedon.utils.{DBUtils, DateUtils, ROWUtils,UnicodeUtils}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import com.bluedon.esinterface.statics.ESStaticsUtils
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.client.Client

import scala.util.control.Breaks._
import java.util.regex.Matcher

/**
  * Created by dengxiwen on 2016/12/27.
  */
class LogMatch {

  /**
    * 根据原始日志匹配的字段获取对应的值
    *
    * @param matchList
    * @param fieldKey
    * @return
    */
  def getMatchValue(matchList: Matcher, fieldKey: String): String = {
    var resValue = ""

    if(StringUtils.isNotEmpty(fieldKey)){
      try{
        resValue=  matchList.group(fieldKey)
      }catch {
        case ex:Exception=>{
          resValue = ""
        }
      }
    }

    resValue
  }

  /**
    * 匹配日志-内置
    *
    * @param x
    * @param logRegex
    * @return
    */
  def matchLog(x:String,logRegex:JSONArray):String = {
    //val rules = logRegex.collect()
    var matchStr = ""
    val rules = logRegex
    val receiveLog = x.split("~")
    if(receiveLog != null && receiveLog.length>6){
      val logStr = receiveLog(6)
      val reportip = receiveLog(2)

      breakable {
        //根据上报设备IP过滤日志匹配规则
        for(i <- 0 to rules.size()-1) {
          val rule:JSONObject = rules.get(i).asInstanceOf[JSONObject]
          if (reportip != null && rule.get("device_ip") != null && reportip.trim.equals(rule.getString("device_ip").trim)) {
            val log_rule_id = rule.getString("log_rule_id").trim
            val matchRegexStr = rule.getString("log_regex").trim.r
            val matchList = matchRegexStr.pattern.matcher(logStr)
            //ROW,recordid,firstrecvtime,reportapp,reportip,sourceip,sourceport,destip,destport,eventaction,actionresult,
            // reportnetype,eventdefid,eventname,eventlevel,orgid,
            // appProto,url, getparameter,
            // 入库前19个字段
            // 后五个字段未入库
            // proto, opentime, eventid,
            // logStr,log_rule_id
            //2017-08-10 霍广新增 13个字段 25-- 37字段
            //infoid affectedsystem attackmethod appid victimtype attackflag attacker victim host filemd5 filedir
            //referer requestmethod
            //

            if (matchList != null && matchList.groupCount > 0 && matchList.find()) {
             // matchStr = dealOriginalLog(receiveLog,rule,matchList)

              val recordid = UUID.randomUUID().toString.replaceAll("-", "")
              matchStr = recordid + "~" + recordid + "~" + receiveLog(1) + "~" + receiveLog(5) + "~" + receiveLog(2)

              //源IP
              val sourceip = getMatchValue(matchList,"sourceIp")   // matchList.group("sourceIp") //获取对应位置上sourceip
              if(StringUtils.isNotEmpty(sourceip)){
                matchStr += "~" +sourceip
              }else {
                matchStr += "~" + " "
              }


              //源端口
              val sourceport = getMatchValue(matchList,"sourcePort")
              if(StringUtils.isNotEmpty(sourceport)){
                matchStr += "~" + sourceport.toString.replace(":", "")
              }else{
                matchStr += "~" + " "
              }

              //目的IP
              val destip = getMatchValue(matchList,"destIp")
              if(StringUtils.isNotEmpty(destip)){
                matchStr += "~" + destip.toString
              }else{
                matchStr += "~" + " "
              }

              //目的端口
              val destport = getMatchValue(matchList,"destPort")
              if(StringUtils.isNotEmpty(destport)){
                matchStr += "~" + destport.toString.replace(":", "")
              }else{
                matchStr += "~" + " "
              }

              //执行操作
              val eventAction = getMatchValue(matchList,"actionOpt")
              if(StringUtils.isNotEmpty(eventAction)){
                matchStr += "~" + eventAction.toString
              }else{
                matchStr += "~" + " "
              }

              //执行结果
              val actionresult =getMatchValue(matchList,"actionResult")
              if(StringUtils.isNotEmpty(actionresult)){
                matchStr += "~" + actionresult.toString
              }else{
                matchStr += "~" + " "
              }

              //设备类型
              if (rule.get("reportnetype") != null) {
                val reportnetype = rule.getString("reportnetype")
                matchStr += "~" + reportnetype
              } else {
                matchStr += "~" + " "
              }

              //事件类型
              if (rule.get("eventdefid") != null) {
                val eventdefid = rule.getString("eventdefid").replaceAll("-", "")
                matchStr += "~" + eventdefid
              } else {
                matchStr += "~" + " "
              }

              //事件名称,这里eventname是随着log一起给出的
              val eventname = getMatchValue(matchList,"eventName")
              if(StringUtils.isNotEmpty(eventname)){
                matchStr += "~" + eventname
              }else{
                matchStr += "~" + " "
              }

              //事件级别
              if (rule.get("eventlevel") != null) {
                val eventlevel = rule.getString("eventlevel")
                if(StringUtils.isNotEmpty(eventlevel)){
                  matchStr += "~" + eventlevel
                }else{
                  matchStr += "~1"
                }
              } else {
                matchStr += "~" + "1"
              }

              //原始日志
              matchStr += "~" + receiveLog(0)

              //应用层协议
              val appproto = getMatchValue(matchList,"appProto")
              if(StringUtils.isNotEmpty(appproto)){
                matchStr += "~" + appproto.toString
              }else{
                matchStr += "~" + " "
              }

              //URL
              val url = getMatchValue(matchList,"url")
              if(StringUtils.isNotEmpty(url)){
                matchStr += "~" + url.toString
              }else{
                matchStr += "~" + " "
              }

              //GET参数
              val getparameter = getMatchValue(matchList,"getParameter")
              if(StringUtils.isNotEmpty(getparameter)){
                matchStr += "~" + getparameter.toString
              }else{
                matchStr += "~" + " "
              }

              //PROTO
              val proto = getMatchValue(matchList,"proto")
              if(StringUtils.isNotEmpty(proto)){
                matchStr += "~" + proto.toString
              }else{
                matchStr += "~" + " "
              }

              //OPENTIME
              val opentime = getMatchValue(matchList,"openTime")
              if(StringUtils.isNotEmpty(opentime)){
                matchStr += "~" + opentime.toString
              }else{
                matchStr += "~" + " "
              }

              //事件id
              if (rule.get("eventid") != null) {
                val eventid = rule.getString("eventid")
                matchStr += "~" + eventid
              } else {
                matchStr += "~" + " "
              }
              matchStr += "~" + logStr + "~" + log_rule_id

              //infoid
              val infoid = getMatchValue(matchList,"infoid")
              if(StringUtils.isNotEmpty(infoid)){
                matchStr += "~" + infoid.toString
              }else{
                matchStr += "~" + " "
              }

              //affectedsystem
              val affectedsystem = getMatchValue(matchList,"affectedsystem")
              if(StringUtils.isNotEmpty(affectedsystem)){
                matchStr += "~" + affectedsystem.toString
              }else{
                matchStr += "~" + " "
              }
              //attackmethod
              val attackmethod = getMatchValue(matchList,"attackmethod")
              if(StringUtils.isNotEmpty(attackmethod)){
                matchStr += "~" + attackmethod.toString
              }else{
                matchStr += "~" + " "
              }

              //appid
              val appid = getMatchValue(matchList,"appid")
              if(StringUtils.isNotEmpty(appid)){
                matchStr += "~" + appid.toString
              }else{
                matchStr += "~" + " "
              }
              //victimtype
              val victimtype = getMatchValue(matchList,"victimtype")
              if(StringUtils.isNotEmpty(victimtype)){
                matchStr += "~" + victimtype.toString
              }else{
                matchStr += "~" + " "
              }

              //attackflag
              val attackflag = getMatchValue(matchList,"attackflag")
              if(StringUtils.isNotEmpty(attackflag)){
                matchStr += "~" + attackflag
              }else{
                matchStr += "~" + " "
              }
              //attacker
              val attacker = getMatchValue(matchList,"attacker")
              if(StringUtils.isNotEmpty(attacker)){
                matchStr += "~" + attacker.toString
              }else{
                matchStr += "~" + " "
              }

              //victim
              val victim = getMatchValue(matchList,"victim")
              if(StringUtils.isNotEmpty(victim)){
                matchStr += "~" + victim.toString
              }else{
                matchStr += "~" + " "
              }

              //host
              val host = getMatchValue(matchList,"host")
              if(StringUtils.isNotEmpty(host)){
                matchStr += "~" + host.toString
              }else{
                matchStr += "~" + " "
              }

              //filemd5
              val filemd5 = getMatchValue(matchList,"filemd5")
              if(StringUtils.isNotEmpty(filemd5)){
                matchStr += "~" + filemd5.toString
              }else{
                matchStr += "~" + " "
              }
              //filedir
              val filedir = getMatchValue(matchList,"filedir")
              if(StringUtils.isNotEmpty(filedir)){
                matchStr += "~" + filedir.toString
              }else{
                matchStr += "~" + " "
              }
              //referer
              val referer = getMatchValue(matchList,"referer")
              if(StringUtils.isNotEmpty(referer)){
                matchStr += "~" + referer.toString
              }else{
                matchStr += "~" + " "
              }
              //requestmethod
              val requestmethod = getMatchValue(matchList,"requestmethod")
              if(StringUtils.isNotEmpty(requestmethod)){
                matchStr += "~" + requestmethod.toString
              }else{
                matchStr += "~" + " "
              }

              return matchStr.toString
              break
            }
          }
        }
      }
    }
    //不匹配返回空
    matchStr
  }

  /**
    * 匹配日志-自定义
    *
    * @param x
    * @param logRegex
    * @return
    */
  def matchLogUser(x:String,logRegex:JSONArray):String = {
    //val rules = logRegex.collect()
    var matchStr = ""
    val rules = logRegex
    val receiveLog = x.split("~")
    if(receiveLog != null && receiveLog.length>6){
      val logStr = receiveLog(6)
      val reportip = receiveLog(2)


      breakable {
        //根据上报设备IP过滤日志匹配规则

        for(i <- 0 to rules.size()-1) {
          val rule:JSONObject = rules.get(i).asInstanceOf[JSONObject]
          val log_rule_id = rule.getString("log_rule_id").trim
          val matchRegexStr = rule.getString("log_regex").trim.r
          val matchList = matchRegexStr.pattern.matcher(logStr)
          //ROW,recordid,firstrecvtime,reportapp,reportip,sourceip,sourceport,destip,destport,eventaction,actionresult,reportnetype,eventdefid,eventname,eventlevel,orgid
          //规则匹配

          if (matchList != null && matchList.groupCount > 0 && matchList.find()) {
            // matchStr = dealOriginalLog(receiveLog,rule,matchList)

            val recordid = UUID.randomUUID().toString.replaceAll("-", "")
            matchStr = recordid + "~" + recordid + "~" + DateUtils.dateToStamp(receiveLog(1)) + "~" + receiveLog(5) + "~" + receiveLog(2)

            //源IP
            val sourceip = getMatchValue(matchList,"sourceIp")
            if(StringUtils.isNotEmpty(sourceip)){
              matchStr += "~" +sourceip
            }else {
              matchStr += "~" + " "
            }
            /*val sourceIp = rule.get("sourceip").toString.trim  //得到sourceip索引
            if (sourceIp != null && StringUtils.isNotEmpty(sourceIp)) {
              val sourceipIndex = sourceIp.toInt
              val sourceip =  matchList.group(sourceipIndex) //获取对应位置上sourceip
              if(StringUtils.isNotEmpty(sourceip)){
                matchStr += "~" +sourceip
              }else {
                matchStr += "~" + " "
              }
            } else {
              matchStr += "~" + " "
            }*/

            //源端口
            //源端口
            val sourceport = getMatchValue(matchList,"sourcePort")
            if(StringUtils.isNotEmpty(sourceport)){
              matchStr += "~" + sourceport.toString.replace(":", "")
            }else{
              matchStr += "~" + " "
            }

            //目的IP
            val destip = getMatchValue(matchList,"destIp")
            if(StringUtils.isNotEmpty(destip)){
              matchStr += "~" + destip.toString
            }else{
              matchStr += "~" + " "
            }

            //目的端口
            val destport = getMatchValue(matchList,"destPort")
            if(StringUtils.isNotEmpty(destport)){
              matchStr += "~" + destport.toString.replace(":", "")
            }else{
              matchStr += "~" + " "
            }

            //执行操作
            val eventAction = getMatchValue(matchList,"actionOpt")
            if(StringUtils.isNotEmpty(eventAction)){
              matchStr += "~" + eventAction.toString
            }else{
              matchStr += "~" + " "
            }

            //执行结果
            val actionresult =getMatchValue(matchList,"actionResult")
            if(StringUtils.isNotEmpty(actionresult)){
              matchStr += "~" + actionresult.toString
            }else{
              matchStr += "~" + " "
            }

            //设备类型
            if (rule.get("reportnetype") != null) {
              val reportnetype = rule.getString("reportnetype")
              matchStr += "~" + reportnetype
            } else {
              matchStr += "~" + " "
            }

            //事件类型
            if (rule.get("eventdefid") != null) {
              val eventdefid = rule.getString("eventdefid").replaceAll("-", "")
              matchStr += "~" + eventdefid
            } else {
              matchStr += "~" + " "
            }

            //事件名称,这里eventname是随着log一起给出的
            val eventname = getMatchValue(matchList,"eventName")
            if(StringUtils.isNotEmpty(eventname)){
              matchStr += "~" + eventname
            }else{
              matchStr += "~" + " "
            }

            //事件级别
            if (rule.get("eventlevel") != null) {
              val eventlevel = rule.getString("eventlevel")
              if(StringUtils.isNotEmpty(eventlevel)){
                matchStr += "~" + eventlevel
              }else{
                matchStr += "~1"
              }
            } else {
              matchStr += "~" + "1"
            }

            //原始日志
            matchStr += "~" + receiveLog(0)

            //应用层协议
            val appproto = getMatchValue(matchList,"appProto")
            if(StringUtils.isNotEmpty(appproto)){
              matchStr += "~" + appproto.toString
            }else{
              matchStr += "~" + " "
            }

            //URL
            val url = getMatchValue(matchList,"url")
            if(StringUtils.isNotEmpty(url)){
              matchStr += "~" + url.toString
            }else{
              matchStr += "~" + " "
            }

            //GET参数
            val getparameter = getMatchValue(matchList,"getParameter")
            if(StringUtils.isNotEmpty(getparameter)){
              matchStr += "~" + getparameter.toString
            }else{
              matchStr += "~" + " "
            }

            //PROTO
            val proto = getMatchValue(matchList,"proto")
            if(StringUtils.isNotEmpty(proto)){
              matchStr += "~" + proto.toString
            }else{
              matchStr += "~" + " "
            }

            //OPENTIME
            val opentime = getMatchValue(matchList,"openTime")
            if(StringUtils.isNotEmpty(opentime)){
              matchStr += "~" + opentime.toString
            }else{
              matchStr += "~" + " "
            }

            //事件id
            if (rule.get("eventid") != null) {
              val eventid = rule.getString("eventid")
              matchStr += "~" + eventid
            } else {
              matchStr += "~" + " "
            }
            matchStr += "~" + logStr + "~" + log_rule_id
            return matchStr.toString
            break
          }

        }
      }
    }
    //不匹配返回空
    matchStr
  }

  /**
    * 范式化日志入库
    *
    * @param log
    * @param phoenixConn
    */
  def saveMatchLog(log:String,phoenixConn:Connection):Unit = {
    if(log != null && log.contains("~")){
      val mlogs = log.toString.split("~")
      val rowkey = ROWUtils.genaralROW()
      var sql:String = "upsert into T_SIEM_GENERAL_LOG "
      sql += "(\"ROW\",\"RECORDID\",\"FIRSTRECVTIME\",\"REPORTAPP\",\"REPORTIP\",\"SOURCEIP\",\"SOURCEPORT\",\"DESTIP\",\"DESTPORT\",\"EVENTACTION\",\"ACTIONRESULT\",\"REPORTNETYPE\",\"EVENTDEFID\",\"EVENTNAME\",\"EVENTLEVEL\",\"ORGID\",\"APPPROTO\",\"URL\",\"GETPARAMETER\") "
      sql += " values ('"+rowkey+"','"+mlogs(1).toString+"','"+mlogs(2).toString+"','"+mlogs(3).toString+"','"+mlogs(4).toString+"','"+mlogs(5).toString+"','"+mlogs(6).toString+"','"+mlogs(7).toString+"','"+mlogs(8).toString+"','"+mlogs(9).toString+"','"+mlogs(10).toString+"','"+mlogs(11).toString+"','"+mlogs(12).toString+"','"+mlogs(13).toString+"',"+mlogs(14).toInt+",'"+mlogs(15).toString+"','"+mlogs(16).toString+"','"+mlogs(17).toString+"','"+mlogs(18).toString +"')"
      val st = phoenixConn.createStatement()
      st.execute(sql)
      phoenixConn.commit()

      var logJson:JSONObject = new JSONObject();
      logJson.put("ROW",rowkey)
      logJson.put("RECORDID",mlogs(1).toString)
      logJson.put("FIRSTRECVTIME",DateUtils.dateToStamp(mlogs(2).toString ))
      logJson.put("REPORTAPP",mlogs(3).toString)
      logJson.put("REPORTIP",mlogs(4).toString)
      logJson.put("SOURCEIP",mlogs(5).toString)
      logJson.put("SOURCEPORT",mlogs(6).toString)
      logJson.put("DESTIP",mlogs(7).toString)
      logJson.put("DESTPORT",mlogs(8).toString)
      logJson.put("EVENTACTION",mlogs(9).toString)
      logJson.put("ACTIONRESULT",mlogs(10).toString)
      logJson.put("REPORTNETYPE",mlogs(11).toString)
      logJson.put("EVENTDEFID",mlogs(12).toString)
      logJson.put("EVENTNAME",mlogs(13).toString)
      logJson.put("EVENTLEVEL",mlogs(14).toString)
      logJson.put("ORGID",mlogs(15).toString)
      logJson.put("APPPROTO",mlogs(16).toString)
      logJson.put("URL",mlogs(17).toString)
      logJson.put("GETPARAMETER",mlogs(18).toString)

      var keySet = logJson.keys()
      var tempjson:JSONObject = new JSONObject();
      while (keySet.hasNext){
        var key:String = keySet.next().asInstanceOf[String];
        tempjson.put(key.toLowerCase(), logJson.get(key));
      }
      logJson = tempjson
      /*
      var logjson = "{"
      logjson += "\"ROW\":\"" +rowkey + "\","
      logjson += "\"RECORDID\":\"" +mlogs(1).toString + "\","
      logjson += "\"FIRSTRECVTIME\":\"" +DateUtils.dateToStamp(mlogs(2).toString ) + "\","
      logjson += "\"REPORTAPP\":\"" +mlogs(3).toString + "\","
      logjson += "\"REPORTIP\":\"" +mlogs(4).toString + "\","
      logjson += "\"SOURCEIP\":\"" +mlogs(5).toString + "\","
      logjson += "\"SOURCEPORT\":\"" +mlogs(6).toString + "\","
      logjson += "\"DESTIP\":\"" +mlogs(7).toString + "\","
      logjson += "\"DESTPORT\":\"" +mlogs(8).toString + "\","
      logjson += "\"EVENTACTION\":\"" +mlogs(9).toString + "\","
      logjson += "\"ACTIONRESULT\":\"" +mlogs(10).toString + "\","
      logjson += "\"REPORTNETYPE\":\"" +mlogs(11).toString + "\","
      logjson += "\"EVENTDEFID\":\"" +mlogs(12).toString + "\","
      logjson += "\"EVENTNAME\":\"" +mlogs(13).toString + "\","
      logjson += "\"EVENTLEVEL\":\"" +mlogs(14).toString + "\","
      logjson += "\"ORGID\":\"" +mlogs(15).toString + "\""
      logjson += "}"
      */
      val client = ESClient.esClient()

      val indexName = "genlog"
      val typeName = "genlog"
      IndexUtils.addIndexData(client, indexName, typeName, logJson.toString)
    }

  }

  /**
    * 批量原始日志入库
    *
    * @param logs
    * @param stmt
    */
  def   batchSaveSysLog(logs:List[String],stmt:Statement,client:Client):Unit = {
    if(logs != null && logs.size>0){
      val tempLogs:java.util.List[String] = new java.util.ArrayList()
      logs.foreach(log =>{
        if(log != null && log.contains("~") && log.toString.split("~").length>=8){
          val mlogs = log.toString.split("~")
          val rowkey = ROWUtils.genaralROW()
          var ismatch = 1
          if(mlogs(7).toString != null && !mlogs(7).toString.trim.equals("")){
            try{
              ismatch = Integer.parseInt(mlogs(7).toString.trim)
            }catch {
              case ex:Exception =>{
                ismatch = 0
              }
            }
          }
//          var sql:String = "upsert into T_SIEM_ORIGINAL_LOG "
//          sql += "(\"ROW\",\"RECORDID\",\"RECVTIME\",\"STORAGETIME\",\"REPORTIP\",\"REPORTAPP\",\"LOGCONTENT\",\"ISMATCH\") "
//          sql += " values ('"+rowkey+"','"+mlogs(0).toString+"','"+mlogs(1).toString+"','"+mlogs(1).toString+"','"+mlogs(2).toString+"','"+mlogs(5).toString+"','"+mlogs(6).toString+ "',"+ismatch+")"
//          stmt.addBatch(sql)

          var logJson:JSONObject = new JSONObject();
          logJson.put("ROW",rowkey)
          logJson.put("RECORDID",mlogs(0).toString)
          logJson.put("RECVTIME",DateUtils.dateToStamp(mlogs(1).toString.trim).toLong )
          logJson.put("STORAGETIME",DateUtils.dateToStamp(mlogs(1).toString.trim).toLong)
          logJson.put("REPORTIP",mlogs(2).toString)
          logJson.put("REPORTAPP",mlogs(5).toString)
          logJson.put("LOGCONTENT",mlogs(6).toString)
          logJson.put("ISMATCH",ismatch)

          var keySet = logJson.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), logJson.get(key));
          }
          logJson = tempjson

          tempLogs.add(logJson.toString);
        }
      })

      val indexName = "syslog"
      val typeName = "syslog"

      IndexUtils.batchIndexData(client,indexName,typeName,tempLogs)
    }
  }
  /**
    * 批量范式化日志入库
    *
    * @param logs
    * @param stmt
    */
  def batchSaveMatchLog(logs:List[String],stmt:Statement,client:Client):Unit = {
    if(logs != null && logs.size>0){
      val tempLogs:java.util.List[String] = new java.util.ArrayList()
      logs.foreach(log =>{
        if(log != null && log.contains("~") && log.toString.split("~").length>=15){
          val mlogs = log.toString.split("~")
          val rowkey = ROWUtils.genaralROW()
//          var sql:String = "upsert into T_SIEM_GENERAL_LOG "
//          sql += "(\"ROW\",\"RECORDID\",\"FIRSTRECVTIME\",\"REPORTAPP\",\"REPORTIP\",\"SOURCEIP\",\"SOURCEPORT\",\"DESTIP\",\"DESTPORT\",\"EVENTACTION\",\"ACTIONRESULT\",\"REPORTNETYPE\",\"EVENTDEFID\",\"EVENTNAME\",\"EVENTLEVEL\",\"ORGID\",\"APPPROTO\",\"URL\",\"GETPARAMETER\",\"INFOID\",\"AFFECTEDSYSTEM\",\"ATTACKMETHOD\",\"APPID\",\"VICITIMTYPE\",\"ATTACKFLAG\",\"ATTACKER\",\"VICTIM\",\"HOST\",\"FILEMD5\",\"FILEDIR\",\"REFERER\",\"REQUESTMETHOD\") "
//          sql += " values ('"+rowkey+"','"+mlogs(1).toString+"','"+mlogs(2).toString+"','"+mlogs(3).toString+"','"+mlogs(4).toString+"','"+mlogs(5).toString+"','"+mlogs(6).toString+"','"+mlogs(7).toString+"','"+mlogs(8).toString+"','"+mlogs(9).toString+"','"+mlogs(10).toString+"','"+mlogs(11).toString+"','"+mlogs(12).toString+"','"+mlogs(13).toString+"',"+mlogs(14).toInt+",'"+mlogs(15).toString +"','"+mlogs(16).toString+"','"+mlogs(17).toString+",'"+mlogs(18).toString+"','"+mlogs(24).toString+"','"+mlogs(25).toString+"','"+mlogs(26).toString+"','"+mlogs(27).toString+"','"+mlogs(28).toString+"','"+mlogs(29).toInt+"','"+mlogs(30).toString+"','"+mlogs(31).toString+"','"+mlogs(32).toInt+"','"+mlogs(33).toString+"','"+mlogs(34).toString+"','"+mlogs(35).toInt+"','"+mlogs(36).toString +"')"
//          stmt.addBatch(sql)

          var logJson:JSONObject = new JSONObject();
          logJson.put("ROW",rowkey)
          logJson.put("RECORDID",mlogs(1).toString)
          logJson.put("FIRSTRECVTIME",DateUtils.dateToStamp(mlogs(2).toString.trim).toLong )
          logJson.put("REPORTAPP",mlogs(3).toString)
          logJson.put("REPORTIP",mlogs(4).toString)
          logJson.put("SOURCEIP",mlogs(5).toString)
          logJson.put("SOURCEPORT",mlogs(6).toString)
          logJson.put("DESTIP",mlogs(7).toString)
          logJson.put("DESTPORT",mlogs(8).toString)
          logJson.put("EVENTACTION",mlogs(9).toString)
          logJson.put("ACTIONRESULT",mlogs(10).toString)
          logJson.put("REPORTNETYPE",mlogs(11).toString)
          logJson.put("EVENTDEFID",mlogs(12).toString)
          logJson.put("EVENTNAME",mlogs(13).toString)
          if(!mlogs(13).toString.trim.equals("")){
            logJson.put("EVENTNAME2",UnicodeUtils.string2Unicode(mlogs(13).toString).replace("\\u","socos"))
          }else{
            logJson.put("EVENTNAME2","")
          }
          logJson.put("EVENTLEVEL",mlogs(14).toString)
          logJson.put("ORGID",mlogs(15).toString)
          logJson.put("APPPROTO",mlogs(16).toString)
          logJson.put("URL",mlogs(17).toString)
          logJson.put("GETPARAMETER",mlogs(18).toString)
          logJson.put("ORGLOG",mlogs(22).toString)
          logJson.put("LOGRULEID",mlogs(23).toString)

          logJson.put("INFOID",mlogs(24).toString)
          logJson.put("AFFECTEDSYSTEM",mlogs(25).toString)
          logJson.put("ATTACKMETHOD",mlogs(26).toString)
          logJson.put("APPID",mlogs(27).toString)
          logJson.put("VICTIMTYPE",mlogs(28).toString)
          logJson.put("ATTACKFLAG",mlogs(29).toString)
          logJson.put("ATTACKER",mlogs(30).toString)
          logJson.put("VICTIM",mlogs(31).toString)
          logJson.put("HOST",mlogs(32).toString)
          logJson.put("FILEMD5",mlogs(33).toString)
          logJson.put("FILEDIR",mlogs(34).toString)
          logJson.put("REFERER",mlogs(35).toString)
          logJson.put("REQUESTMETHOD",mlogs(36).toString)

          var keySet = logJson.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), logJson.get(key));
          }
          logJson = tempjson

          tempLogs.add(logJson.toString);
        }
      })

      val indexName = "genlog"
      val typeName = "genlog"

      IndexUtils.batchIndexData(client,indexName,typeName,tempLogs)
    }
  }

  /**
    * 批量范式化日志入库并且上传kafka
    *
    * @param logs
    * @param stmt
    */
  def batchSaveMatchLogToKafka(logs:List[String],stmt:Statement,client:Client,kafkalistURL:String):Unit = {
    if(logs != null && logs.size>0){
      val tempLogs:java.util.List[String] = new java.util.ArrayList()
      val dbUtils = new DBUtils
      val kafkaproducer = dbUtils.getKafkaProducer(kafkalistURL)
      logs.foreach(log =>{
        if(log != null && log.contains("~")){
          val mlogs = log.toString.split("~")
          val rowkey = ROWUtils.genaralROW()
          var sql:String = "upsert into T_SIEM_GENERAL_LOG "
          sql += "(\"ROW\",\"RECORDID\",\"FIRSTRECVTIME\",\"REPORTAPP\",\"REPORTIP\",\"SOURCEIP\",\"SOURCEPORT\",\"DESTIP\",\"DESTPORT\",\"EVENTACTION\",\"ACTIONRESULT\",\"REPORTNETYPE\",\"EVENTDEFID\",\"EVENTNAME\",\"EVENTLEVEL\",\"ORGID\",\"APPPROTO\",\"URL\",\"GETPARAMETER\") "
          sql += " values ('"+rowkey+"','"+mlogs(1).toString+"','"+DateUtils.stampToDate(mlogs(2).toString)+"','"+mlogs(3).toString+"','"+mlogs(4).toString+"','"+mlogs(5).toString+"','"+mlogs(6).toString+"','"+mlogs(7).toString+"','"+mlogs(8).toString+"','"+mlogs(9).toString+"','"+mlogs(10).toString+"','"+mlogs(11).toString+"','"+mlogs(12).toString+"','"+mlogs(13).toString+"',"+mlogs(14).toInt+",'"+mlogs(15).toString +"','"+mlogs(16).toString+"','"+mlogs(17).toString+"','"+mlogs(18).toString +"')"
          stmt.addBatch(sql)

          var logJson:JSONObject = new JSONObject();
          logJson.put("ROW",rowkey)
          logJson.put("RECORDID",mlogs(1).toString)
          logJson.put("FIRSTRECVTIME",mlogs(2).toString )
          logJson.put("REPORTAPP",mlogs(3).toString)
          logJson.put("REPORTIP",mlogs(4).toString)
          logJson.put("SOURCEIP",mlogs(5).toString)
          logJson.put("SOURCEPORT",mlogs(6).toString)
          logJson.put("DESTIP",mlogs(7).toString)
          logJson.put("DESTPORT",mlogs(8).toString)
          logJson.put("EVENTACTION",mlogs(9).toString)
          logJson.put("ACTIONRESULT",mlogs(10).toString)
          logJson.put("REPORTNETYPE",mlogs(11).toString)
          logJson.put("EVENTDEFID",mlogs(12).toString)
          logJson.put("EVENTNAME",mlogs(13).toString)
          if(!mlogs(13).toString.trim.equals("")){
            logJson.put("EVENTNAME2",UnicodeUtils.string2Unicode(mlogs(13).toString).replace("\\u","socos"))
          }else{
            logJson.put("EVENTNAME2","")
          }
          logJson.put("EVENTLEVEL",mlogs(14).toString)
          logJson.put("ORGID",mlogs(15).toString)
          logJson.put("APPPROTO",mlogs(16).toString)
          logJson.put("URL",mlogs(17).toString)
          logJson.put("GETPARAMETER",mlogs(18).toString)
          logJson.put("ORGLOG",mlogs(22).toString)
          logJson.put("LOGRULEID",mlogs(23).toString)

          var keySet = logJson.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), logJson.get(key));
          }
          logJson = tempjson

          tempLogs.add(logJson.toString);
          dbUtils.sendKafkaList("genlog-topic",log,kafkaproducer)
        }
      })

      val indexName = "genlog"
      val typeName = "genlog"

      IndexUtils.batchIndexData(client,indexName,typeName,tempLogs)
      kafkaproducer.close()
    }
  }

  /**
    * 批量范式化日志(上网行为)入库
    *
    * @param logs
    */
  def batchSaveMatchLogUser(logs:List[String],client:Client):Unit = {
    if(logs != null && logs.size>0){
      val tempLogs:java.util.List[String] = new java.util.ArrayList()
      logs.foreach(log =>{
        if(log != null && log.contains("~") && log.toString.split("~").length>=15){
          val mlogs = log.toString.split("~")
          val rowkey = ROWUtils.genaralROW()

          var logJson:JSONObject = new JSONObject();
          logJson.put("ROW",rowkey)
          logJson.put("RECORDID",mlogs(1).toString)
          logJson.put("FIRSTRECVTIME",mlogs(2).toString )
          logJson.put("REPORTAPP",mlogs(3).toString)
          logJson.put("REPORTIP",mlogs(4).toString)
          logJson.put("SOURCEIP",mlogs(5).toString)
          logJson.put("SOURCEPORT",mlogs(6).toString)
          logJson.put("DESTIP",mlogs(7).toString)
          logJson.put("DESTPORT",mlogs(8).toString)
          logJson.put("EVENTACTION",mlogs(9).toString)
          logJson.put("ACTIONRESULT",mlogs(10).toString)
          logJson.put("REPORTNETYPE",mlogs(11).toString)
          logJson.put("EVENTDEFID",mlogs(12).toString)
          logJson.put("EVENTNAME",mlogs(13).toString)
          if(!mlogs(13).toString.trim.equals("")){
            logJson.put("EVENTNAME2",UnicodeUtils.string2Unicode(mlogs(13).toString).replace("\\u","socos"))
          }else{
            logJson.put("EVENTNAME2","")
          }
          logJson.put("EVENTLEVEL",mlogs(14).toString)
          logJson.put("ORGID",mlogs(15).toString)
          logJson.put("APPPROTO",mlogs(16).toString)
          logJson.put("URL",mlogs(17).toString)
          logJson.put("GETPARAMETER",mlogs(18).toString)
          logJson.put("ORGLOG",mlogs(22).toString)
          logJson.put("LOGRULEID",mlogs(23).toString)

          var keySet = logJson.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), logJson.get(key));
          }
          logJson = tempjson

          tempLogs.add(logJson.toString);
        }
      })

      val indexName = "flowlog"
      val typeName = "flowlog"

      IndexUtils.batchIndexData(client,indexName,typeName,tempLogs)
    }
  }
}
