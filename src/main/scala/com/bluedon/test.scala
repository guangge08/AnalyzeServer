package com.bluedon

import java.sql.Statement
import java.util.UUID
import java.util.regex.Matcher

import com.bluedon.dataMatch.{EventMatch, LogMatch}
import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import com.bluedon.utils.{DateUtils, ROWUtils, UnicodeUtils}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.client.Client
import redis.clients.jedis.Jedis

import scala.util.control.Breaks.{break, breakable}

/**
  * Created by huoguang on 2017/8/9.
  */
object test extends App {
//  val groupid = "console-consumer-42018"
//  print(Math.abs(groupid.hashCode)%50)


//  val host = s"172.16.10.70"
//  val port = 6379
//  val jedis = new Jedis(host, port)
//  jedis.auth("123456")
//  val regexSystem = jedis.get("eventDefBySystem")
//  println(regexSystem)
  val olog = s"111111111111~2017-08-14 15:23:38~172.16.12.144~user~notice~sysl0gcl1ent~sysl0gcl1ent[1755]: ips_alert|!17550|!SQL注入攻击|!1502182632|!SQL注入|!123.65.71.86|!16029|!172.16.26.9|!80|!3|!|!操作系统:,应用程序:,主机:server|!6|!远程|!77|!server|!true|!123.65.71.86|!172.16.26.9"
//  val logstr = s"sysl0gcl1ent[1755]: ips_alert|!17550|!SQL注入攻击|!1502179603|!SQL注入|!10.151.138.76|!51360|!121.8.98.110|!80|!3|!|!操作系统:,应用程序:,主机:server|!6|!远程|!77|!server|!true|!10.151.138.76|!121.8.98.110"
  val regex = s".*ips_alert\\|!.*\\|!(?<eventName>.*)\\|!(?<openTime>.*)\\|!(?<attackType>.*)\\|!(?<sourceIp>.*)\\|!(?<sourcePort>.*)\\|!(?<destIp>.*)\\|!(?<destPort>.*)\\|!(?<eventLevel>.*)\\|!(?<infoId>.*)\\|!(?<affectedSystem>.*)\\|!(?<proto>.*)\\|!(?<attackMethod>.*)\\|!(?<appId>.*)\\|!(?<victimType>.*)\\|!(?<attackFlag>.*)\\|!(?<attacker>.*)\\|!(?<victim>.*)"

  //  val matchRegexStr = regex.trim.r
//  val matchList = matchRegexStr.pattern.matcher(logstr)

  val  genlog = matchLog(olog, regex)
  println(s"输入日志: " + olog)
  println(s"匹配正则: " + regex)
  genlog.split("~").foreach(x => {
    println(s"字段: " + x)
  })
  println(s"范式化结果字段总数: " + genlog.split("~").length)

  val matchlog = genlog

    val host = s"172.16.10.70"
    val port = 6379
    val jedis = new Jedis(host, port)
    jedis.auth("123456")

  val ruleArray: JSONArray = getDataFrame(jedis, "eventDefBySystem")

  val eventMatch = new EventMatch
  val event: String = eventMatch.eventMatchBySystem(matchlog,ruleArray)

  println(s"系统事件: " + event)
  println(s"系统事件字段数: " + event.split("#####")(0).split("~").length)

  val cls = new LogMatch()
  var client: Client = ESClient.esClient()
  val logs: List[String] = List(matchlog)
  val events: List[String] = List(event)
  batchSaveMatchLog(logs, client)

  batchSaveSinalMatchEvent(events, client)


  def batchSaveSinalMatchEvent(events:List[String],client:Client):Unit = {
    if(events != null && events.size>0) {
      val tempevents: java.util.List[String] = new java.util.ArrayList()
      events.foreach(event => {
        if(event.toString.contains("#####")){
          val matchEventResult = event.toString.split("#####")(0).split("~")
          var eventruleid = matchEventResult(2).toString
          if(eventruleid != null){
            eventruleid = eventruleid.trim.toLowerCase
          }
          val rowkey = ROWUtils.genaralROW()
          var jsonEvent = new JSONObject()
          jsonEvent.put("ROW",rowkey)
          jsonEvent.put("EVENT_RULE_RESULT_ID",matchEventResult(1).toString)
          jsonEvent.put("EVENT_RULE_ID",eventruleid)
          jsonEvent.put("LOGID",matchEventResult(3).toString)
          jsonEvent.put("EVENT_RULE_NAME",matchEventResult(4).toString)
          jsonEvent.put("EVENT_RULE_LEVEL",matchEventResult(5).toString)
          jsonEvent.put("REPORTNEIP",matchEventResult(6).toString)
          jsonEvent.put("SOURCEIP",matchEventResult(7).toString)
          jsonEvent.put("SOURCEPORT",matchEventResult(8).toString)
          jsonEvent.put("DESTIP",matchEventResult(9).toString)
          jsonEvent.put("DESTPORT",matchEventResult(10).toString)
          jsonEvent.put("OPENTIME",DateUtils.dateToStamp(matchEventResult(11).toString.trim).toLong)
          jsonEvent.put("ACTIONOPT",matchEventResult(12).toString)
          jsonEvent.put("IS_INNER",matchEventResult(13).toString)
          jsonEvent.put("PROTO",matchEventResult(14).toString)
          //2017-08-10 新增
          jsonEvent.put("INFOID",matchEventResult(15).toString)
          jsonEvent.put("AFFECTEDSYSTEM",matchEventResult(16).toString)
          jsonEvent.put("ATTACKMETHOD",matchEventResult(17).toString)
          jsonEvent.put("APPID",matchEventResult(18).toString)
          jsonEvent.put("VICTIMTYPE",matchEventResult(19).toString)
          jsonEvent.put("ATTACKFLAG",matchEventResult(20).toString)
          jsonEvent.put("ATTACKER",matchEventResult(21).toString)
          jsonEvent.put("VICTIM",matchEventResult(22).toString)
          jsonEvent.put("HOST",matchEventResult(23).toString)
          jsonEvent.put("FILEMD5",matchEventResult(24).toString)
          jsonEvent.put("FILEDIR",matchEventResult(25).toString)
          jsonEvent.put("REFERER",matchEventResult(26).toString)
          jsonEvent.put("REQUESTMETHOD",matchEventResult(27).toString)

          var keySet = jsonEvent.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), jsonEvent.get(key));
          }
          jsonEvent = tempjson

          tempevents.add(jsonEvent.toString);
        }
      })

      val indexName = "huoguang1"
      val typeName = "huoguang1"
      IndexUtils.batchIndexData(client,indexName,typeName,tempevents)
    }
  }

  def batchSaveMatchLog(logs:List[String], client:Client):Unit = {
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

      val indexName = "huoguang"
      val typeName = "huoguang"

      IndexUtils.batchIndexData(client,indexName,typeName,tempLogs)
      println(s"Finish")
    }
  }



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

  def getDataFrame(jedis: Jedis,ruleName:String): JSONArray ={

    var res = jedis.get(ruleName)
    //    res = res.replace("\\","\\\\")
    val ruleJson = JSONArray.fromObject(res)
    val newRuleJson = new JSONArray()

    for(i<- 0 to ruleJson.size()-1){
      val json:JSONObject = ruleJson.get(i).asInstanceOf[JSONObject]
      val newJson = new JSONObject()
      val it = json.keys();
      while (it.hasNext()){
        val key = it.next()
        //        newJson.put(key,json.get(key).toString.replace("\\\\","\\"))
        newJson.put(key,json.get(key).toString)
      }
      newRuleJson.add(newJson)
    }

    newRuleJson
  }

  def matchLog(logs:String,regex:String):String = {
  var matchStr = ""

            val lo = logs.split("~")
            val logStr = lo(6)
    val reportip = lo(2)
            val log_rule_id = s"V360_NskyEye_MV1.0_001_001"
            val matchRegexStr = regex.trim.r
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
              matchStr = recordid + "~" + recordid + "~" + lo(1) + "~" + lo(5) + "~" + lo(2)

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
              val reportnetype =getMatchValue(matchList,"reportnetype")
              if(StringUtils.isNotEmpty(reportnetype)) {
                matchStr += "~" + reportnetype.toString
              } else {
                matchStr += "~" + " "
              }

              //事件类型
              val eventdefid =getMatchValue(matchList,"eventdefid")
              if(StringUtils.isNotEmpty(eventdefid)) {
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
              val eventlevel = getMatchValue(matchList,"eventlevel")
              if(StringUtils.isNotEmpty(eventlevel)) {
                val eventlevel = getMatchValue(matchList,"eventlevel")
                if(StringUtils.isNotEmpty(eventlevel)){
                  matchStr += "~" + eventlevel
                }else{
                  matchStr += "~1"
                }
              } else {
                matchStr += "~" + "1"
              }

              //原始日志
              matchStr += "~" + logStr

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
              val eventid = getMatchValue(matchList,"eventid")
              if(StringUtils.isNotEmpty(eventid)) {
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
    //不匹配返回空
    matchStr
  }


}
