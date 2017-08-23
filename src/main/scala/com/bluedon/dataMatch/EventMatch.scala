package com.bluedon.dataMatch

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util.UUID

import com.bluedon.entity.TSiemGeneralLog
import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import com.bluedon.utils.{DateUtils, ROWUtils}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import org.elasticsearch.client.Client
import redis.clients.jedis.Jedis

/**
  * Created by dengxiwen on 2016/12/27.
  */
class EventMatch {

  /**
    * 新获取的规则,保存到redis中
    *
    * @param newEventDefId 新生成该规则id
    * @param matchlogs 原始日志范式化后的结果
    * @param eventDefRule 将要添加的规则集合
    * @param jedis
    */
  def saveEventRuleToRedis(newEventDefId: String, matchlogs: Array[String], eventDefRule: JSONArray, jedis: Jedis) = {

    val ruleName = "eventDefBySystem"
    var eventName = matchlogs(13).trim
    if(StringUtils.isEmpty(eventName)){
      eventName = ""
    }
    var eventLevel = matchlogs(14).trim
    if(StringUtils.isEmpty(eventLevel)){
      eventLevel = ""
    }
    val is_inner = "1"
    val is_use = "1"

    val jo = new JSONObject()
    jo.put("event_rule_id",newEventDefId)
    jo.put("event_rule_lib_id","")
    jo.put("event_rule_code","")
    jo.put("event_rule_name",eventName)
    jo.put("event_rule_level",eventLevel)
    jo.put("event_basic_type","")
    jo.put("event_sub_type","")
    jo.put("event_rule_desc","")
    jo.put("event_rule","")
    jo.put("is_inner",is_inner)
    jo.put("is_use",is_use)
    eventDefRule.add(jo.toString)
    jedis.set(ruleName,eventDefRule.toString)
  }

  /**
    * 拼装事件结果
    *
    * @param eventRuleResultId
    * @param eventDefId
    * @param eventDefName
    * @param eventRuleLevel
    * @param matchlog
    * @return
    */
  def getEventRuleResult(eventRuleResultId: String, eventDefId: String, eventDefName: String,
                         eventRuleLevel: String,matchlog:String):String = {
    var matchEvent:String = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")) {
        val matchlogs = matchlog.split("~")
        val eventtype = matchlogs(3).toString
        matchEvent = eventRuleResultId + "~" + eventRuleResultId + "~" + eventDefId + "~" + matchlogs(0).toString + "~" + eventDefName
        matchEvent += "~" + eventRuleLevel + "~" + matchlogs(4).toString + "~" + matchlogs(5).toString + "~" + matchlogs(6).toString
        matchEvent += "~" + matchlogs(7).toString + "~" + matchlogs(8).toString + "~" + matchlogs(2).toString + "~" + matchlogs(10).toString
        if (eventtype != null && eventtype.equals("NE_FLOW")) {
          //南基自定义规则判断，如是南基的自定义规则，则事件结果表的是否内置值为内置
          matchEvent += "~" + "0" + "~" + matchlogs(3).toString
        } else {
          matchEvent += "~" + "1" + "~" + matchlogs(3).toString
        }
        matchEvent += "~" + matchlogs(24) + "~" + matchlogs(25)+ "~" + matchlogs(26)+ "~" + matchlogs(27)+ "~" + matchlogs(28)+ "~" + matchlogs(29)
        matchEvent += "~" + matchlogs(30) + "~" + matchlogs(31)+ "~" + matchlogs(32)+ "~" + matchlogs(33)+ "~" + matchlogs(34)+ "~" + matchlogs(35)+ "~" + matchlogs(36)
        matchEvent += "#####" + matchlog
    }
    matchEvent
  }

  /**
    * 当事件name没有的时候,从范式化后的日志中获取规则相关信息,保存到redis中并返回结果
    *
    * @param matchlog
    * @param eventDefRule
    * @param jedis
    * @return
    */
  def dealEventRule(matchlog: String, eventDefRule: JSONArray,newEventDefId:String,jedis: Jedis): String = {
    var matchEvent = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")){
      val matchlogs = matchlog.split("~")

      //保存到redis
      saveEventRuleToRedis(newEventDefId,matchlogs,eventDefRule,jedis)
      //新结果集id,这个id对应的数据发送到kafka与保存到hbase或ES中
      val newEventRuleResultId = UUID.randomUUID().toString.replaceAll("-","")
      val eventDefName = matchlogs(13)
      val eventRuleLevel = matchlogs(14)
      matchEvent = getEventRuleResult(newEventRuleResultId,newEventDefId,eventDefName,eventRuleLevel,matchlog)
    }
    matchEvent
  }


  /**
    * 单事件匹配--内置
    *
    * @param matchlog 范式化日志
    * @param eventDefRule 事件定义信息
    * @return
    */
  def eventMatchBySystem(matchlog:String,eventDefRule:JSONArray):String = {
    var matchEvent:String = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")){
      val matchlogs = matchlog.split("~")
      val eventdefname = matchlogs(13).toString

      for(i<- 0 to eventDefRule.size() -1){
        val eventDef:JSONObject = eventDefRule.get(i).asInstanceOf[JSONObject]
        val eventDefId = eventDef.getString("event_rule_id")     //事件定义ID
        val eventDefName = eventDef.getString("event_rule_name")   //事件名称
        val eventRuleLevel = eventDef.getString("event_rule_level")   //事件级别
        val eventBasicType = eventDef.getString("event_basic_type")   //事件基类
        val eventSubType = eventDef.getString("event_sub_type")   //事件子类
        val filedBeginValue = eventDef.getString("filed_begin_value")   //事件规则值
        val matchMethod = eventDef.getString("match_method")   //匹配方式

        //"ROW","recordid","firstrecvtime","reportapp","reportip","sourceip","sourceport","destip","destport","eventaction","actionresult","reportnetype","eventdefid","eventname","eventlevel","orgid"
        //"ROW","eventRuleResultId","eventRuleId","logid","eventRuleName","eventRuleLevel","reportneip","sourceip","sourceport","destip","destport","opentime","actionopt","dublecount","isInner","proto"
        //if(eventdefid.trim.eq(eventDefId.trim)){
        if(eventdefname.trim.equalsIgnoreCase(filedBeginValue.trim)){
          val eventRuleResultId = UUID.randomUUID().toString.replaceAll("-","")
          matchEvent = getEventRuleResult(eventRuleResultId,eventDefId,eventDefName,eventRuleLevel,matchlog)
          return matchEvent
        }
      }
    }
    matchEvent
  }

  /**
    * 单事件匹配--内置(以事件ID来匹配）
    *
    * @param matchlog 范式化日志
    * @return
    */
  def eventMatchBySystemForId(matchlog:String):String = {
    var matchEvent:String = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")){
      val matchlogs = matchlog.split("~")
      val eventdefid = matchlogs(12).toString
      val eventdefname = matchlogs(13).toString
      val eventlevel = matchlogs(14).toString
      val eventtype = matchlogs(3).toString
      val eventid = matchlogs(21).toString

      val eventRuleResultId = UUID.randomUUID().toString.replaceAll("-","")
      matchEvent = eventRuleResultId + "~" + eventRuleResultId  + "~" + eventid  + "~" + matchlogs(0).toString + "~" + eventdefname
      matchEvent += "~" + eventlevel + "~" + matchlogs(4).toString + "~" + matchlogs(5).toString + "~" + matchlogs(6).toString
      if(eventtype != null && eventtype.equals("NE_FLOW")){ //南基自定义规则判断，如是南基的自定义规则，则事件结果表的是否内置值为内置
        matchEvent += "~" + matchlogs(7).toString + "~" + matchlogs(8).toString + "~" + matchlogs(2).toString + "~" + matchlogs(10).toString  + "~" + "0" + "~" + matchlogs(3).toString
      }else{
        matchEvent += "~" + matchlogs(7).toString + "~" + matchlogs(8).toString + "~" + matchlogs(2).toString + "~" + matchlogs(10).toString  + "~" + "1" + "~" + matchlogs(3).toString
      }
      matchEvent += "#####" + matchlog
    }
    matchEvent
  }

  /**
    * 单事件匹配--用户自定义
    *
    * @param matchlog 范式化日志
    * @param eventDefRule 事件定义信息
    * @param eventFieldRule 事件规则
    * @return
    */
  def eventMatchByUser(matchlog:String,eventDefRule:JSONArray,eventFieldRule:JSONArray):String = {
    var matchEvent:String = ""
    if(matchlog != null && !matchlog.trim.eq("") && matchlog.contains("~")){
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val matchlogs = matchlog.split("~")
      val eventdefid = matchlogs(12).toString

      for(i<- 0 to eventDefRule.size() -1){
        val eventDef:JSONObject = eventDefRule.get(i).asInstanceOf[JSONObject]
        val eventDefId = eventDef.getString("event_rule_id")     //事件定义ID
        val eventDefName = eventDef.getString("event_rule_name")   //事件名称
        val eventRuleLevel = eventDef.getString("event_rule_level")   //事件级别
        //"ROW","recordid","firstrecvtime","reportapp","reportip","sourceip","sourceport","destip","destport","eventaction","actionresult","reportnetype","eventdefid","eventname","eventlevel","orgid"
        //"ROW","eventRuleResultId","eventRuleId","logid","eventRuleName","eventRuleLevel","reportneip","sourceip","sourceport","destip","destport","opentime","actionopt","dublecount","isInner","proto"
        var isMatch = true
        val tSiemGeneralLog = new TSiemGeneralLog
        tSiemGeneralLog.setRecordid(matchlogs(0).toString)
        tSiemGeneralLog.setFirstrecvtime(format.parse(matchlogs(2).toString));
        tSiemGeneralLog.setReportapp(matchlogs(3).toString)
        tSiemGeneralLog.setReportip(matchlogs(4).toString)
        tSiemGeneralLog.setSourceip(matchlogs(5).toString)
        tSiemGeneralLog.setSourceport(matchlogs(6).toString)
        tSiemGeneralLog.setDestip(matchlogs(7).toString)
        tSiemGeneralLog.setDestport(matchlogs(8).toString)
        tSiemGeneralLog.setEventaction(matchlogs(9).toString)
        tSiemGeneralLog.setEventname(matchlogs(13).toString)
        tSiemGeneralLog.setEventlevel(matchlogs(14).toInt)

        breakable{
          for(j<- 0 to eventFieldRule.size()-1){
            val eventField:JSONObject = eventFieldRule.get(j).asInstanceOf[JSONObject]
            if(eventField.getString("event_rule_id").eq(eventDefId.trim)){
              val eventRuleField = eventField.getString("event_rule_field")//匹配字段
              val matchMethod = eventField.getString("match_method") //匹配 方式
              val filedBeginValue = eventField.getString("field_begin_value")//匹配规则的开始值
              val filedEndValue = eventField.getString("field_end_value")//匹配规则的结束值
              if(eventRuleField != null && eventRuleField.trim.length()>1){
                val eventRuleFieldMatch = eventRuleField.substring(0, 1).toUpperCase() + eventRuleField.substring(1)
                val eventRuleFieldValue = tSiemGeneralLog.getClass().getMethod("get"+eventRuleField).invoke(tSiemGeneralLog).toString
                matchMethod match {
                  //等于
                  case "1" => {
                    if(!filedBeginValue.equalsIgnoreCase(eventRuleFieldValue)){
                      isMatch = false;
                    }
                  }
                  //不等于
                  case "2" => {
                    if (filedBeginValue.equalsIgnoreCase(eventRuleFieldValue)) {
                      isMatch = false;
                    }
                  }
                  //大于
                  case "3" => {

                    if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) <= 0) {
                      isMatch = false;
                    }
                  }
                  //小于
                  case "4" => {
                    if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) >= 0) {
                      isMatch = false;
                    }
                  }
                  //大于等于
                  case "5" => {
                    if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) < 0) {
                      isMatch = false;
                    }
                  }
                  //小于等于
                  case "6" => {
                    if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) > 0) {
                      isMatch = false;
                    }
                  }
                  //大于 小于
                  case "7" => {
                  }
                  //在() 中
                  case "8" => {
                  }
                  //包含
                  case "9" => {
                    if (!filedBeginValue.contains(eventRuleFieldValue)) {
                      isMatch = false;
                    }
                  }
                  case _ => isMatch = false
                }
              }else{
                isMatch = false
              }
              if(!isMatch){//若其中一项不匹配直接跳出
                break()
              }
            }
          }
        }

        //若事件匹配成功，则写入SQL文件
        if(isMatch) {
          val eventRuleResultId = UUID.randomUUID().toString.replaceAll("-","")
          matchEvent += eventRuleResultId + "~" + eventRuleResultId  + "~" + eventDefId  + "~" + matchlogs(0).toString + "~" + eventDefName
          matchEvent += "~" + eventRuleLevel + "~" + matchlogs(4).toString + "~" + matchlogs(5).toString + "~" + matchlogs(6).toString
          matchEvent += "~" + matchlogs(7).toString + "~" + matchlogs(8).toString + "~" + matchlogs(2).toString + "~" + matchlogs(10).toString  + "~0" + "~" + matchlogs(3).toString
          matchEvent += "#####" + matchlog
          return matchEvent
        }
      }
    }
    matchEvent
  }

  def saveSinalMatchEvent(event:String,phoenixConn:Connection):Unit = {
    if(event.toString.contains("#####")){
      val matchEventResult = event.toString.split("#####")(0).split("~")
      val rowkey = ROWUtils.genaralROW()
      var eventruleid = matchEventResult(2).toString
      if(eventruleid != null){
        eventruleid = eventruleid.trim.toLowerCase
      }
      var sql:String = "upsert into T_SIEM_EVENT_RESULT "
      sql += "(\"ROW\",\"EVENT_RULE_RESULT_ID\",\"EVENT_RULE_ID\",\"LOGID\",\"EVENT_RULE_NAME\",\"EVENT_RULE_LEVEL\",\"REPORTNEIP\",\"SOURCEIP\",\"SOURCEPORT\",\"DESTIP\",\"DESTPORT\",\"OPENTIME\",\"ACTIONOPT\",\"IS_INNER\",\"PROTO\") "
      sql += " values ('"+rowkey+"','"+matchEventResult(1).toString +"','"+eventruleid +"','"+matchEventResult(3).toString +"','"+matchEventResult(4).toString +"','"+matchEventResult(5).toString +"','"+matchEventResult(6).toString +"','"+matchEventResult(7).toString +"','"+matchEventResult(8).toString +"','"+matchEventResult(9).toString +"','"+matchEventResult(10).toString +"','"+matchEventResult(11).toString +"','"+matchEventResult(12).toString +"',"+matchEventResult(13).toInt+",'"+matchEventResult(14).toString+"')"
      val st = phoenixConn.createStatement()
      st.execute(sql)
      phoenixConn.commit()
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
      jsonEvent.put("OPENTIME",DateUtils.dateToStamp(matchEventResult(11).toString).toLong)
      jsonEvent.put("ACTIONOPT",matchEventResult(12).toString)
      jsonEvent.put("IS_INNER",matchEventResult(13).toString)
      jsonEvent.put("PROTO",matchEventResult(14).toString)

      var keySet = jsonEvent.keys()
      var tempjson:JSONObject = new JSONObject();
      while (keySet.hasNext){
        var key:String = keySet.next().asInstanceOf[String];
        tempjson.put(key.toLowerCase(), jsonEvent.get(key));
      }
      jsonEvent = tempjson

      /*
      var enventjson = "{"
      enventjson += "\"ROW\":\""+rowkey + "\","
      enventjson += "\"EVENT_RULE_RESULT_ID\":\""+matchEventResult(1).toString + "\","
      enventjson += "\"EVENT_RULE_ID\":\""+matchEventResult(2).toString + "\","
      enventjson += "\"LOGID\":\""+matchEventResult(3).toString + "\","
      enventjson += "\"EVENT_RULE_NAME\":\""+matchEventResult(4).toString + "\","
      enventjson += "\"EVENT_RULE_LEVEL\":\""+matchEventResult(5).toString + "\","
      enventjson += "\"REPORTNEIP\":\""+matchEventResult(6).toString + "\","
      enventjson += "\"SOURCEIP\":\""+matchEventResult(7).toString + "\","
      enventjson += "\"SOURCEPORT\":\""+matchEventResult(8).toString + "\","
      enventjson += "\"DESTIP\":\""+matchEventResult(9).toString + "\","
      enventjson += "\"DESTPORT\":\""+matchEventResult(10).toString + "\","
      enventjson += "\"OPENTIME\":\""+DateUtils.dateToStamp(matchEventResult(11).toString) + "\","
      enventjson += "\"ACTIONOPT\":\""+matchEventResult(12).toString + "\","
      enventjson += "\"IS_INNER\":\""+matchEventResult(13).toString + "\","
      enventjson += "\"PROTO\":\""+matchEventResult(14).toString + "\""
      enventjson += "}"
      */
      val client = ESClient.esClient()

      val indexName = "event"
      val typeName = "event"
      IndexUtils.addIndexData(client, indexName, typeName, jsonEvent.toString)
    }
  }

  def batchSaveSinalMatchEvent(events:List[String],stmt:Statement,client:Client):Unit = {
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
//          var sql:String = "upsert into T_SIEM_EVENT_RESULT "
//          sql += "(\"ROW\",\"EVENT_RULE_RESULT_ID\",\"EVENT_RULE_ID\",\"LOGID\",\"EVENT_RULE_NAME\",\"EVENT_RULE_LEVEL\",\"REPORTNEIP\",\"SOURCEIP\",\"SOURCEPORT\",\"DESTIP\",\"DESTPORT\",\"OPENTIME\",\"ACTIONOPT\",\"IS_INNER\",\"PROTO\",\"INFOID\",\"AFFECTEDSYSTEM\",\"ATTACKMETHOD\",\"APPID\",\"VICITIMTYPE\",\"ATTACKFLAG\",\"ATTACKER\",\"VICTIM\",\"HOST\",\"FILEMD5\",\"FILEDIR\",\"REFERER\",\"REQUESTMETHOD\") "
//          sql += " values ('"+rowkey+"','"+matchEventResult(1).toString +"','"+eventruleid +"','"+matchEventResult(3).toString +"','"+matchEventResult(4).toString +"','"+matchEventResult(5).toString +"','"+matchEventResult(6).toString +"','"+matchEventResult(7).toString +"','"+matchEventResult(8).toString +"','"+matchEventResult(9).toString +"','"+matchEventResult(10).toString +"','"+matchEventResult(11).toString +"','"+matchEventResult(12).toString +"',"+matchEventResult(13).toInt+","+matchEventResult(14).toString+","+matchEventResult(15).toString+","+matchEventResult(16).toString+","+matchEventResult(17).toString+","+matchEventResult(18).toString+","+matchEventResult(19).toString+","+matchEventResult(20).toInt+","+matchEventResult(21).toString+","+matchEventResult(22).toString+","+matchEventResult(23).toString+","+matchEventResult(24).toString+","+matchEventResult(25).toString+","+matchEventResult(26).toString+",'"+matchEventResult(27).toString+"')"
//          stmt.addBatch(sql)
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

      val indexName = "event"
      val typeName = "event"
      IndexUtils.batchIndexData(client,indexName,typeName,tempevents)
    }
  }
}
