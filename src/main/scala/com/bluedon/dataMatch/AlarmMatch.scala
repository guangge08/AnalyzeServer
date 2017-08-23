package com.bluedon.dataMatch

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import com.bluedon.utils.{DBUtils, DateUtils, ROWUtils}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.phoenix.spark._
import org.apache.spark.sql._
import org.elasticsearch.client.Client
import redis.clients.jedis.{Jedis, Pipeline}

import scala.util.control.Breaks._

/**
  * Created by dengxiwen on 2016/12/28.
  */
class AlarmMatch {

  /**
    * 单事件告警匹配
    *
    * @param event
    * @param policyWhole
    * @param policyEvent
    * @param phoenixConn
    * @return
    */
  def matchSinalEventAlarm(event:String, policyWhole:JSONArray, policyEvent:JSONArray,phoenixConn:Connection,redisHost:String):String={
    var alarmStr = ""
    if( !event.trim.equals("") && event.contains("#####")){
      val eventObj = event.split("#####")(0).split("~")
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jedis:Jedis = new Jedis(redisHost,6379)
      jedis.auth("123456");
      val pl:Pipeline = jedis.pipelined()

      for(i <- 0 to policyWhole.size()-1){
        val whole:JSONObject = policyWhole.get(i).asInstanceOf[JSONObject]
        if(whole.getString("objtable").equalsIgnoreCase("t_siem_alarm_policy_event")){
          val objid = whole.getString("recordid")
          val objtable = "t_siem_alarm_policy_event"
          val eventname = eventObj(4).toString
          val eventid = eventObj(0).toString
          val eventdefid = eventObj(2).toString   //事件定义ID
          val crttime:Calendar = Calendar.getInstance()

          for(j <- 0 to policyEvent.size()-1){
            val rowevent:JSONObject = policyEvent.get(j).asInstanceOf[JSONObject]
            //判断策略事件是否事件的规则
            if(rowevent.getString("alarm_id").trim.equalsIgnoreCase(whole.getString("recordid").toString) && eventdefid.trim.equalsIgnoreCase(rowevent.getString("event_rule_id").trim)){

              val timelimit = rowevent.getInt("control_time")
              val times = rowevent.getInt("count")
              val id = UUID.randomUUID().toString().replaceAll("-","")
              val loserDate:Calendar = Calendar.getInstance()
              loserDate.setTime(crttime.getTime)
              loserDate.add(Calendar.SECOND,timelimit)
              /*
              var sql:String = "upsert into T_SIEM_ALARM_EVENT "
              sql += "(RECORDID,ALARMPOLICYID,EVENTRESULTID,STORAGETIME,TIMES,LOSERTIME) "
              sql += " values ('" + id+"','"+rowevent.getString(0)+"','"+ eventid.toString+"','"+ format.format(crttime.getTime)+"',"+timelimit+",'"+ format.format(loserDate.getTime) +"')"
              val st = phoenixConn.createStatement()
              st.execute(sql)
              phoenixConn.commit()
              */
              val uuid = UUID.randomUUID().toString.replaceAll("-","")
              val hkey = rowevent.getString("recordid")
              val key  = rowevent.getString("recordid") + "_#_" + uuid
              val value = eventid.toString
              jedis.setex(key,timelimit,value)
              jedis.hsetnx(hkey,key,value)



              //val alarmEventLists = alarmEventCollect(phoenixConn,format.format(crttime.getTime))
              val alarmCount = jedis.hlen(hkey)
              //if(alarmEventLists.size >= times){
              if(alarmCount >= times){
                //recordid,firstalarmtime,alarmcontent,objtable,objid,alarmtimes
                alarmStr = UUID.randomUUID().toString.replaceAll("-","") + "~" + format.format(crttime.getTime) + "~" + format.format(crttime.getTime) + "事件【" + eventname + "】发生告警!"
                alarmStr += "~" + objtable + "~" + objid + "~" + 1
                jedis.hvals(hkey)
                val keySet = jedis.hkeys(hkey).iterator()
                while(keySet.hasNext){
                  val htkey:String = keySet.next()
                  jedis.hdel(hkey,htkey)
                  jedis.del(htkey)
                }

                //deleteAlarmEvent(format.format(crttime.getTime),phoenixConn)
                //deleteAlarmEventByPolicyid(format.format(crttime.getTime),rowevent.getString(0),phoenixConn)
                return alarmStr
              }

            }
          }
        }
      }
    }

    alarmStr
  }

  /**
    * 联合事件告警匹配
    *
    * @param event
    * @param policyWhole
    * @param policyRelationEvent
    * @param phoenixConn
    * @return
    */
  def matchRelationEventAlarm(event:String, policyWhole:JSONArray, policyRelationEvent:JSONArray, phoenixConn:Connection, redisHost:String):String={
    var alarmStr = ""
    if( !event.trim.equals("")){
      val eventObj = event.split("~")
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jedis:Jedis = new Jedis(redisHost,6379)
      jedis.auth("123456");
      val pl:Pipeline = jedis.pipelined()
      for(i <- 0 to policyWhole.size()-1){
        val whole:JSONObject = policyWhole.get(i).asInstanceOf[JSONObject]
        if(whole.getString("objtable").equalsIgnoreCase("t_siem_alarm_policy_relation_ev")){
          val objid = whole.getString("recordid")
          val objtable = "t_siem_alarm_policy_relation_ev"
          val eventname = eventObj(2).toString
          val eventid = eventObj(0).toString.replaceAll("$$$$$","").replaceAll("#####","")
          val eventdefid = eventObj(1).toString   //事件定义ID
          val crttime:Calendar = Calendar.getInstance()

          for(j <- 0 to policyRelationEvent.size()-1){
            val rowevent:JSONObject = policyRelationEvent.get(j).asInstanceOf[JSONObject]
            //判断策略事件是否事件的规则
            if(rowevent.getString("alarm_id").trim.equalsIgnoreCase(whole.getString("recordid").toString) && eventdefid.trim.equalsIgnoreCase(rowevent.getString("event_rule_id").trim)){

              val timelimit = rowevent.getInt("control_time")
              val times = rowevent.getInt("count")
              val id = UUID.randomUUID().toString().replaceAll("-","")
              val loserDate:Calendar = Calendar.getInstance()
              loserDate.setTime(crttime.getTime)
              loserDate.add(Calendar.SECOND,timelimit)
              /*
              var sql:String = "upsert into T_SIEM_ALARM_EVENT "
              sql += "(RECORDID,ALARMPOLICYID,EVENTRESULTID,STORAGETIME,TIMES,LOSERTIME) "
              sql += " values ('" + id+"','"+rowevent.getString(0)+"','"+ eventid.toString+"','"+ format.format(crttime.getTime)+"',"+timelimit+",'"+ format.format(loserDate.getTime) +"')"
              val st = phoenixConn.createStatement()
              st.execute(sql)
              phoenixConn.commit()
              */
              val uuid = UUID.randomUUID().toString.replaceAll("-","")
              val hkey = rowevent.getString("recordid")
              val key  = rowevent.getString("recordid") + "_#_" + uuid
              val value = eventid.toString
              jedis.setex(key,timelimit,value)
              jedis.hsetnx(hkey,key,value)



              //val alarmEventLists = alarmEventCollect(phoenixConn,format.format(crttime.getTime))
              val alarmCount = jedis.hlen(hkey)
              //if(alarmEventLists.size >= times){
              if(alarmCount >= times){
                //recordid,firstalarmtime,alarmcontent,objtable,objid,alarmtimes
                alarmStr = UUID.randomUUID().toString.replaceAll("-","") + "~" + format.format(crttime.getTime) + "~" + format.format(crttime.getTime) + "联合事件【" + eventname + "】发生告警!"
                alarmStr += "~" + objtable + "~" + objid + "~" + 1
                jedis.hvals(hkey)
                val keySet = jedis.hkeys(hkey).iterator()
                while(keySet.hasNext){
                  val htkey:String = keySet.next()
                  jedis.hdel(hkey,htkey)
                  jedis.del(htkey)
                }

                //deleteAlarmEvent(format.format(crttime.getTime),phoenixConn)
                //deleteAlarmEventByPolicyid(format.format(crttime.getTime),rowevent.getString(0),phoenixConn)
                return alarmStr
              }

            }
          }
        }
      }

    }

    alarmStr
  }

  /**
    * 事件告警入库
    *
    * @param event
    * @param phoenixConn
    */
  def saveMatchEventAlarm(event:String,phoenixConn:Connection):Unit = {
    if(event.toString.contains("~")){
      val matchEventAlarms = event.toString.split("~")
      val rowkey = ROWUtils.genaralROW()
      var sql:String = "upsert into T_SIEM_ALARM_ACTIVE "
      sql += "(\"ROW\",\"RECORDID\",\"FIRSTALARMTIME\",\"ALARMCONTENT\",\"OBJTABLE\",\"OBJID\",\"ALARMTIMES\") "
      sql += " values ('"+rowkey+"','" +matchEventAlarms(0).toString+"','"+matchEventAlarms(1).toString+"','"+matchEventAlarms(2).toString+"','"+matchEventAlarms(3).toString+"','"+matchEventAlarms(4).toString+"',"+matchEventAlarms(5).toInt +")"
      val st = phoenixConn.createStatement()
      st.execute(sql)
      phoenixConn.commit()
      var jsonAlarm = new JSONObject()
      jsonAlarm.put("ROW",rowkey);
      jsonAlarm.put("RECORDID",matchEventAlarms(0).toString );
      jsonAlarm.put("FIRSTALARMTIME",DateUtils.dateToStamp(matchEventAlarms(1).toString.trim).toLong);
      jsonAlarm.put("ALARMCONTENT",matchEventAlarms(2).toString);
      jsonAlarm.put("OBJTABLE",matchEventAlarms(3).toString);
      jsonAlarm.put("OBJID",matchEventAlarms(4).toString);
      jsonAlarm.put("ALARMTIMES",matchEventAlarms(5).toInt);

      var keySet = jsonAlarm.keys()
      var tempjson:JSONObject = new JSONObject();
      while (keySet.hasNext){
        var key:String = keySet.next().asInstanceOf[String];
        tempjson.put(key.toLowerCase(), jsonAlarm.get(key));
      }
      jsonAlarm = tempjson
      /*
      var alarmjson = "{"
      alarmjson += "\"ROW\":\""+rowkey + "\","
      alarmjson += "\"RECORDID\":\""+matchEventAlarms(0).toString + "\","
      alarmjson += "\"FIRSTALARMTIME\":\""+matchEventAlarms(1).toString + "\","
      alarmjson += "\"ALARMCONTENT\":\""+matchEventAlarms(2).toString + "\","
      alarmjson += "\"OBJTABLE\":\""+matchEventAlarms(3).toString + "\","
      alarmjson += "\"OBJID\":\""+matchEventAlarms(4).toString + "\","
      alarmjson += "\"ALARMTIMES\":" +matchEventAlarms(5).toInt
      alarmjson += "}"
      */
      val client = ESClient.esClient()

      val indexName = "alarm"
      val typeName = "alarm"
      IndexUtils.addIndexData(client, indexName, typeName, jsonAlarm.toString)


    }
  }

  def batchSaveMatchEventAlarm(alarms:List[String],stmt:Statement,client:Client):Unit = {
    if(alarms != null && alarms.size>0) {
      val tempalarms: java.util.List[String] = new java.util.ArrayList()
      alarms.foreach(event => {
        if(event.toString.contains("~")){
          val matchEventAlarms = event.toString.split("~")
          val rowkey = ROWUtils.genaralROW()
          var sql:String = "upsert into T_SIEM_ALARM_ACTIVE "
          sql += "(\"ROW\",\"RECORDID\",\"FIRSTALARMTIME\",\"ALARMCONTENT\",\"OBJTABLE\",\"OBJID\",\"ALARMTIMES\") "
          sql += " values ('"+rowkey+"','" +matchEventAlarms(0).toString+"','"+matchEventAlarms(1).toString+"','"+matchEventAlarms(2).toString+"','"+matchEventAlarms(3).toString+"','"+matchEventAlarms(4).toString+"',"+matchEventAlarms(5).toInt +")"
          stmt.addBatch(sql)

          var jsonAlarm = new JSONObject()
          jsonAlarm.put("ROW",rowkey);
          jsonAlarm.put("RECORDID",matchEventAlarms(0).toString );
          jsonAlarm.put("FIRSTALARMTIME",DateUtils.dateToStamp(matchEventAlarms(1).toString));
          jsonAlarm.put("ALARMCONTENT",matchEventAlarms(2).toString);
          jsonAlarm.put("OBJTABLE",matchEventAlarms(3).toString);
          jsonAlarm.put("OBJID",matchEventAlarms(4).toString);
          jsonAlarm.put("ALARMTIMES",matchEventAlarms(5).toInt);

          var keySet = jsonAlarm.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), jsonAlarm.get(key));
          }
          jsonAlarm = tempjson
          tempalarms.add(jsonAlarm.toString);
        }
      })

      val indexName = "alarm"
      val typeName = "alarm"
      IndexUtils.batchIndexData(client,indexName,typeName,tempalarms)
    }
  }

  /**
    * 根据失效时间获取暂存事件
    *
    * @param phoenixConn
    * @param losertime
    * @return
    */
  def  alarmEventCollect(phoenixConn:Connection,losertime:String):List[(String,String,String,String,String)] ={
    var list:List[(String,String,String,String,String)] = null
    val sql:String = "select ALARMPOLICYID,EVENTRESULTID,STORAGETIME,TIMES,LOSERTIME " +
      "from T_SIEM_ALARM_EVENT where LOSERTIME>TO_DATE('"+losertime+"')"
    val st = phoenixConn.createStatement()
    val rs = st.executeQuery(sql)
    while (rs.next()){
      if(list == null){
        list = List((rs.getString("ALARMPOLICYID"),rs.getString("EVENTRESULTID"),rs.getString("STORAGETIME"),rs.getString("TIMES"),rs.getString("LOSERTIME")))
      }else{
        list ++= List((rs.getString("ALARMPOLICYID"),rs.getString("EVENTRESULTID"),rs.getString("STORAGETIME"),rs.getString("TIMES"),rs.getString("LOSERTIME")))
      }

    }
    rs.close()
    st.close()
    list
  }

  /**
    * 根据告警策略ID删除暂存事件
    *
    * @param losertime
    * @param policyid
    */
  def  deleteAlarmEventByPolicyid(losertime:String,policyid:String,phoenixConn:Connection):Unit ={
    val sql:String = "delete from T_SIEM_ALARM_EVENT where STORAGETIME>=TO_DATE('"+losertime+"') and ALARMPOLICYID='" + policyid + "'"
    val dbUtils=new DBUtils
    val conn:Connection = phoenixConn
    val st:Statement = conn.createStatement()
    st.execute(sql)
    conn.commit()
  }

  /**
    * 根据失效时间删除暂存事件
    *
    * @param losertime
    */
  def  deleteAlarmEvent(losertime:String,phoenixConn:Connection):Unit ={
    val sql:String = "delete from T_SIEM_ALARM_EVENT where LOSERTIME>TO_DATE('"+losertime+"')"
    val dbUtils=new DBUtils
    val conn:Connection = phoenixConn
    val st:Statement = conn.createStatement()
    st.execute(sql)
    conn.commit()
  }
}
