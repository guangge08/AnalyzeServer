package com.bluedon.dataMatch

import java.lang.Exception
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import com.bluedon.entity.TSiemGeneralLog
import org.apache.spark.sql._
import org.apache.phoenix.spark._
import org.apache.spark.streaming.dstream.DStream

import scala.util.control.Breaks._
import com.bluedon.utils.{DBUtils, DateUtils, ROWUtils}
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util

import com.bluedon.esinterface.config.ESClient
import com.bluedon.esinterface.index.IndexUtils
import net.sf.json.{JSONArray, JSONObject}
import org.elasticsearch.client.Client
import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
/**
  * 关联事件匹配
  * Created by dengxiwen on 2016/12/27.
  */
class RelationEventMatch {

  /**
    * 关联事件匹配
    *
    * @param matchEvent
    * @param ruleMap
    * @param dataMap
    * @return
    */
  def relationEventMatch(matchEvent:String, ruleMap:Map[String,JSONArray],dataMap:Map[String,JSONArray],phoenixConn:Connection,jedis:Jedis):(String,List[String],List[String],List[String]) = {
    //eventmedefine:Array[Row], eventmedefinesub:Array[Row], eventmedetail:Array[Row]
    var gevents:List[String] = List[String]()
    var gsevents:List[String]  = List[String]()
    var sqls:List[String]  = List[String]()
    val eventmedefine:JSONArray = ruleMap("eventmedefine")
    val eventmedefinesub:JSONArray = ruleMap("eventmedefinesub")
    val eventmedetail:JSONArray = ruleMap("eventmedetail")
    //漏洞扫描信息
    val leakscans:JSONArray = dataMap("leakscans")
    var relationEventStr = ""
    if(matchEvent.contains("#####")){
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val eventStr = matchEvent.split("#####")(0).split("~")
      val logStr = matchEvent.split("#####")(1).split("~")
      val tSiemGeneralLog = new TSiemGeneralLog
      tSiemGeneralLog.setRecordid(logStr(0).toString)
      tSiemGeneralLog.setFirstrecvtime(format.parse(logStr(2).toString));
      tSiemGeneralLog.setReportapp(logStr(3).toString)
      tSiemGeneralLog.setReportip(logStr(4).toString)
      tSiemGeneralLog.setSourceip(logStr(5).toString)
      tSiemGeneralLog.setSourceport(logStr(6).toString)
      tSiemGeneralLog.setDestip(logStr(7).toString)
      tSiemGeneralLog.setDestport(logStr(8).toString)
      tSiemGeneralLog.setEventaction(logStr(9).toString)
      tSiemGeneralLog.setEventname(logStr(13).toString)
      tSiemGeneralLog.setEventlevel(logStr(14).toInt)
      //联合事件定义循环
      for(i<- 0 to eventmedefine.size() -1){
        val defrule:JSONObject = eventmedefine.get(i).asInstanceOf[JSONObject]
        val eventDefId = defrule.getString("recordid")
        val eventDefName = defrule.getString("multieventname")
        var relationEvents:List[((String,String,String,String,String,String,String,String,String,String,String))] = null
        var isRelationEventMatch = false
        //联合事件的子集
        for(j<- 0 to eventmedefinesub.size()-1){
          val definesub:JSONObject = eventmedefinesub.get(j).asInstanceOf[JSONObject]
          if(definesub.getString("medefineid").trim.equals(eventDefId)){
            isRelationEventMatch = true
          }
        }

        var keyList:List[(String,String)] = null

        //联合事件子集循环比较
        for(k<- 0 to eventmedefinesub.size()-1){
          val definesub:JSONObject = eventmedefinesub.get(k).asInstanceOf[JSONObject]
          if(definesub.getString("medefineid").trim.equals(eventDefId)){
            var isfineSubMatch = true
            val definesubId = definesub.getString("recordid")
            var isUpAnd = true
            //判断操作符0为and,1为or
            definesub.getInt("op") match {
              case 1 => {
                isUpAnd = false
              }
              case 0 => {
                isUpAnd = true
              }
              case _ => {
                isUpAnd = true
              }
            }
            definesub.getString("objtable").toUpperCase match  {
              //规则关联
              case "T_SIEM_MEDETAIL" =>{
                breakable{
                  var isAnd = true
                  for(l<- 0 to eventmedetail.size()-1){
                    val fieldRule:JSONObject = eventmedetail.get(l).asInstanceOf[JSONObject]
                    if(fieldRule.getString("medid").equals(definesubId)){
                      var isRuleMatch = false
                      val eventFieldId = fieldRule.getString("recordid")
                      if(fieldRule.getInt("flag")==0){  //是事件规则比较
                        if(fieldRule.getString("eventid").equals(eventStr(2).toString)){
                          isRuleMatch = true;
                        }
                      }else if(fieldRule.getInt("flag")==1){ //是字段规则比较
                      val isinner = eventStr(13).toInt
                        if(isinner == 1){ //内置
                        val matchMethod = fieldRule.getString("fieldop") //匹配 方式
                        val eventRuleField = fieldRule.getString("fieldname")//匹配字段
                        val filedBeginValue = fieldRule.getString("fieldvalue")//匹配规则的值
                        val eventRuleFieldMatch = eventRuleField.substring(0, 1).toUpperCase() + eventRuleField.substring(1)
                          val eventRuleFieldValue = tSiemGeneralLog.getClass().getMethod("get"+eventRuleField).invoke(tSiemGeneralLog).toString
                          matchMethod match {
                            //等于
                            case "1" => {
                              if(!filedBeginValue.equalsIgnoreCase(eventRuleFieldValue)){
                                isRuleMatch = false;
                              }
                            }
                            //不等于
                            case "2" => {
                              if (filedBeginValue.equalsIgnoreCase(eventRuleFieldValue)) {
                                isRuleMatch = false;
                              }
                            }
                            //大于
                            case "3" => {

                              if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) <= 0) {
                                isRuleMatch = false;
                              }
                            }
                            //小于
                            case "4" => {
                              if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) >= 0) {
                                isRuleMatch = false;
                              }
                            }
                            //大于等于
                            case "5" => {
                              if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) < 0) {
                                isRuleMatch = false;
                              }
                            }
                            //小于等于
                            case "6" => {
                              if (filedBeginValue.compareToIgnoreCase(eventRuleFieldValue) > 0) {
                                isRuleMatch = false;
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
                                isRuleMatch = false;
                              }
                            }
                            case _ => isRuleMatch = false
                          }
                        }
                      }

                      if(isRuleMatch){
                        val uuid = UUID.randomUUID().toString.replaceAll("-","")
                        val timelimit = fieldRule.getInt("timelimit")
                        val times = fieldRule.getInt("times")
                        val storeDate:Calendar = Calendar.getInstance()
                        val loserDate:Calendar = Calendar.getInstance()
                        loserDate.setTime(storeDate.getTime)
                        loserDate.add(Calendar.SECOND,timelimit)
                        /*
                        var sql:String = "upsert into T_SIEM_RELATIONEVETN_EVENT "
                        sql += "(RECORDID,MEDETAILID,MEDEFINESUBID,MEDEFINEID,EVENTRESULTID,STORAGETIME,TIMES,LOSERTIME) "
                        sql += " values ('" +uuid+"','"+eventFieldId+"','"+definesubId+"','"+eventDefId+"','"+eventStr(0).toString+"','"+format.format(storeDate.getTime)+"',"+timelimit+",'"+format.format(loserDate.getTime) +"')"
                        val st = phoenixConn.createStatement()
                        st.execute(sql)
                        phoenixConn.commit()
                        */

                        val hkey:String = definesubId + "_" + eventFieldId
                        val key:String = definesubId + "_" + eventFieldId + "_#_" + uuid
                        val value:String = eventStr(0).toString
                        jedis.setex(key,timelimit,value)
                        jedis.hsetnx(hkey,key,value)

                        var isSinalDetail = false
                        val eventCount = jedis.hlen(hkey)
                        if(eventCount >= times){
                          val keySet = jedis.hkeys(hkey).iterator()
                          while(keySet.hasNext){
                            val htkey:String = keySet.next()
                            if(keyList == null){
                              keyList = List((htkey,hkey))
                            }else{
                              keyList ++= List((htkey,hkey))
                            }
                          }

                          isSinalDetail = true
                          val eventList = jedis.hvals(hkey).iterator()
                          while(eventList.hasNext){
                            val eventResult:String = eventList.next()
                            val relationEventLists = getEventResultById(phoenixConn,eventResult)
                            relationEventLists.foreach(relationevent=>{
                              if(relationEvents == null){
                                relationEvents = List(relationevent)
                              }else{
                                relationEvents ++= List(relationevent)
                              }

                            })
                          }
                        }
                        /*
                        val relationEventListsDF = relationEventCollect(phoenixConn,format.format(storeDate.getTime),eventFieldId,eventDefId,definesubId)
                        val relationEventLists = relationEventListsDF
                        if(relationEventLists != null && relationEventLists.size >= times){
                          isSinalDetail = true;
                          relationEventLists.foreach(relationevent=>{
                            //relationEvents ++= List(relationevent)

                            if(relationEvents == null){
                              relationEvents = List(relationevent)
                            }else{
                              relationEvents ++= List(relationevent)
                            }

                          })

                        }
                        */

                        if(isAnd){
                          isfineSubMatch = isfineSubMatch && isSinalDetail
                        }else{
                          isfineSubMatch = isfineSubMatch || isSinalDetail
                        }
                      }else{
                        if(isAnd){
                          isfineSubMatch = false
                        }else{
                          isfineSubMatch = isfineSubMatch || false
                        }
                      }
                    }
                  }

                }
              }
              //漏洞关联
              case "T_SIEM_MEDETAIL3" =>{
                val leakmedetail:JSONArray = ruleMap("leakmedetail")
                isfineSubMatch = true
                for(m<- 0 to leakmedetail.size()-1){
                  val fieldRule:JSONObject = leakmedetail.get(m).asInstanceOf[JSONObject]
                  if(fieldRule.getString("medid").equals(definesubId)){
                    var isRuleMatch = false
                    breakable{
                      for(x<- 0 to leakscans.size()-1){
                        val leakscan:JSONObject = leakscans.get(x).asInstanceOf[JSONObject]

                        relationEvents.foreach(event =>{
                          //漏洞IP与事件IP相同
                          if(leakscan.getString("neip").equalsIgnoreCase(event._7)){
                            //CVE关联
                            if(fieldRule.getInt("cvematch") == 1){
                              if(leakscan.getString("leak_cve").equalsIgnoreCase(event._9)){
                                isRuleMatch = true
                                break()
                              }
                            }
                            //漏洞关联
                            if(fieldRule.getInt("leakmatch") == 1){
                              isRuleMatch = true
                              break()
                            }
                            //端口关联
                            if(fieldRule.getInt("portmatch") == 1){
                              if(leakscan.getString("leak_port").equalsIgnoreCase(event._8)){
                                isRuleMatch = true
                                break()
                              }
                            }
                          }
                        })
                      }
                    }
                    isfineSubMatch = isfineSubMatch && isRuleMatch
                  }
                }
              }
              //流量关联
              case "T_SIEM_MEDETAIL6" =>{
                isfineSubMatch = true
                val netflowmedetail:JSONArray = ruleMap("netflowmedetail")
                var isValidate = true;//true为验证，false为预测
                isfineSubMatch = true
                for(y<- 0 to netflowmedetail.size()-1){
                  val netflow:JSONObject = netflowmedetail.get(y).asInstanceOf[JSONObject]
                  if(netflow.getString("medid").equals(definesubId)){
                    var isRuleMatch = false
                    val statisKind = netflow.getInt("statisperiod")
                    val flowlimit = netflow.getInt("flowlimit")
                    val exp = netflow.getString("conditionexp")
                    var sql = "select SRCIP,DSTIP,SRCPORT,DSTPORT,PROTO,PROTO7,PACKERNUM,BYTESIZE,FLOWNUM,STATISTIME,FLAG from "
                    val crtTime:Calendar = Calendar.getInstance();
                    var formatFlow = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    statisKind match {
                      //统计-秒
                      case 1=>{
                        sql += " T_SIEM_NETFLOW_SEC where STATISTIME between '" + formatFlow.format(crtTime) +"'"
                        val beginTime = crtTime.set(Calendar.SECOND, crtTime.get(Calendar.SECOND) - 10)
                        sql += " and '" + formatFlow.format(beginTime) +"' order by STATISTIME desc limit 1"

                      }
                      //统计-分钟
                      case 2=>{
                        formatFlow = new SimpleDateFormat("yyyy-MM-dd HH:mm")
                        val beginTime = crtTime.set(Calendar.MINUTE, crtTime.get(Calendar.MINUTE) - 1)
                        sql += " T_SIEM_NETFLOW_SEC where STATISTIME between '" + formatFlow.format(crtTime) +":00'"
                        sql += " and '" + formatFlow.format(beginTime) +":00' order by STATISTIME desc limit 1"
                      }
                      //统计-小时
                      case 3=>{
                        formatFlow = new SimpleDateFormat("yyyy-MM-dd HH")
                        val beginTime = crtTime.set(Calendar.HOUR_OF_DAY, crtTime.get(Calendar.HOUR_OF_DAY) - 1)
                        sql += " T_SIEM_NETFLOW_SEC where STATISTIME between '" + formatFlow.format(crtTime) +":00:00'"
                        sql += " and '" + formatFlow.format(beginTime) +":00:00' order by STATISTIME desc limit 1"
                      }
                    }

                    val ps:Statement = phoenixConn.createStatement()
                    val rs:ResultSet = ps.executeQuery(sql)
                    while (rs.next()){
                      if(flowlimit>rs.getInt(8)){
                        isRuleMatch = true
                      }
                    }
                    if(isRuleMatch){
                      if(exp != null && !exp.trim.equals("")){
                        val items = exp.split("~:")
                        if(items != null && items.length>0){
                          items.foreach(item =>{
                            if(item != null && item.contains(":")){
                              val itemcfg = item.split(":")
                              if(itemcfg.length==3){
                                itemcfg(0).trim match {
                                  //源IP
                                  case "sourceip"=>{
                                    itemcfg(1) match {
                                      //等于
                                      case "0"=>{
                                        if(tSiemGeneralLog.getSourceip.equalsIgnoreCase(itemcfg(2))){
                                          isRuleMatch = isRuleMatch && true
                                        }
                                      }
                                      //大于
                                      case "1"=>{
                                        if(tSiemGeneralLog.getSourceip.compareToIgnoreCase(itemcfg(2))<= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                      //小于
                                      case "2"=>{
                                        if(tSiemGeneralLog.getSourceip.compareToIgnoreCase(itemcfg(2))>= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                    }
                                  }
                                  //源端口
                                  case "sourceport"=>{
                                    itemcfg(1) match {
                                      //等于
                                      case "0"=>{
                                        if(tSiemGeneralLog.getSourceport.equalsIgnoreCase(itemcfg(2))){
                                          isRuleMatch = isRuleMatch && true
                                        }
                                      }
                                      //大于
                                      case "1"=>{
                                        if(tSiemGeneralLog.getSourceport.compareToIgnoreCase(itemcfg(2))<= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                      //小于
                                      case "2"=>{
                                        if(tSiemGeneralLog.getSourceport.compareToIgnoreCase(itemcfg(2))>= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                    }
                                  }
                                  //目的IP
                                  case "destip"=>{
                                    itemcfg(1) match {
                                      //等于
                                      case "0"=>{
                                        if(tSiemGeneralLog.getDestip.equalsIgnoreCase(itemcfg(2))){
                                          isRuleMatch = isRuleMatch && true
                                        }
                                      }
                                      //大于
                                      case "1"=>{
                                        if(tSiemGeneralLog.getDestip.compareToIgnoreCase(itemcfg(2))<= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                      //小于
                                      case "2"=>{
                                        if(tSiemGeneralLog.getDestip.compareToIgnoreCase(itemcfg(2))>= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                    }
                                  }
                                  //目的端口
                                  case "destport"=>{
                                    itemcfg(1) match {
                                      //等于
                                      case "0"=>{
                                        if(tSiemGeneralLog.getDestport.equalsIgnoreCase(itemcfg(2))){
                                          isRuleMatch = isRuleMatch && true
                                        }
                                      }
                                      //大于
                                      case "1"=>{
                                        if(tSiemGeneralLog.getDestport.compareToIgnoreCase(itemcfg(2))<= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                      //小于
                                      case "2"=>{
                                        if(tSiemGeneralLog.getDestport.compareToIgnoreCase(itemcfg(2))>= 0){
                                          isRuleMatch = isRuleMatch && false
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }

                          })
                        }
                      }
                    }
                    isfineSubMatch = isfineSubMatch && isRuleMatch
                  }
                }

              }
              //资产关联
              case "T_SIEM_MEDETAIL6" => {
                isfineSubMatch = true
                val nemedetail:JSONArray = ruleMap("nemedetail")
                for(z<- 0 to nemedetail.size()-1){
                  val ne:JSONObject = nemedetail.get(z).asInstanceOf[JSONObject]
                  if(ne.getString("medid").equals(definesubId)){
                    var isRuleMatch = false
                    val reportip = logStr(4).toString.trim
                    val sourceip = logStr(5).toString.trim
                    val destip = logStr(7).toString.trim
                    val iptype = ne.getInt("iptype")
                    val ipaddr = ne.getString("ipaddr").trim
                    val os = ne.getInt("os")
                    iptype match {
                      //上报ip
                      /*
                      case 1 =>{

                      }
                      */
                      //源ip
                      case 2 =>{
                        val ne_os = getOsByIp(sourceip,phoenixConn)
                        if(sourceip.equals(ipaddr) && ne_os.equals(os)){
                          isRuleMatch = true
                        }else{
                          isRuleMatch = false
                        }
                      }
                      //目的ip
                      case 3 =>{
                        val ne_os = getOsByIp(destip,phoenixConn)
                        if(destip.equals(ipaddr) && ne_os.equals(os)){
                          isRuleMatch = true
                        }else{
                          isRuleMatch = false
                        }
                      }

                    }
                    isfineSubMatch = isfineSubMatch && isRuleMatch
                  }
                }
              }
              //情报关联
              case "T_SIEM_MEDETAIL4" => {
                isfineSubMatch = true
                val sourceip = logStr(5).toString.trim
                val intelligencemedetail:JSONArray = ruleMap("intelligencemedetail")
                for(p <- 0 to intelligencemedetail.size()-1){
                  val intelligence:JSONObject = intelligencemedetail.get(p).asInstanceOf[JSONObject]
                  if(intelligence.getString("medid").equals(definesubId)){
                    var isRuleMatch = false
                    val ipmatch = intelligence.getInt("ipmatch")
                    if(ipmatch == 1){
                      if(getIntelligenceByIp(sourceip,phoenixConn)){
                        isRuleMatch = true
                      }else{
                        isRuleMatch = false
                      }
                    }else{
                      isRuleMatch = false
                    }
                    isfineSubMatch = isfineSubMatch && isRuleMatch
                  }
                }
              }
            }

            isUpAnd match {
              case true =>{ //与前一个规则and关联
                isRelationEventMatch = isRelationEventMatch && isfineSubMatch
              }
              case false =>{//与前一个规则or关联
                isRelationEventMatch = isRelationEventMatch || isfineSubMatch
              }
            }
          }
        }


        //如果联合事件匹配
        if(isRelationEventMatch && relationEvents != null && relationEvents.size >0) {
          val crtDate:Calendar = Calendar.getInstance()
          //删除失效的数据
          //deleteRelationEvent(format.format(crtDate.getTime),phoenixConn)
          val id = UUID.randomUUID().toString().replaceAll("-","")
          if(relationEventStr.equals("")){
            relationEventStr = "$$$$$" + id+"~"+eventDefId+"~"+eventDefName+"~"+format.format(crtDate.getTime)
          }else{
            relationEventStr += "#####"+id+"~"+eventDefId+"~"+eventDefName+"~"+format.format(crtDate.getTime)
          }

          val client = ESClient.esClient()
          val rowekey = ROWUtils.genaralROW()
          var mrdsql:String = "upsert into T_SIEM_MERECORD "
          mrdsql += "(\"ROW\",\"RECORDID\",\"MEDID\",\"STORAGETIME\") "
          mrdsql += " values ('"+rowekey+"','"+ id +"','"+ eventDefId +"','"+ format.format(crtDate.getTime)+"')"
          sqls = sqls.::(mrdsql)
          var jsonGEvent = new JSONObject()
          jsonGEvent.put("ROW",rowekey)
          jsonGEvent.put("RECORDID",id)
          jsonGEvent.put("MEDID",eventDefId)

          if(logStr(3).toString.equals("NE_FLOW")){//南基专用事件
            jsonGEvent.put("EVENT_RULE_ID",eventStr(2).toString)
            jsonGEvent.put("EVENT_RULE_NAME",logStr(13).toString)
            jsonGEvent.put("sourceip",logStr(5).toString)
            jsonGEvent.put("sourceport",logStr(6).toString)
            jsonGEvent.put("destip",logStr(7).toString)
            jsonGEvent.put("destport",logStr(8).toString)
            jsonGEvent.put("eventlevel",logStr(14).toString)
            jsonGEvent.put("appproto",logStr(16).toString)
            jsonGEvent.put("url",logStr(17).toString)
            jsonGEvent.put("getparameter",logStr(18).toString)
            jsonGEvent.put("proto",logStr(19).toString)
            jsonGEvent.put("opentime",logStr(20).toString)
          }

          jsonGEvent.put("STORAGETIME",DateUtils.dateToStamp(format.format(crtDate.getTime)).toLong)

          var keySet = jsonGEvent.keys()
          var tempjson:JSONObject = new JSONObject();
          while (keySet.hasNext){
            var key:String = keySet.next().asInstanceOf[String];
            tempjson.put(key.toLowerCase(), jsonGEvent.get(key));
          }
          jsonGEvent = tempjson
          gevents = gevents.::(jsonGEvent.toString)

          relationEvents.foreach(event =>{
            val rowkey = ROWUtils.genaralROW()
            val mrid = UUID.randomUUID().toString().replaceAll("-","")
            var mrdesql:String = "upsert into T_SIEM_MERECORD_EVENT "
            mrdesql += "(\"ROW\",\"RECORDID\",\"EVENT_RULE_RESULT_ID\",\"T_S_RECORDID\") "
            mrdesql += " values ('"+rowkey+"','"+ mrid+"','"+event._1+"','"+id+"')"
            sqls = sqls.::(mrdesql)

            var jsonGSEvent = new JSONObject()
            jsonGSEvent.put("ROW",rowekey)
            jsonGSEvent.put("RECORDID",mrid)
            jsonGSEvent.put("EVENT_RULE_RESULT_ID",event._1)
            jsonGSEvent.put("T_S_RECORDID",id)

            var keySet = jsonGSEvent.keys()
            var tempgjson:JSONObject = new JSONObject();
            while (keySet.hasNext){
              var key:String = keySet.next().asInstanceOf[String];
              tempgjson.put(key.toLowerCase(), jsonGSEvent.get(key));
            }
            jsonGSEvent = tempgjson
            gsevents = gsevents.::(jsonGSEvent.toString)
          })
          //删除暂存事件
          for(tkey <- keyList){
            jedis.del(tkey._1)
            jedis.hdel(tkey._2,tkey._1)
          }

          //deleteRelationEventByMedineId(eventDefId,format.format(crtDate.getTime),phoenixConn)
        }

      }

      jedis.close()
    }
    (relationEventStr,gevents,gsevents,sqls)
    //(gevents,gsevents)
  }

  /**
    * 批量存储联合事件
    * @param sqls
    * @param gevents
    * @param gsevents
    * @param stmt
    * @param client
    */
  def batchSaveRelationEvent(sqls:List[String],gevents:List[String],gsevents:List[String],stmt:Statement,client:Client):Unit = {
    //关联事件入库
    if(sqls != null && sqls.size>0){
      sqls.foreach(sql =>{
        if(sql != null && !sql.trim.equals("")){
          stmt.addBatch(sql)
        }
      })
    }
    //关联事件入ES
    if(gevents != null && gevents.size>0){
      val tempgevents: java.util.List[String] = new java.util.ArrayList()
      gevents.foreach(gevent =>{
        if(gevent != null && !gevent.trim.equals("")){
          tempgevents.add(gevent)
        }
      })
      val indexName = "gevent"
      val typeName = "gevent"
      IndexUtils.batchIndexData(client,indexName,typeName,tempgevents)
    }

    if(gsevents != null && gsevents.size>0){
      val tempgsevents: java.util.List[String] = new java.util.ArrayList()
      gsevents.foreach(gsevent =>{
        if(gsevent != null && !gsevent.trim.equals("")){
          tempgsevents.add(gsevent)
        }
      })
      val indexName = "gsevent"
      val typeName = "gsevent"
      IndexUtils.batchIndexData(client,indexName,typeName,tempgsevents)
    }

  }
  /**
    * 统计联合事件关联事件数
    *
    * @param phoenixConn
    * @param losertime
    * @param medetailid
    * @param medefineid
    * @return
    */
  def  relationEventCollect(phoenixConn:Connection,losertime:String,medetailid:String,medefineid:String,definesubId:String):List[(String,String,String,String,String,String,String,String,String,String,String)] ={
    var list:List[(String,String,String,String,String,String,String,String,String,String,String)] = null
    val sql:String = "select RECORDID,MEDETAILID,MEDEFINEID,EVENTRESULTID,STORAGETIME,TIMES,LOSERTIME,DESTIP,DESTPORT,CVE,ATTACKTYPE " +
      "from T_SIEM_RELATIONEVETN_EVENT where LOSERTIME>=TO_DATE('"+losertime+"') and MEDETAILID='"+medetailid+"' and MEDEFINEID='"+medefineid+"' and MEDEFINESUBID='"+definesubId+"'"
    val st = phoenixConn.createStatement()
    val rs = st.executeQuery(sql)
    while (rs.next()){
      if(list == null){
        list = List((rs.getString("RECORDID"),rs.getString("MEDETAILID"),rs.getString("MEDEFINEID"),rs.getString("EVENTRESULTID"),rs.getString("STORAGETIME"),rs.getString("TIMES"),rs.getString("LOSERTIME"),rs.getString("DESTIP"),rs.getString("DESTPORT"),rs.getString("CVE"),rs.getString("ATTACKTYPE")))
      }else{
        list ++= List((rs.getString("RECORDID"),rs.getString("MEDETAILID"),rs.getString("MEDEFINEID"),rs.getString("EVENTRESULTID"),rs.getString("STORAGETIME"),rs.getString("TIMES"),rs.getString("LOSERTIME"),rs.getString("DESTIP"),rs.getString("DESTPORT"),rs.getString("CVE"),rs.getString("ATTACKTYPE")))
      }

    }
    rs.close()
    st.close()
    list
  }

  /**
    * 根据事件结果ID获取事件结果信息
    *
    * @param phoenixConn
    * @param eventResultId
    * @return
    */
  def getEventResultById(phoenixConn:Connection,eventResultId:String):List[(String,String,String,String,String,String,String,String,String,String,String)] ={
    var list:List[(String,String,String,String,String,String,String,String,String,String,String)] = null
    val sql:String = "select EVENT_RULE_RESULT_ID,EVENT_RULE_NAME,EVENT_RULE_LEVEL,PROTO,SOURCEIP,SOURCEPORT,DESTIP,DESTPORT,OPENTIME,CVE,ATTACKTYPE " +
      "from T_SIEM_EVENT_RESULT where EVENT_RULE_RESULT_ID='"+eventResultId+"'"
    val st = phoenixConn.createStatement()
    val rs = st.executeQuery(sql)
    while (rs.next()){
      if(list == null){
        list = List((rs.getString("EVENT_RULE_RESULT_ID"),rs.getString("EVENT_RULE_NAME"),rs.getString("EVENT_RULE_LEVEL"),rs.getString("PROTO"),rs.getString("SOURCEIP"),rs.getString("SOURCEPORT"),rs.getString("DESTIP"),rs.getString("DESTPORT"),rs.getString("OPENTIME"),rs.getString("CVE"),rs.getString("ATTACKTYPE")))
      }else{
        list ++= List((rs.getString("EVENT_RULE_RESULT_ID"),rs.getString("EVENT_RULE_NAME"),rs.getString("EVENT_RULE_LEVEL"),rs.getString("PROTO"),rs.getString("SOURCEIP"),rs.getString("SOURCEPORT"),rs.getString("DESTIP"),rs.getString("DESTPORT"),rs.getString("OPENTIME"),rs.getString("CVE"),rs.getString("ATTACKTYPE")))
      }

    }
    rs.close()
    st.close()
    list
  }


  /**
    * 根据联合事件定义ID删除关联事件
    *
    * @param medefineid
    * @return
    */
  def deleteRelationEventByMedineId(medefineid:String,losertime:String,phoenixConn:Connection):Unit ={
    val dbUtils=new DBUtils
    val conn:Connection = phoenixConn
    val st:Statement = conn.createStatement()
    //val sql = "delete from T_SIEM_RELATIONEVETN_EVENT where STORAGETIME<=TO_DATE('"+losertime+"') and MEDEFINEID='" + medefineid + "'"
    val sql = "delete from T_SIEM_RELATIONEVETN_EVENT where STORAGETIME<=TO_DATE('"+losertime+"') and MEDEFINEID='" + medefineid + "'"
    st.execute(sql)
    conn.commit()
  }

  /**
    * 根据时间删除已经失效的关联事件
    *
    * @param losertime
    * @return
    */
  def deleteRelationEvent(losertime:String,phoenixConn:Connection):Unit ={
    val dbUtils=new DBUtils
    val sql:String = "delete from T_SIEM_RELATIONEVETN_EVENT where LOSERTIME<TO_DATE('"+losertime+"')"
    val conn:Connection = phoenixConn
    val st:Statement = conn.createStatement()
    st.execute(sql)
    conn.commit()
  }

  /**
    * 根据IP获取操作系统版本
    *
    * @param ip
    * @param phoenixConn
    * @return
    */
  def getOsByIp(ip:String,phoenixConn:Connection):Int = {
    var os = 1
    val sql = "select ne.OS_ID from T_SIEM_MONI_NE ne left join T_MONI_DEVICE_IP ip on ne.NE_ID=ip.NE_ID where ip.DEVICE_IP='" + ip +"'"
    val st = phoenixConn.createStatement()
    val rs = st.executeQuery(sql)
    while (rs.next()){
      os = rs.getInt("OS_ID")
    }
    rs.close()
    st.close()
    os
  }
  /**
    * 根据IP查询情报威胁库
    *
    * @param ip
    * @param phoenixConn
    * @return
    */
  def getIntelligenceByIp(ip:String,phoenixConn:Connection):Boolean = {
    var isIntelligence = false
    val sql = "select ATTACK_IP from T_SIEM_INTELLIGENCE where ATTACK_IP='" + ip +"'"
    val st = phoenixConn.createStatement()
    val rs = st.executeQuery(sql)
    while (rs.next()){
      isIntelligence = true
    }
    rs.close()
    st.close()
    isIntelligence
  }

}
