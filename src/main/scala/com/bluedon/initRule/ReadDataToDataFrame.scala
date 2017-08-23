package com.bluedon.initRule

import java.io.InputStream
import java.util.Properties

import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2017/6/15 0015.
  * 获取从redis中加载的数据
  */
class ReadDataToDataFrame {


  /**
    * 读取redis字符串数据转化JSONArray对象
    *
    * @param jedis
    * @param ruleName
    * @return
    */
  private def getDataFrame(jedis: Jedis,ruleName:String): JSONArray ={

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

  /**
    * 对外提供接口访问获取JSONArray
    *
    * @param jedis
    * @param methodName
    * @return
    */
  def getDataFrameFromRedis(jedis: Jedis,methodName:String):JSONArray={

    if(StringUtils.isEmpty(methodName.trim)){
      println("methodName is null")
      return  null
    }
    var resDF:JSONArray = null
    try{
        methodName match {
          case "alarmPolicyWhole" =>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "alarmPolicyEvent"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "alarmPolicyRelationEvent"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "leakScans"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "eventDefByUser"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "eventDefBySystem"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "eventRule"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "logRegex"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "logRegexByUser"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "relationEventMedefine"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "relationEventMedefineSub"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "relationEventMedetail"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "relationLeakMedetail"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "relationNetflowMedetail"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "relationNeMedetail"=>{
            resDF = getDataFrame(jedis,methodName)
          }
          case "relationIntelligenceMedetail"=>{
            resDF = getDataFrame(jedis,methodName)
          }
        }
    }catch {
      case e:Exception=>{
        println("match is error")
        println(e.getMessage)
        println(e.printStackTrace())
      }
    }
    resDF
  }
}
