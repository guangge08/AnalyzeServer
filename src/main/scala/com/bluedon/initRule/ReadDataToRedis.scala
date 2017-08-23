package com.bluedon.initRule

import java.util
import java.util.concurrent.TimeUnit

import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/**
  * 读取pgsql规则库数据存入redis中
  * Created by Administrator on 2017/6/15 0015.
  */
class ReadDataToRedis {


  /**
    * 定时的更新日志规则,从pgSQL中读取数据存入redis中
    * @param sparkSession
    * @param jedis
    * @param pgUrl pgSQL url
    * @param pgUsername
    * @param pgPassword
    */
  def startRefreshDataToRedis(sparkSession: SparkSession,jedis: Jedis,pgUrl:String,pgUsername:String,pgPassword:String):Unit={

//    dealAlarmRegexRule(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
//    dealDataCheck(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
//    dealEventRegexRule(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
    dealLogRegexRule(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
//    dealRelationEventRegexRule(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
//    dealLogRegexRuleByUser(sparkSession,jedis,pgUrl,pgUsername,pgPassword)
  }


  /**
    * 保存数据进redis
    * @param sparkSession
    * @param jedis
    * @param ruleName
    * @param jdbcDF
    */
    def readDataToRedis(sparkSession: SparkSession,jedis: Jedis,ruleName:String,jdbcDF:DataFrame):Unit={

      val cols: Array[String] = jdbcDF.columns
      val ruleArray:JSONArray = new JSONArray()
      jdbcDF.collect().foreach(row=>{
        val ruleJson = new JSONObject()
        var rowjson = ""
        for (index<-0  to cols.length-1){
          var field = row.get(index)
          if(StringUtils.isEmpty(String.valueOf(field)) || "null".equals(field) || null == field){
            field = ""
          }
          ruleJson.put(cols(index),String.valueOf(field))
        }
        ruleArray.add(ruleJson)
      })

      jedis.set(ruleName,ruleArray.toString)
    }

   /**
    * 处理告警规则
    * @param sparkSession
    * @param jedis
    */
     def dealAlarmRegexRule(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={
        val arr = new AlarmRegexRule
        val ruleName = "alarmPolicyWhole"
        val df = arr.getAlarmPolicyWhole(sparkSession,url,username,password)
        readDataToRedis(sparkSession,jedis,ruleName,df)

        val df2 = arr.getAlarmPolicyEvent(sparkSession,url,username,password)
        val ruleName2 = "alarmPolicyEvent"
        readDataToRedis(sparkSession,jedis,ruleName2,df2)

        val df3 = arr.getAlarmPolicyRelationEvent(sparkSession,url,username,password)
        val ruleName3="alarmPolicyRelationEvent"
        readDataToRedis(sparkSession,jedis,ruleName3,df3)
     }//告警规则


  /**
    * 漏洞扫描
    * @param sparkSession
    * @param jedis
    * @param url
    * @param username
    * @param password
    */
    def dealDataCheck(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={
      val ruleName = "leakScans"
      val df = new DataCheck().getLeakScans(sparkSession,url,username,password)
      readDataToRedis(sparkSession,jedis,ruleName,df)
    }

  /**
    * 定义规则
     * @param sparkSession
    * @param jedis
    * @param url
    * @param username
    * @param password
    */
    def dealEventRegexRule(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={

      val evenrr = new EventRegexRule
      val df = evenrr.getEventDefByUser(sparkSession,url,username,password)
      val ruleName = "eventDefByUser"
      readDataToRedis(sparkSession,jedis,ruleName,df)

      val df2 = evenrr.getEventDefBySystem(sparkSession,url,username,password)
      val ruleName2 = "eventDefBySystem"
      readDataToRedis(sparkSession,jedis,ruleName2,df2)

      val df3 = evenrr.getEventRule(sparkSession,url,username,password)
      val ruleName3 = "eventRule"
      readDataToRedis(sparkSession,jedis,ruleName3,df3)
    }

  /**
    * 原始日志规则-内置
    * @param sparkSession
    * @param jedis
    * @param url
    * @param username
    * @param password
    */
    def dealLogRegexRule(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={
      val df = new LogRegexRule().getLogRegex(sparkSession,url,username,password)
      val ruleName = "logRegex"
//      jedis.del(ruleName)
      readDataToRedis(sparkSession,jedis,ruleName,df)
    }

  /**
    * 原始日志规则-自定义
    * @param sparkSession
    * @param jedis
    * @param url
    * @param username
    * @param password
    */
  def dealLogRegexRuleByUser(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={
    val df = new LogRegexRule().getLogRegexByUser(sparkSession,url,username,password)
    val ruleName = "logRegexByUser"
    readDataToRedis(sparkSession,jedis,ruleName,df)
  }

  /**
    * 处理关联关系规则
    * @param sparkSession
    * @param jedis
    * @param url
    * @param username
    * @param password
    */
    def dealRelationEventRegexRule(sparkSession: SparkSession,jedis: Jedis,url:String,username:String,password:String):Unit={

        val rerr = new RelationEventRegexRule

        val df = rerr.getRelationEventMedefine(sparkSession,url,username,password)
        val ruleName = "relationEventMedefine"
        readDataToRedis(sparkSession,jedis,ruleName,df)

        val df2 = rerr.getRelationEventMedefineSub(sparkSession,url,username,password)
        val ruleName2 = "relationEventMedefineSub"
        readDataToRedis(sparkSession,jedis,ruleName2,df2)

        val df3 = rerr.getRelationEventMedetail(sparkSession,url,username,password)
        val ruleName3 = "relationEventMedetail"
        readDataToRedis(sparkSession,jedis,ruleName3,df3)

        val df4 = rerr.getRelationLeakMedetail(sparkSession,url,username,password)
        val ruleName4 = "relationLeakMedetail"
        readDataToRedis(sparkSession,jedis,ruleName4,df4)

        val df5 = rerr.getRelationNetflowMedetail(sparkSession,url,username,password)
        val ruleName5 = "relationNetflowMedetail"
        readDataToRedis(sparkSession,jedis,ruleName5,df5)

        val df6 = rerr.getRelationNeMedetail(sparkSession,url,username,password)
        val ruleName6 = "relationNeMedetail"
        readDataToRedis(sparkSession,jedis,ruleName6,df6)

        val df7 = rerr.getRelationIntelligenceMedetail(sparkSession,url,username,password)
        val ruleName7 = "relationIntelligenceMedetail"
        readDataToRedis(sparkSession,jedis,ruleName7,df7)
    }

}
























