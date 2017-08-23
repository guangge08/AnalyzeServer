package com.bluedon.listener

import java.io.InputStream
import java.util.{Date, Properties}

import com.bluedon.utils.{BCVarUtil, RuleUtils}
import net.sf.json.JSONArray
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchStarted}
import redis.clients.jedis.Jedis

/**
  * kafka数据流事件监听类
  * Created by dengxiwen on 2017/7/20.
  */
class KafkaStreamListener extends  StreamingListener{

  private var bcVarUtil:BCVarUtil = _
  private var spark:SparkSession = _

  def kafkaStreamListener(spark:SparkSession,bcVarUtil:BCVarUtil):Unit = {
    this.bcVarUtil = bcVarUtil
    this.spark = spark
  }
  /**
    * 数据批处理开始触发
    * @param batchStarted
    */
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit ={
    val properties:Properties = new Properties();
    val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
    properties.load(ipstream);

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")
    val time = new Date()
    var refreshTime = this.bcVarUtil.getRefreshTime()
    //间隔一分钟刷新规则

    if(time.getTime - refreshTime > 300000){
      this.bcVarUtil.setRefreshTime(time.getTime)
      //获取规则
      val jedis:Jedis = new Jedis(redisHost,6379)
      jedis.auth("123456");
      val ruleUtils = new RuleUtils
      val ruleVarBC = this.bcVarUtil.getBCMap()
      var allRuleMapBC:Map[String, JSONArray] = ruleUtils.getAllRuleMapByRedis(jedis,url,username,password)
      var dataCheckMapBC:Map[String, JSONArray] = ruleUtils.getCheckDataMapByRedis(jedis,url,username,password)
      //获取告警规则
      var alarmRuleMapBC:Map[String, JSONArray] = ruleUtils.getAlarmRuleMapByRedis(jedis,url,username,password)
      val ruleMap = Map[String,Map[String, JSONArray]]("allRuleMapBC" -> allRuleMapBC,"dataCheckMapBC" -> dataCheckMapBC,"alarmRuleMapBC" -> alarmRuleMapBC)
      var ruleMapBC:Broadcast[Map[String, Map[String, JSONArray]]] = spark.sparkContext.broadcast(ruleMap)
      ruleVarBC("ruleBC").unpersist
      ruleVarBC("ruleBC") = null
      ruleVarBC("ruleBC") = ruleMapBC
      jedis.close()

      println("############更新规则##################")

    }

  }
}
