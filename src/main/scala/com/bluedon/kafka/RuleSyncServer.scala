package com.bluedon.kafka

import java.io.InputStream
import java.util.Properties

import com.bluedon.dataMatch._
import com.bluedon.initRule.{LogRegexRule, ReadDataToRedis}
import com.bluedon.utils.RuleUtils
import net.sf.json.{JSON, JSONArray, JSONObject}
import org.apache.phoenix.exception.PhoenixParserException
import org.apache.spark.sql._
import org.apache.spark.streaming._
import redis.clients.jedis.{Jedis, Pipeline}


/**
  * Created by Administrator on 2016/12/14.
  */
object RuleSyncServer{

  def main(args: Array[String]) {
    try{
      val properties:Properties = new Properties();
      val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
      properties.load(ipstream);

      val url:String = properties.getProperty("aplication.sql.url")
      val username:String = properties.getProperty("aplication.sql.username")
      val password:String = properties.getProperty("aplication.sql.password")
      val redisHost:String = properties.getProperty("redis.host")
      val reflushTime = properties.getProperty("redis.rule.reflush")

      val masterUrl = properties.getProperty("spark.master.url")
      val appName = properties.getProperty("spark.analyze.app.name")
//      val spark = SparkSession
//        .builder()
//        .master(masterUrl)
//        .appName(appName+"_rule")
//        .config("spark.some.config.option", "some-value")
//        .getOrCreate()

      val spark: SparkSession = SparkSession.builder().master("local").
        appName("dataset").getOrCreate()

      while (true){
        val jedis:Jedis = new Jedis(redisHost,6379)
        jedis.auth("123456");
        val readDataToRedis:ReadDataToRedis = new ReadDataToRedis()
        println("########开始刷新规则########")
        readDataToRedis.startRefreshDataToRedis(spark,jedis,url,username,password)

        jedis.close()
        Thread.sleep(Integer.parseInt(reflushTime))
      }
    }catch {
      case e:Exception=>{
        e.printStackTrace()
      }
    }
  }

}
