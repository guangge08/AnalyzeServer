package com.bluedon

import java.io.InputStream
import java.util.Properties

import com.bluedon.asset.AssetAuto
import com.bluedon.dataMatch._
import com.bluedon.esinterface.config.ESClient
import com.bluedon.utils._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._;


/**
  * Created by Administrator on 2016/12/14.
  */
object AssetAutoServer {

  def main(args: Array[String]) {
    /*
    val properties:Properties = new Properties();
    val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
    properties.load(ipstream);

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name") + "_FLOW"
    val spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //val sparkBCF = spark.sparkContext.broadcast(spark)

    val sparkConf = spark.sparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //上网行为处理
    flowProcess(spark,ssc,properties)

    ssc.start()
    ssc.awaitTermination()
    */
  }
  /*
    /**
      * 上网行为处理
      *
      * @param spark
      * @param properties
      */

    def flowProcess(spark:SparkSession,ssc:StreamingContext,properties:Properties): Unit ={

      val url:String = properties.getProperty("aplication.sql.url")
      val username:String = properties.getProperty("aplication.sql.username")
      val password:String = properties.getProperty("aplication.sql.password")
      val redisHost:String = properties.getProperty("redis.host")

      val zkQuorumRoot = properties.getProperty("zookeeper.host")+":" + properties.getProperty("zookeeper.port")
      val zkQuorumURL = spark.sparkContext.broadcast(zkQuorumRoot)

      val numThreads = 1
      val group = "1"
      val topics = properties.getProperty("kafka.flow.topic")

      val sparkConf = spark.sparkContext
      //ssc.checkpoint("checkpoint")

      val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

      //regexRule.regexMatch(x)
      //获取kafka上网行为流数据
      val flows = KafkaUtils.createStream(ssc, zkQuorumRoot, group, topicMap,StorageLevel.MEMORY_AND_DISK_SER)
      val assetAuto:AssetAuto = new AssetAuto
      val neips = assetAuto.getNeIp(spark,url,username,password).collect()
      val neipsBD = spark.sparkContext.broadcast(neips)
      val flowsRDD = flows.map(flow =>flow._2).flatMap(flow =>flow.split(",")).map(flow=>(flow,1)).reduceByKey(_+_).map(flow =>{
        flow._1
      })
      val flowsRDDCache = flowsRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      flowsRDDCache.repartition(3)
      flowsRDDCache.foreachRDD(rdd =>{
        rdd.foreachPartition(flows=>{
          //数据库连接
          val zkQuorum = zkQuorumURL.value
          val dbUtils = new DBUtils
          var phoenixConn = dbUtils.getPhoniexConnect(zkQuorum)
          var stmt = phoenixConn.createStatement()
          flows.foreach(flow=>{
            val assetAuto:AssetAuto = new AssetAuto
            //assetAuto.assetAuto(flow,phoenixConn,stmt,neipsBD.value)
          })
          phoenixConn.commit()
        })
      })

      flows.slideDuration
      flowsRDD.slideDuration
    }
    */
}
