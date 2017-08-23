package com.bluedon

import java.io.InputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by dengxiwen on 2017/3/6.
  */
object HbaseSpark {
  def main(args: Array[String]) {

    val properties:Properties = new Properties();
    val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
    properties.load(ipstream);

    val url:String = properties.getProperty("aplication.sql.url")
    val username:String = properties.getProperty("aplication.sql.username")
    val password:String = properties.getProperty("aplication.sql.password")
    val redisHost:String = properties.getProperty("redis.host")

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //val sparkBCF = spark.sparkContext.broadcast(spark)

    val sparkConf = spark.sparkContext
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.quorum", "aqtsgzpt215,aqtsgzsjfxpt213,aqtsgzsjfxpt212,aqtsgzsjfxpt211")
    hBaseConf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_ORIGINAL_LOG")

    val sqlContext = new SQLContext(sparkConf)
    import sqlContext.implicits._

    val hbaseRDD = sparkConf.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val syslog = hbaseRDD.map(r=>(
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("ROW"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("RECORDID"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("LOGID"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("RECVTIME"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("STORAGETIME"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("REPORTIP"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("REPORTAPP"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("LOGCONTENT"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("ISMATCH"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("OLOG"),Bytes.toBytes("DUBLECOUNT")))
      )).toDF("ROW","RECORDID","LOGID","RECVTIME","STORAGETIME","REPORTIP","REPORTAPP","LOGCONTENT","ISMATCH","DUBLECOUNT")

    syslog.registerTempTable("syslog")
    val df2 = sqlContext.sql("SELECT ROW FROM syslog where ")
    df2.collect().foreach(row=>{
      print(row.getString(0))
    })

    println("#####################################################")
    println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"+df2.count())
  }
}
