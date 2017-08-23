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
import org.apache.spark.streaming.kafka010._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import com.bluedon.neModel._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}


/**
  * Created by Administrator on 2016/12/14.
  */
object NetflowModelServer {

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
    val masterUrlBD = sparkConf.broadcast(masterUrl)
    val assetsPortrait = new AssetsPortrait()
    assetsPortrait.assetsPortraitMain(spark,"2017-3-10 16:19:24","2017-3-25 16:49:26")

  }
}
