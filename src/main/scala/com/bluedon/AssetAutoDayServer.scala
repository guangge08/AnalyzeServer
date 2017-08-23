package com.bluedon

import java.io.InputStream
import java.sql.{Connection, DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.bluedon.asset.AssetAuto
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

/**
  * Created by huoguang on 2017/5/10.
  */

case class flow(srcip: String, dstip: String, srcport: Int, dstport: Int, proto: String, packageNum: Int, recordTime: String)

