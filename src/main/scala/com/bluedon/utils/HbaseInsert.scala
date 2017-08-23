package com.bluedon.utils

import java.io.InputStream
import java.sql.{PreparedStatement, Timestamp}

import com.bluedon.trainResult
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import java.util.{Properties, UUID}

import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.phoenix.schema.types.{PInteger, PTimestamp}

import scala.collection.mutable

/**
  * Created by huoguang on 2017/3/22.
  */
trait HbaseProcess {
//建立sc
  def getSparkSession(): SparkSession = {
    val properties:Properties = new Properties();
    val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
    properties.load(ipstream)

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark
  }

//插入簇点表
  def insertSIEMCluster(in: trainResult, CLON: String, time: String, describe: String) = {
    val input: mutable.Map[Int, (Array[(String, Array[Double])], Array[Double])] = in.result
    val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181")
    conn.setAutoCommit(false)
    val state = conn.createStatement()

    try{
      input.map(point => {
        //类别
        val label: Int = point._1
        //该类下所有点 ip,和 坐标
        val array1: Array[(String, Array[Double])] = point._2._1
        //该点到中心点的距离
        val array2: Array[Double] = point._2._2
        val len = (0 to array1.length-1).toList
        len.map(n => {
          val ip: String = array1(n)._1
          val index: String = label.toString
          val DISTANCE: Double = array2(n)
          val row: String = ROWUtils.genaralROW()
          val info = array1(n)._2.mkString("#")
          val sql = new StringBuilder()
            .append("upsert into T_SIEM_KMEANS_CLUSTER (\"ROW\",\"CLNO\",\"NAME\",\"CENTER_INDEX\",\"DISTANCE\",\"CENTER_INFO\",\"CLTIME\",\"DEMO\") ")
            .append("values ('" + row + "','" + CLON + "','" + ip + "','" + index + "'," +DISTANCE + ",'" + info + "','" + Timestamp.valueOf(time) +"','" + describe + "')" )
            .toString()
          state.execute(sql)
          conn.commit()
        })
      })
    } catch {
      case e: Exception => {
        println(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }
//一级聚类插入簇中心点表
  def insertSIEMCenter(in: trainResult, CLON: String, time: String, ty: Int) = {
    val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181")
    conn.setAutoCommit(false)
    val state = conn.createStatement()

    try{
      val centerNum = in.Clusters.length

      for (i <- 0 to centerNum-1) {
        val row: String = ROWUtils.genaralROW()
        val info: String = in.Clusters.apply(i).mkString("#")
        val index = i.toString
        val demo = s"TCP#ICMP#GRE#UDP#time#packernum#bytesize"

        val sql = new StringBuilder()
          .append("upsert into T_SIEM_KMEANS_CENTER (\"ROW\",\"CENTER_INDEX\",\"CENTER_INFO\",\"CLNO\",\"TYPE\",\"CLTIME\",\"DEMO\") ")
          .append("values ('" + row + "','" + index + "','" + info + "','" + CLON + "'," + ty + ",'"  + Timestamp.valueOf(time) +"','" + demo + "')" )
          .toString()
        state.execute(sql)
        conn.commit()
      }
    } catch {
      case e: Exception => {
        println(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }

  //二级聚类插入簇中心点
  def insertSIEMCenterSec(in: trainResult, CLONFather: String, indexFather: Int, CLON: String, time: String, ty: Int) = {
    val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181")
    conn.setAutoCommit(false)
    val state = conn.createStatement()

    try{
      val centerNum = in.Clusters.length

      for (i <- 0 to centerNum-1) {
        val row: String = ROWUtils.genaralROW()
        val info: String = in.Clusters.apply(i).mkString("#")
        val index = i.toString
        val index_p = indexFather.toString
        val demo = s"QQ#FTP#HTTP#TELNET#DBAudit#bd_local_trojan#GET#POST"

        val sql = new StringBuilder()
          .append("upsert into T_SIEM_KMEANS_CENTER (\"ROW\",\"CENTER_INDEX\",\"CENTER_INDEX_P\",\"CLUSTER_INFO\",\"CLNO\",\"CLNO_P\",\"TYPE\",\"CLTIME\",\"DEMO\") ")
          .append("values ('" + row + "','" + index + "','" + index_p + "','" +  info + "','"+ CLON +"','"+ CLONFather +"'," + ty + ",'" + Timestamp.valueOf(time) +"','" + demo + "')" )
          .toString()
        state.execute(sql)
        conn.commit()
      }
    } catch {
      case e: Exception => {
        println(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getSIEMDataFromHbase(sc: SparkContext, sql:SQLContext) = {
    println(s"test 1")
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址
    conf.set("hbase.zookeeper.quorum","10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_NETFLOW")//表名
    val start=Timestamp.valueOf("2017-03-06 00:00:00").getTime
    val end=Timestamp.valueOf("2017-03-07 00:00:00").getTime
    val scan = new Scan()
//    scan.setStartRow(Bytes.toBytes("1000021490287656"))
//    scan.setStopRow(Bytes.toBytes("1004641489851462"))
    scan.setTimeRange(start,end)
    scan.setCaching(10000)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val rdd = hbaseRDD.map(row => {
      val col1 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("SRCIP"))
      val col2 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("DSTIP"))
      val col3 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("STARTTIME"))
      val col4 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("ENDTIME"))
      val col5 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PROTO"))
      val col6 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM"))
      val col7 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("BYTESIZE"))
      val flag=if(col1==null||col2==null||col3==null||col4==null||col4==null||col6==null||col7==null) 0 else 1
//      println(s"col1: " + col1 +s"col2: " + col2+s"col3: " + col3+s"col4: " + col4+s"col5: " + col5+s"col6: " + col6+s"col7: " + col7 +s"col8: " + flag)
      (col1,col2,col3,col4,col5,col6,col7,flag)
    }).filter(_._8 > 0).map(line =>{
      (
        Bytes.toString(line._1),
        Bytes.toString(line._2),
        PTimestamp.INSTANCE.toObject(line._3).toString,
        PTimestamp.INSTANCE.toObject(line._4).toString,
        Bytes.toString(line._5),
        PInteger.INSTANCE.toObject(line._6).toString,
        PInteger.INSTANCE.toObject(line._7).toString
      )
    })
    rdd
  }
//
  def getMONIDataFromHbase(sc: SparkContext, sql:SQLContext) = {
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址
    conf.set("hbase.zookeeper.quorum","10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    conf.set(TableInputFormat.INPUT_TABLE,"T_MONI_FLOW")//表名

    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val rdd: RDD[(String, String, String, String)] = hbaseRDD.map(row => {
      val col1 = Bytes.toString(row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("SRCIP")))
      val col2 = Bytes.toString(row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("DSTIP")))
      val col3 = Bytes.toString(row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("APPPROTO")))
      val col4 = Bytes.toString(row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("METHOD")))
      val flag=if(col1==null||col2==null||col3==null||col4==null||col4==null) 0 else 1
      (col1,col2,col3,col4,flag)
    }).filter(_._5 > 0).map( row =>{
      (row._1,row._2,row._3,row._4)
    })
    rdd
  }

}
