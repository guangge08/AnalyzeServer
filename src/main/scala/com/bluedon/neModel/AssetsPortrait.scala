package com.bluedon.neModel
import java.io.InputStream

import com.bluedon.utils.DateUtils
import org.apache.spark.sql.{Dataset, Encoder, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.util.Bytes
import java.lang.Long
import java.lang.Math
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}

import net.sf.json.{JSONArray, JSONObject}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.mesos.Protos.Value.Scalar
import org.apache.phoenix.schema.types.{PDouble, PInteger, PTimestamp}
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler, StandardScalerModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

import scala.util.control.Breaks._

case class netflowEsObj(rowkey:String)
case class netflowStatic(DSTIP:String,RECORDTIME:Double,FEATURES:Vector)

case class StaticResult(DSTIP:String,FEATURES:Vector,SCLFEATURES:Vector,PREDICTION:Int,DISTANCE:Double)
/**
  * Created by dengxiwen on 2017/3/15.
  * 资产画像
  */
class AssetsPortrait {

  /**
    * 资产画像
    */
  def assetsPortraitMain(spark:SparkSession,startTime:String,endTime:String):Unit={
    val sparkConf = spark.sparkContext
    //准备数据
    println("###########################准备数据##################################")
    val readydata = dataReady(sparkConf,startTime,endTime)
    val hdfsPath = "hdfs://10.252.47.211:9000/tempKMean3.dm"
    //数据暂存hdfs
    saveDataHDFS(readydata,hdfsPath)
    println("###########################数据保存成功HDFS##################################")
    val readydataRead = dataReadFromHdfs(sparkConf,hdfsPath)
    println("###########################总数据记录##################################" + readydata.count())


    //组织数据
    println("###########################组织数据##################################")
    val initdata = dataInit(spark,readydataRead)
    //模型训练
    println("###########################开始模型训练##################################")
    val model = kMeansModel(spark,initdata,2)
    println("###########################打印中心点##################################")
    val clusterCenters = model._1
    //打印中心点
    var clusterIndex:Int = 0
    clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })
    println("###########################打印簇点##################################")
    val  points:Dataset[StaticResult] = model._2.asInstanceOf[Dataset[StaticResult]]
    points.show(10)
  }

  /**
    * 数据组织准备
    *
    * @param startTime
    * @param endTime
    */
  def dataReady(sparkConf:SparkContext,startTime:String,endTime:String): RDD[(String, Double, Double, Double, Double, Double, Vector)]  = {
    //读取配置属性
    val properties:Properties = new Properties();
    val ipstream:InputStream=this.getClass().getResourceAsStream("/manage.properties");
    properties.load(ipstream);

    var netflowDataset:RDD[(String, Double, Double, Double, Double, Double, Vector)] = null
    val zkHost = properties.getProperty("zookeeper.host")
    val zkHostBD = sparkConf.broadcast(zkHost)
    val zkPort = properties.getProperty("zookeeper.port")
    val zkPortBD = sparkConf.broadcast(zkPort)
    val options = Map("pushdown" -> "true", "es.nodes" -> properties.getProperty("es.nodes"), "es.port" -> properties.getProperty("es.port"))
    val query="{\"query\":{\"range\":{\"recordtime\":{\"gte\":"+DateUtils.dateToStamp(startTime).toLong+",\"lte\":"+DateUtils.dateToStamp(endTime).toLong+"}}}}"
    val sqlContext = new SQLContext(sparkConf)
    import sqlContext.implicits._
    import org.elasticsearch.spark._
    import org.elasticsearch.spark.sql._
    //从ES读取数据
    val netflowEs = sparkConf.esRDD("soc/netflow",query,options).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var netflowsRDD:DataFrame = null
    if(netflowEs.count()>0){
      //把数据转换格式
      var netflowEsDF = netflowEs.map(row=>{
        netflowEsObj(row._2.get("row").get.toString)
      })

      println("###########################ES记录总数##################################" + netflowEsDF.count())
      val EsCount = netflowEsDF.count()
      val partitionPage = 10000
      val partitionNum:Int = (EsCount/partitionPage).toInt
      //分区去读取hbase数据（根据rowkey)
      netflowDataset = netflowEsDF.repartition(partitionNum).mapPartitions(netflowIt=>{
        var netflowItResult = List[(String,Double,Double,Double,Double,Double,Vector)]()
        val hBaseConf = HBaseConfiguration.create()
        hBaseConf.set("hbase.zookeeper.property.clientPort", zkPortBD.value)
        hBaseConf.set("hbase.zookeeper.quorum", zkHostBD.value)
        hBaseConf.set(TableInputFormat.INPUT_TABLE, "T_SIEM_NETFLOW")
        hBaseConf.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 1200000)
        hBaseConf.set(HConstants.HBASE_CLIENT_PAUSE, "50");
        hBaseConf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "3");
        hBaseConf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "2000000");
        hBaseConf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "3000");
        hBaseConf.set(HConstants.HBASE_CLIENT_SCANNER_CACHING, "100");
        hBaseConf.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "1200000");

        val tablename = "T_SIEM_NETFLOW"
        val table: HTable = new HTable(hBaseConf, Bytes.toBytes(tablename));
        var gets = new util.ArrayList[Get]()

        while(netflowIt.hasNext){
          var netflowObj:netflowEsObj = netflowIt.next()
          val rowkey = netflowObj.rowkey
          val get: Get = new Get(Bytes.toBytes(rowkey.toString));
          get.addFamily(Bytes.toBytes("NETFLOW"))
          get.addColumn(Bytes.toBytes("NETFLOW"), Bytes.toBytes("DSTIP"))
          get.addColumn(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PROTO"))
          get.addColumn(Bytes.toBytes("NETFLOW"), Bytes.toBytes("STARTTIME"))
          get.addColumn(Bytes.toBytes("NETFLOW"), Bytes.toBytes("ENDTIME"))
          get.addColumn(Bytes.toBytes("NETFLOW"), Bytes.toBytes("FLOWNUM"))
          get.addColumn(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM"))
          get.addColumn(Bytes.toBytes("NETFLOW"), Bytes.toBytes("RECORDTIME"))
          gets.add(get)
        }
        val results = table.get(gets)
        for (netflow <- results) {

          var DSTIP = "" //目的IP
          var PROTO: Double = -1.0 //协议
          var TIMEDIFF = -1.0 //时间差
          var FLOWNUM = 0.0 //流量
          var PACKERNUM = 0.0 //包数
          var RECORDTIME = 0.0 //发包时间
          var FEATURES: Vector = null
          var STARTTIME: Double = 0.0 //流量开始时间
          var ENDTIME: Double = 0.0 //流量结束时间

          if (netflow != null && !netflow.isEmpty) {

            if (netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("DSTIP")) != null) {
              DSTIP = Bytes.toString(netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("DSTIP"))) //目的IP
            }

            if (netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PROTO")) != null) {
              //协议
              PROTO = Bytes.toString(netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PROTO"))).toUpperCase.toString match {
                case "TCP" => 1.0
                case "UDP" => 2.0
                case "ICMP" => 3.0
                case "GRE" => 4.0
              }
            }

            if (netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("STARTTIME")) != null && !netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("STARTTIME")).toString.toLowerCase.equals("null")) {
              //流量开始时间
              STARTTIME = Timestamp.valueOf(PTimestamp.INSTANCE.toObject(netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("STARTTIME"))).toString).getTime.toDouble
            }

            if (netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("ENDTIME")) != null && !netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("ENDTIME")).toString.toLowerCase.equals("null")) {
              //流量结束时间
              ENDTIME = Timestamp.valueOf(PTimestamp.INSTANCE.toObject(netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("ENDTIME"))).toString).getTime.toDouble
            }

            if (netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("FLOWNUM")) != null && !netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("FLOWNUM")).toString.toLowerCase.equals("null")) {
              //流量字节
              FLOWNUM = PInteger.INSTANCE.toObject(netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("FLOWNUM"))).toString.toDouble
            }

            if (netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM")) != null && !netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM")).toString.toLowerCase.equals("null")) {
              //包数
              PACKERNUM = PInteger.INSTANCE.toObject(netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM"))).toString.toDouble
            }

            if (netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("RECORDTIME")) != null && !netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("RECORDTIME")).toString.toLowerCase.equals("null")) {
              //流量发生时间
              RECORDTIME = Timestamp.valueOf(PTimestamp.INSTANCE.toObject(netflow.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("RECORDTIME"))).toString).getTime
            }
            //时间差
            TIMEDIFF = ENDTIME - STARTTIME
            //特征值
            FEATURES = Vectors.dense(Array(PROTO, TIMEDIFF, FLOWNUM, PACKERNUM)) //特征值

          }
          netflowItResult = netflowItResult.::((DSTIP, PROTO, TIMEDIFF, FLOWNUM, PACKERNUM, RECORDTIME, FEATURES))
        }
        netflowItResult.iterator
      })
    }
    netflowDataset
  }

  /**
    * 数据存储到HDFS
    * @param netflowDataset
    * @param hdfsPath
    */
  def saveDataHDFS(netflowDataset:RDD[(String, Double, Double, Double, Double, Double, Vector)],hdfsPath:String):Unit ={
    //保存预数据到HDFS
    netflowDataset.saveAsObjectFile(hdfsPath)
  }

  /**
    * 从HDFS读取预备数据
    * @param sparkConf
    * @param hdfsName
    * @return
    */
  def dataReadFromHdfs(sparkConf:SparkContext,hdfsName:String):DataFrame = {
    val sqlContext = new SQLContext(sparkConf)
    import sqlContext.implicits._
    val datas:RDD[(String, Double, Double, Double, Double, Double, Vector)] = sparkConf.objectFile[(String, Double, Double, Double, Double, Double, Vector)](hdfsName)
    val dataDF = datas.toDF("DSTIP","PROTO","TIMEDIFF","FLOWNUM","PACKERNUM","RECORDTIME","FEATURES")
    dataDF
  }

  /**
    * 组织数据
    *
    * @param spark
    * @param netflows
    * @return
    */

  def dataInit(spark:SparkSession,netflows:DataFrame ):Dataset[netflowStatic] ={
    import spark.implicits._
    var netflowsRDD:Dataset[netflowStatic] = null
    if(netflows != null){
      netflowsRDD = netflows.map(netflow=>
        netflowStatic(netflow.getAs[String]("DSTIP"),netflow.getAs[Double]("RECORDTIME"),netflow.getAs[Vector]("FEATURES"))
      )
      println("###########################打印DataSet##################################")
      netflowsRDD.show(10)
    }
    netflowsRDD

  }

  /**
    * 训练KMEANS模型
    *
    * @return
    */

  def kMeansModel(spark:SparkSession,netflows:Dataset[netflowStatic],itNum:Int ):(Array[Vector],Dataset[StaticResult]) = {
    import spark.implicits._
    var clusterCenters:Array[Vector] = null
    var points:Dataset[StaticResult] = null
    if(netflows != null){
      //归一化处理
      val scaler = new StandardScaler().setInputCol("FEATURES").setOutputCol("SCLFEATURES").fit(netflows)
      val sclNetflows = scaler.transform(netflows)
      println("###########################打印归一化的数据##################################")
      sclNetflows.show(10)
      println(scaler)
      var numClusters = 2 //预测分为2个簇类
      var kmeans = new KMeans().setK(numClusters).setSeed(1L)
      var kMeansModel:KMeansModel = kmeans.setFeaturesCol("SCLFEATURES").fit(sclNetflows)
      var WSSSE = kMeansModel.computeCost(sclNetflows)
      //println(WSSSE)
      for(i <- 3 to itNum){
        numClusters = i
        val kmeans_I = new KMeans().setK(numClusters).setSeed(1L)
        val kMeansModel_I :KMeansModel = kmeans_I .setFeaturesCol("SCLFEATURES").fit(sclNetflows)
        val WSSSE_I  = kMeansModel_I .computeCost(sclNetflows)
        if(WSSSE_I<WSSSE){
          kMeansModel = kMeansModel_I
        }
      }


      clusterCenters = kMeansModel.clusterCenters
      val centersList = clusterCenters.toList
      val centersListBD = spark.sparkContext.broadcast(centersList)
      points = kMeansModel.transform(sclNetflows).map(row=>{
        val centersListBDs = centersListBD.value
        val DSTIP = row.getAs[String]("DSTIP")
        val FEATURES = row.getAs[Vector]("FEATURES")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        val PREDICTION = row.getAs[Int]("prediction")
        var DISTANCE:Double = 0
        var index:Int = 0
        val centerPoint:Vector = centersListBDs(PREDICTION)
        DISTANCE = math.sqrt(centerPoint.toArray.zip(SCLFEATURES.toArray).
          map(p => p._1 - p._2).map(d => d*d).sum)

        StaticResult(DSTIP,FEATURES,SCLFEATURES,PREDICTION,DISTANCE)
      })
    }

    (clusterCenters,points)
  }

}
