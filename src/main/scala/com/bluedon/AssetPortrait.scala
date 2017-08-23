package com.bluedon

import java.io.{File, InputStream, Serializable}
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import com.bluedon.utils.HbaseProcess
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


/**
  * Created by huoguang on 2017/3/13.
  * 资产画像
  */
//封装下ml库override的Vector
//case class feature (features: Vector)
case class model_instance(ip: String, features: Vector, featuresNor: Vector)

//聚类输出结果封装， important
case class trainResult(Clusters: Array[Array[Double]], result: mutable.Map[Int, (Array[(String, Array[Double])], Array[Double])], allPointsNum: Long)

//聚类簇数量，轮廓系数
case class silhouetteCoefficientResult(clusterNum: Int, value: Double)

//封装下netflow表 11 个字段
case class netFlowColumnBean(startTime: String, endTime: String, proto: String, packernum: Int, bytesize: Int, srcip: String,
                             dstip: String, recordtime: String, recordid: String, srcport: Int, dstport: Int)

//rdd变换后的输出表 5个字段，prote按四种协议类型处理sum
case class netFlowAfterProcess(ip: String, time: Long, prote: String, packernum: Int, bytesize: Int)

case class mapNormalization(original: Array[Double], Normalization: Array[Double])

case class norMaxMin(TCP: container, ICMP: container, GRE: container, UDP: container, time: container, packernum: container, bytesize: container)

case class norMaxMinS(QQ: container, FTP: container, HTTP: container, TELNET: container, DBAudit: container, bd_local_trojan: container, GET: container, POST: container)

case class container(x: Double, n: Double)

case class moniflowAfterProcess(ip: String, appproto: String, method: String)


object AssetPortrait extends HbaseProcess {
  //储存一级聚类结果 map( 聚类个数 -> ip组)
  val firstClusterResult = scala.collection.mutable.Map[Int, List[List[String]]]()
  //保存一级聚类结果信息
  val trainResultFirst: mutable.Map[Int, trainResult] = mutable.Map[Int, trainResult]()
  //保存二级聚类结果信息
  val trainResultSecMap: mutable.Map[Int, List[trainResult]] = mutable.Map[Int, List[trainResult]]()
  //干掉spark无用日志
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    //生成本批次的时间
    val CLTIME = getNowTime
    //生成本次计算的批次信息，UUID
    val CLNO = UUID.randomUUID().toString
    val CLNOSec = UUID.randomUUID().toString
    // 第一次聚类指标描述
    val describeForFirst = s"TCP#ICMP#GRE#UDP#time#packernum#bytesize"
    // 第二次聚类指标描述
    val describeForSec = s"QQ#FTP#HTTP#TELNET#DBAudit#bd_local_trojan#GET#POST"
//    val first = s"hdfs://172.16.12.38:9000/spark/hgTest/trainData.txt"
//    val second = s"hdfs://172.16.12.38:9000/spark/wzd_test/moni_flow_new.txt"
    // mutable.Map  装载 ip -> (Array(TCP, ICMP, GRE, UDP,SUM(time), SUM(packernum), SUM(bytesize), 归一化后的Array))
    val sumMap = scala.collection.mutable.Map[String, (Array[Double], Array[Double])]()

    val spark = getSparkSession()
    val sc = spark.sparkContext
    val sql = spark.sqlContext

    // 从hbase T_MONI_NETFLOW 表读取 SRCIP，DSTIP，STARTTIME，ENDTIME，PROTO，PACKERNUM，BYTESIZE 七个字段
    val data: RDD[(String, String, String, String, String, String, String)] = getSIEMDataFromHbase(sc, sql)
    // 从hbase T_SIEM_NETFLOW 表读取 SRCIP，DSTIP，APPPROTO，METHOD四个字段
    val data2 = getMONIDataFromHbase(sc, sql)

    import sql.implicits._
    println(s"show after filter")
    data.toDF().show(5)
    //过滤给定IP区段
    val ipRegion = List("221.176.64", "221.176.65", "221.176.68","221.176.70","221.176.71","221.176.72","221.176.73","221.176.74","221.176.75","221.176.76","221.176.77","221.176.78","221.176.79")
    data.filter(row =>
      ipRegion.contains(ipSplit(row._1)) || ipRegion.contains(ipSplit(row._2))
    ).map(row => {
      var ip:String = row._2
      if (ipRegion.contains(ipSplit(row._1))) {
        ip = row._1
      }
      val time: Long = getMillionSubtract(row._4)-getMillionSubtract(row._3)
      //"ip", "time","prote","packernum","bytesize"  注册成临时表 netFlowTemp
      netFlowAfterProcess(ip, time, row._5, row._6.toInt, row._7.toInt)
    }).toDF("ip", "time","prote","packernum","bytesize").createOrReplaceTempView("netFlowTemp")
    println(s"test 3 ")
    val process = sql.sql(getSqlStrF())
    println(s"show after group")
    process.show(10)
    process.createOrReplaceTempView("netFlowStatistics")
    sql.dropTempTable(s"netFlowTemp")
    //各个字段max， min 值  用于归一化处理
    val re: List[Double] = sql.sql(
      s"""select max(time), min(time), max(packernum), min(packernum), max(bytesize), min(bytesize), max(TCP), min(TCP), max(ICMP), min(ICMP),
         |max(GRE), min(GRE), max(UDP), min(UDP) from netFlowStatistics
       """.stripMargin).collect().map(x => List(aD(x(0)), aD(x(1)), aD(x(2)), aD(x(3)), aD(x(4)), aD(x(5)), aD(x(6)), aD(x(7)), aD(x(8)), aD(x(9))
      , aD(x(10)), aD(x(11)), aD(x(12)), aD(x(13)))).head
    sql.dropTempTable(s"netFlowStatistics")
    // /储存所有数据的极值信息 用于归一化处理 TCP, ICMP, GRE, UDP, time, packernum, bytesize
    val maxMin: norMaxMin = norMaxMin(container(re(6), re(7)), container(re(8), re(9)), container(re(10), re(11)), container(re(12), re(13)), container(re(0), re(1)),
      container(re(2), re(3)), container(re(4), re(5)))
    //
    val processRdd = process.rdd
    processRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(s"[AssetPortrait] after Persist !!")
    processRdd.collect().map(row => {
      val ar = Array(row(4), row(5), row(6), row(7), row(1), row(2), row(3)).map(_.toString.toDouble)
      val arNor = Array(tF(ar(0), maxMin.TCP), tF(ar(1), maxMin.ICMP), tF(ar(2), maxMin.GRE), tF(ar(3), maxMin.UDP),
        tF(ar(4), maxMin.time), tF(ar(5), maxMin.packernum), tF(ar(6), maxMin.bytesize))
      sumMap += (row(0).toString -> (ar, arNor))
    })
    //初步确定聚类簇数量3-20
    val clusterNum: List[Int] = (5 to 5).toList
    val coefficients: mutable.Map[Double, Int] = mutable.Map[Double, Int]()
    clusterNum.foreach(n => {
      println(s"############ Train begin !!!, clusterNum is $n !!!")
      // 训练, 包含第一次训练的所有输出结果
      val trainResult: trainResult = kMeansTrain(n, 100, sumMap, sc, sql)
      trainResultFirst += (n -> trainResult)
      // 求轮廓系数
      val coefficient: silhouetteCoefficientResult = silhouetteCoefficient(trainResult)
      println(s"####! " + coefficient.value)
      coefficients += (coefficient.value -> n)
    })
    coefficients.keys.foreach(key => {
      println(s"Coefficients Map: $key ----------> ${coefficients.getOrElse(key, 0)}")
    })


    println(s"Max is: " + coefficients.keys.max)
    val elClusterNum = coefficients.getOrElse(coefficients.keys.max, 0)
    if (elClusterNum == 0) {
      println(s"Error during select the cluster num")
      return
    }
    println(s"###################### the first level cluster finish #############################")
    println(s"The best cluster num  is:" + elClusterNum)
    println(s"############# begin the second level cluster with ipGroups of the first ############")
    //  第一级聚类完成,存入hbase表
    val saveFirst: trainResult = trainResultFirst.get(elClusterNum).get

    insertSIEMCluster(saveFirst, CLNO, CLTIME, describeForFirst)
    insertSIEMCenter(saveFirst, CLNO, CLTIME, 1)

    //根据选定的分组数目获得ip组,开始二级聚类处理
    val ipGroup: List[List[String]] = getFirstClusterResult.get(elClusterNum).get
    val nn = ipGroup.filter(x => x.length > 3)

    println(s"Num of groups is: " + nn.length)
    val filterIp: List[DataFrame] = nn.map(group => {
      println(s"Num of ip from this group is: " + group.length)
      val ips: List[String] = group
      data2.filter(row => group.contains(row._1) || group.contains(row._2)).map(row => {
        var ip:String = row._2
        if (group.contains(row._1)) {
          ip = row._1}
        moniflowAfterProcess(ip,row._3,row._4)
      }).toDF()
    })

    println(s"Num of groups after filter is: " + filterIp.length)
    val groups: List[DataFrame] = filterIp.map(df => {
      println(s"############Second Train begin !!!, process one of the ipGroup")
      //      println(s"Num of ip from this group before filter is: ${df.count()}")
      df.createOrReplaceTempView(s"moniFlowTmp")
      val processSec = sql.sql(getSqlStrS())
      //      println(s"Num of ip from this group after filter is: ${processSec.count()}")
      processSec
    }).filter(x => x.count() > 3)

    val resultSecond =  groups.map(processSec => {
      processSec.createOrReplaceTempView("moniFlowGroupTmp")
      //最大最小值//返回数组//
      val re: List[Double] = sql.sql(
        s"""select max(QQ), min(QQ), max(FTP), min(FTP), max(HTTP), min(HTTP), max(TELNET), min(TELNET), max(DBAudit), min(DBAudit),
           |max(bd_local_trojan), min(bd_local_trojan), max(GET), min(GET), max(POST), min(POST) from moniFlowGroupTmp
             """.stripMargin).collect().map(x => List(aD(x(0)), aD(x(1)), aD(x(2)), aD(x(3)), aD(x(4)), aD(x(5)), aD(x(6)), aD(x(7)), aD(x(8)), aD(x(9)), aD(x(10)), aD(x(11)), aD(x(12)), aD(x(13)), aD(x(14)), aD(x(15)))).head
      val maxMin = norMaxMinS(container(re(0), re(1)), container(re(2), re(3)), container(re(4), re(5)), container(re(6), re(7)), container(re(8), re(9)),
        container(re(10), re(11)), container(re(12), re(13)), container(re(14), re(15)))
      val processRddS = processSec.rdd
      processRddS.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //将处理后的数据添加到MAP创建的映射中
      var sumMapS = scala.collection.mutable.Map[String, (Array[Double], Array[Double])]()
      processRddS.collect().map(row => {
        val a = row
        val ar = Array(row(1), row(2), row(3), row(4), row(5), row(6), row(7), row(8)).map(_.toString.toDouble)
        val arNor = Array(tF(ar(0), maxMin.QQ), tF(ar(1), maxMin.FTP), tF(ar(2), maxMin.HTTP), tF(ar(3), maxMin.TELNET),
          tF(ar(4), maxMin.DBAudit), tF(ar(5), maxMin.bd_local_trojan), tF(ar(6), maxMin.GET), tF(ar(7), maxMin.POST))
        sumMapS += (row(0).toString -> (ar, arNor))
      })
      //
      val coefficientsSec: mutable.Map[Double, Int] = mutable.Map[Double, Int]()
      val trainResultSecond: mutable.Map[Int, trainResult] = mutable.Map[Int, trainResult]()
      clusterNum.foreach(n => {
        println(s"############Second Train begin !!!, clusterNum is $n !!!")
        println(s"############Second Train begin !!!, ip size is ${sumMapS.size} !!!")

        if (n > sumMapS.size) {
          println(s"data size < clusters num, set the min coefficients")
          coefficientsSec += (-1.0 -> n)
        } else {
          val trainResult: trainResult = kMeansTrainSec(n, 100, sumMapS, sc, sql)
          trainResultSecond += (n -> trainResult)
          // 求轮廓系数
          val coefficient: silhouetteCoefficientResult = silhouetteCoefficient(trainResult)
          println(s"####! " + coefficient.value)
          coefficientsSec += (coefficient.value -> n)
        }
      })
      coefficientsSec.keys.foreach(key => {
        println(s"coefficientsSec Map: $key ----------> ${coefficientsSec.getOrElse(key, 0)}")
      })

      val qu: Iterable[Double] = coefficientsSec.keys
      println(s"!! max: " + qu.max)


      val elClusterNum = coefficientsSec.getOrElse(coefficientsSec.keys.max, 0)
      if (elClusterNum == 0) {
        println(s"Error during select the cluster num")
        return
      }
      val res: trainResult = trainResultSecond.get(elClusterNum).get
      res
    })
    val aaa: List[trainResult] = resultSecond
    //TODO  输出第二次聚类结果
    for (i <- 0 to resultSecond.length -1) {
      insertSIEMCluster(resultSecond(i), CLNOSec, CLTIME, describeForSec)
      insertSIEMCenterSec(resultSecond(i), CLNO, i, CLNOSec, CLTIME, 2)
    }
    println(s"Start time: " + CLTIME)
    val finishTime = getNowTime()
    println(s"End time: " + finishTime)
  }

  private def getFirstClusterResult = this.firstClusterResult

  /**
    * 切割ip用于配制ip段
    */
  def ipSplit(str: String) = {
    str.replace(".", "#").split("#").dropRight(1).mkString(".")
  }

  /**l/
    * any to double
    */
  def aD(in: Any) = {
    in.toString.toDouble
  }

  def getSqlStrF(): String = {
    val str =
      s"""SELECT ip as ip, SUM(time) as time, SUM(packernum) as packernum, SUM(bytesize) as bytesize,
         |COUNT(CASE WHEN prote = 'TCP' THEN 'TCP' ELSE NULL END) as TCP,
         |COUNT(CASE WHEN prote = 'ICMP' THEN 'ICMP' ELSE NULL END) as ICMP,
         |COUNT(CASE WHEN prote = 'GRE' THEN 'GRE' ELSE NULL END) as GRE,
         |COUNT(CASE WHEN prote = 'UDP' THEN 'UDP' ELSE NULL END) as UDP
         |FROM netFlowTemp GROUP BY ip""".stripMargin
    str
  }

  def getSqlStrS() = {
    val sqlstr =
      s"""SELECT ip as ip,
         |COUNT(CASE WHEN appproto='QQ' THEN 1 ELSE NULL END) as QQ,
         |COUNT(CASE WHEN appproto='FTP' THEN 1 ELSE NULL END) as FTP,
         |COUNT(CASE WHEN appproto='HTTP' THEN 1 ELSE NULL END) as HTTP,
         |COUNT(CASE WHEN appproto='TELNET' THEN 1 ELSE NULL END) as TELNET,
         |COUNT(CASE WHEN appproto='DBAudit' THEN 1 ELSE NULL END) as DBAudit,
         |COUNT(CASE WHEN appproto='bd-local-trojan' THEN 1 ELSE NULL END) as bd_local_trojan,
         |COUNT(CASE WHEN method='GET' THEN 1 ELSE NULL END) as GET,
         |COUNT(CASE WHEN method='POST' THEN 1 ELSE NULL END) as POST
         |FROM moniFlowTmp GROUP BY ip""".stripMargin
    sqlstr
  }
  /**
    * 计算归一化
    */

  private def tF(in: Double, con: container) = {
    val max = con.x
    val min = con.n
    (in - min) / (max - min)

  }


  def createRemoteDir(resultDir: String): Unit = {
    val file = new File(resultDir)
    if (!file.exists() && !file.isDirectory) {
      file.mkdir()
      println(s"[AssetPortrait] mkdir successfully , Path is $resultDir")
    } else {
      println(s"[AssetPortrait] mkdir is already exist")
    }
  }

  /**
    * 一级聚类的训练入口
    */
  def kMeansTrain(numClusters: Int, numIterations: Int, data: mutable.Map[String, (Array[Double], Array[Double])], sc: SparkContext, sql: SQLContext) = {
    val data1: List[(String, Array[Double], Array[Double])] = data.map(x => (x._1, x._2._1, x._2._2)).toList
    import sql.implicits._
    val df1: RDD[(String, Array[Double], Array[Double])] = sc.makeRDD(data1)
    val df: DataFrame = sc.makeRDD(data1).map(x => {
      val s1 = x._2
      val s2 = x._3
      model_instance(x._1.toString, Vectors.dense(Array(s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6))), Vectors.dense(Array(s2(0), s2(1), s2(2), s2(3), s2(4), s2(5), s2(6))))
    }).toDF()

    //    val initMode = s"k-means||"
    val kmeansmodel: KMeansModel = new KMeans().setK(numClusters).setMaxIter(numIterations).setFeaturesCol("featuresNor")
      .setPredictionCol("prediction").fit(df)
    val result: DataFrame = kmeansmodel.transform(df)
    result.createOrReplaceTempView(s"tempView")
    //计算簇的个数
    val indexs: Array[Int] = sql.sql(s"select distinct prediction from tempView").collect().map(x => x(0)).map(_.toString.toInt)
    println(s"[AssetPortrait] indexs is ${indexs.toList}")
    val cluNum = indexs.length
    println(s"[AssetPortrait] clusters number is $cluNum")
    //中心点
    val clusterCenters: Array[Array[Double]] = kmeansmodel.clusterCenters.map(_.toArray)
    clusterCenters.foreach(x => {
      println(s"[AssetPortrait] cluster center is: " + x.toList)
    })
    //所有点
    var allPointsNum: Long = 0
    val resultMap: mutable.Map[Int, (Array[(String, Array[Double])], Array[Double])] = scala.collection.mutable.Map[Int, (Array[(String, Array[Double])], Array[Double])]()
    indexs.foreach(index => {
      val str = s"select ip ,features from  tempView where prediction = $index"
      //      sql.sql(str).show()
      val df: Array[(String, Array[Double])] = sql.sql(str).collect().map(x => {
        (x(0).toString, x(1).toString.drop(1).dropRight(1).split(",").map(_.toDouble))
      }
      )
      allPointsNum += df.length
      //每个点到该点所在中心的距离
      val distance = df.map(point => {
        getEucDistance(clusterCenters(index), point._2)
      })

      resultMap += (index -> (df, distance))

      println(s"[AssetPortrait] index $index --------------->  cul number is ${resultMap.get(index).get._1.length}")
    })

    println(s"[AssetPortrait] total points ---------------> $allPointsNum")

    var totalIp: List[List[String]] = Nil
    (0 to cluNum - 1).foreach(x => {
      val ip: List[String] = sql.sql(s"select ip from tempView where prediction = $x").collect().map(x => x(0).toString).toList
      totalIp = totalIp :+ ip
    })
    //保存到内存
    getFirstClusterResult += (cluNum -> totalIp)

    trainResult(clusterCenters, resultMap, allPointsNum)
  }

  /**
    * 二级聚类的训练入口
    */
  def kMeansTrainSec(numClusters: Int, numIterations: Int, data: mutable.Map[String, (Array[Double], Array[Double])], sc: SparkContext, sql: SQLContext) = {
    println(s"kMeansTrainSec ip size is:" + data.size)
    val data1: List[(String, Array[Double], Array[Double])] = data.map(x => (x._1, x._2._1, x._2._2)).toList
    import sql.implicits._
    val df1: RDD[(String, Array[Double], Array[Double])] = sc.makeRDD(data1)
    val df: DataFrame = sc.makeRDD(data1).map(x => {
      val s1 = x._2
      val s2 = x._3
      model_instance(x._1.toString, Vectors.dense(Array(s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7))),
        Vectors.dense(Array(s1(0), s1(1), s1(2), s1(3), s1(4), s1(5), s1(6), s1(7))))
    }).toDF()

    //    val initMode = s"k-means||"
    val kmeansmodel: KMeansModel = new KMeans().setK(numClusters).setMaxIter(numIterations).setFeaturesCol("featuresNor")
      .setPredictionCol("prediction").fit(df)
    val result: DataFrame = kmeansmodel.transform(df)
    result.show(20)

    result.createOrReplaceTempView(s"tempViewSec")
    //计算簇的个数
    val indexs: Array[Int] = sql.sql(s"select distinct prediction from tempViewSec").collect().map(x => x(0)).map(_.toString.toInt)
    println(s"[AssetPortraitSec] indexs is ${indexs.toList}")
    val cluNum = indexs.length
    println(s"[AssetPortraitSec] clusters number is $cluNum")

    //中心点
    val clusterCenters: Array[Array[Double]] = kmeansmodel.clusterCenters.map(_.toArray)
    clusterCenters.foreach(x => {
      println(s"[AssetPortraitSec] cluster center is: " + x.toList)
    })

    //所有点
    var allPointsNum: Long = 0
    val resultMap: mutable.Map[Int, (Array[(String, Array[Double])], Array[Double])] = scala.collection.mutable.Map[Int, (Array[(String, Array[Double])], Array[Double])]()
    indexs.foreach(index => {
      val str = s"select ip, features from  tempViewSec where prediction = $index"
      val df: Array[(String, Array[Double])] = sql.sql(str).collect().map(x => {
        (x(0).toString, x(1).toString.drop(1).dropRight(1).split(",").map(_.toDouble))
      }
      )
      allPointsNum += df.length
      //每个点到该点所在中心的距离
      val distance = df.map(point => {
        getEucDistance(clusterCenters(index), point._2)
      })

      resultMap += (index -> (df, distance))

      println(s"[AssetPortraitSec] index $index --------------->  cul number is ${resultMap.get(index).get._1.length}")
    })
    println(s"[AssetPortraitSec] total points ---------------> $allPointsNum")
    trainResult(clusterCenters, resultMap, allPointsNum)
  }

  /**
    * 计算欧式距离
    */

  def getEucDistance(point: Array[Double], center: Array[Double]): Double = {
    var distance: Double = 0.0
    try {
      distance = Math.sqrt(point.zip(center).map {
        case (x: Double, y: Double) => Math.pow(y - x, 2)
      }.sum
      )
    } catch {
      case e: Exception => {
        println(s"[AssetPortrait] Some error occur during getEucDistance !!: " + e.getMessage)
      }
    }
    distance
  }

  /**
    * 计算轮廓系数用于遍历求K
    */

  def silhouetteCoefficient(processData: trainResult): silhouetteCoefficientResult = {
    def helper(ava: Double, min: Double): Double = {
      if (Math.max(ava, min) == 0.0) {
        0.0
      } else {
        (min - ava) / Math.max(ava, min)
      }
    }

    val totalPointNum: Long = processData.allPointsNum
    val clusters: Array[Array[Double]] = processData.Clusters
    val clusterResult: mutable.Map[Int, (Array[(String, Array[Double])], Array[Double])] = processData.result
    val number = clusters.length
    val indexArray = new Array[Int](number)
    var distance = List
    for (i <- 0 to number - 1) {
      indexArray(i) = i
    }

    val result = indexArray.map(index => {
      val pointsOfTheIndex: Array[Array[Double]] = clusterResult.get(index).get._1.map(_._2)
      val pointsOfOtherOuter = indexArray.filterNot(n => n == index).map(x => {
        clusterResult.get(index).get._1.map(_._2).apply(0)
      })
      pointsOfTheIndex.map(point => {
        val avaInner = pointsOfTheIndex.map(x => {
          getEucDistance(x, point)
        }).toList.sum / pointsOfTheIndex.length

        val minOutter = pointsOfOtherOuter.map(y => {
          getEucDistance(y, point)
        }).min
        helper(avaInner, minOutter)
      })
    })
    silhouetteCoefficientResult(number, result.flatten.sum / totalPointNum)
  }

  /**
    * 本地数据读取测试用
    */

  def getBasicDataFromHdfs(hdfsPath: String): (RDD[String], SparkSession) = {
    val properties: Properties = new Properties();
    val ipstream: InputStream = this.getClass().getResourceAsStream("/manage.properties")
    properties.load(ipstream)

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val spark: SparkSession = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext
    //    import sqlContext.implicits._
    val rdd: RDD[String] = sc.textFile(hdfsPath)
    (rdd, spark)
  }

  /**
    * 转化时间类型并求毫秒差
    * exp ： 1970-01-05 01:23:56.297
    */

  def getMillionSubtract(in: String): Long = {
    val sp = in.replace(".", "#").split("#")
    var result: Long = 1
    try {
      val (first, second) = (sp(0), sp(1))
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      result = fm.parse(first).getTime + second.toLong
    } catch {
      case e: Exception => {
        println(s"[AssetPortrait] Some error occur during getMillionSubtract: " + e.getMessage)
      }
    }
    result
  }

  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }
}



