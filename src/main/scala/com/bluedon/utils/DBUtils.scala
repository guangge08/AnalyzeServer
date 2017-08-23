package com.bluedon.utils

import org.logicalcobwebs.proxool.configuration.JAXPConfigurator
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord,ProducerConfig}

/**
  * Created by dengxiwen on 2017/1/4.
  */
class DBUtils{
  //val conn:Connection

  def getPhoniexConnect(connUrl:String):Connection={

    val url:String = "jdbc:phoenix:" + connUrl
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    val conn:Connection = DriverManager.getConnection(url)
    conn
  }
  /*
  def getPhoniexConnect(connUrl:String):Connection={


    Class.forName("org.logicalcobwebs.proxool.ProxoolDriver");
    val conn:Connection = DriverManager.getConnection("proxool.phoenix:jdbc:phoenix:aqtsgzpt215,aqtsgzsjfxpt213,aqtsgzsjfxpt212,aqtsgzsjfxpt211:2181");
    conn
  }
  */
  def getPostgresConnect():Connection={
    //Java应用中先要加载配置文件，否则谁知道你配置给谁用的
    JAXPConfigurator.configure("/proxool.xml", false);
    //1：注册驱动类，这次这个驱动已经不是Oracle的驱动了，是Proxool专用的驱动
    Class.forName("org.postgresql.Driver");
    //2：创建数据库连接，这个参数是一个字符串，是数据源的别名，在配置文件中配置的timalias，参数格式为：proxool.数据源的别名
    val conn:Connection = DriverManager.getConnection("postgresql.alias");
    conn
  }

  def getKafkaProducer(kafkalist:String):KafkaProducer[String, String] = {
    val props = new util.HashMap[String,Object]();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkalist);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    val producer:KafkaProducer[String, String] = new KafkaProducer[String, String](props);
    producer
  }

  def  sendKafkaList(topic:String,message:String,producer:KafkaProducer[String, String])={
    val topicMsg = new ProducerRecord[String, String](topic,null,message)
    producer.send(topicMsg)
  }
}
