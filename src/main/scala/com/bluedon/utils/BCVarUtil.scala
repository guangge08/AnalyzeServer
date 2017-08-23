package com.bluedon.utils

import net.sf.json.JSONArray
import org.apache.spark.broadcast.Broadcast


/**
  * Created by dengxiwen on 2017/7/21.
  */
class BCVarUtil {

  //规则map
  private var map:scala.collection.mutable.Map[String,Broadcast[Map[String, Map[String, JSONArray]]]] = _
  private var refreshTime:Long = _

  def getBCMap():scala.collection.mutable.Map[String,Broadcast[Map[String, Map[String, JSONArray]]]] = {
    map
  }

  def setBCMap(map:scala.collection.mutable.Map[String,Broadcast[Map[String, Map[String, JSONArray]]]]):Unit = {
    this.map = map
  }

  def getRefreshTime():Long = {
    refreshTime
  }

  def setRefreshTime(refreshTime:Long):Unit = {
    this.refreshTime = refreshTime
  }
}
