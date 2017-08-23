package com.bluedon.utils

import java.sql.{PreparedStatement, Timestamp}

/**
  * Created by huoguang on 2017/3/30.
  * 预编译，批量插入，每个表写一个方法
  */
trait batchInsertHbase extends HbaseHelper{

  //批量插入T_SIEM_KMEANS_CENTER表
  def insertSiemKmeanCenter(batchSize :Int, data: List[List[String]]) = {
    usingHbase("hadoop:2181"){
      conn => {
        conn.setAutoCommit(false)
        val templet = s"""upsert into T_SIEM_KMEANS_CENTER (\"ROW\", \"NAME\",\"CENTER_INDEX\", \"CENTER_INFO\", \"CLNO\", \"TYPE\", \"CLTIME\", \"DEMO\", \"CENTER_INDEX_P\",\"CLNO_P\") values (?,?,?,?,?,?,?,?,?,?)"""
        val Pstmt: PreparedStatement = conn.prepareStatement(templet)
        val state = conn.createStatement()
        var flag = 0
        try{
          data.foreach(x =>{
            Pstmt.setString(1, x.head)
            Pstmt.setString(2, x(1))
            Pstmt.setString(3, x(2))
            Pstmt.setString(4, x(3))
            Pstmt.setString(5, x(4))
            Pstmt.setInt(6, x(5).toInt)
            Pstmt.setTimestamp(7, Timestamp.valueOf(x(6)))
            Pstmt.setString(8, x(7))
            Pstmt.setString(9, x(8))
            Pstmt.setString(10, x(9))
            Pstmt.execute()
            flag = flag+1
            if(flag%batchSize == 0){
              println(s"!!!")
              conn.commit()
            }
          })
        } catch {
          case e: Exception => {
            println(s"Insert into Hbase occur error!! " + e.getMessage)
            conn.rollback()
          }
        } finally {
          conn.commit()
          state.close()
          conn.close()
        }
      }
    }
  }

}
