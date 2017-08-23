package com.bluedon.utils

import java.sql.Connection

/**
  * Created by huoguang on 2017/3/30.
  */
trait HbaseHelper {

  def usingHbase(ip: String)(f:(Connection) => Unit) = {
    val conn: Connection = new DBUtils().getPhoniexConnect(ip)
    try {
      f(conn)
    } catch {
      case e:Throwable => println(s" Some error occur during" + e.getMessage)
    } finally {
      conn.close()
    }
  }

}
