package zzl.test

import org.apache.spark.sql.SparkSession

/**
 * Created by Administrator on 2017/7/5.
 */
import com.pingcap.tispark._
import org.apache.spark.sql.TiContext
import org.apache.spark.SparkConf
object TiSparkTest {
  def main (args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()
    val ti = new TiContext(spark,List("192.168.14.71:2379","192.168.14.77:2379","192.168.14.82:2379"))//
    //runT1Table(ti,spark)
    runUserTable(ti,spark)

  }
  def runT1Table(ti:TiContext,spark:SparkSession): Unit ={
    val df = ti.tidbTable( "test", "t1")
    df.createGlobalTempView("t1")
    spark.sql("select avg(c1) from global_temp.t1").show
  }
  def runUserTable(ti:TiContext,spark:SparkSession): Unit ={
    val df = ti.tidbTable( "test", "users")
    df.createGlobalTempView("t1")
    spark.sql("select avg(uid) from global_temp.t1").show
  }
}
