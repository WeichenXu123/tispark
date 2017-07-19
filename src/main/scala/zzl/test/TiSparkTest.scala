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
    //runUserTable(ti,spark)
    val sql=args.mkString(" ")
    runStudentTable(ti,spark,sql)

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
  def runStudentTable(ti:TiContext,spark:SparkSession,sql:String): Unit ={
    val df = ti.tidbTable( "test", "student")
    df.createGlobalTempView("t1")
    //df.createOrReplaceTempView("t1")
    println("sql:"+sql)
    val sql1="select class, avg(score) from global_temp.t1 WHERE school = 's1'  and (studentId >= 4 and studentId < 10) or studentId in (1, 2) group by class"
    spark.sql(sql1).show
  }
}
