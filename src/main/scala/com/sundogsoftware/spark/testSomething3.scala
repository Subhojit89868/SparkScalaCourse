package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object testSomething3 {

  case class Line(id:Int, x1:Int, y1:Int, x2:Int, y2:Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("testSomething3")
      .getOrCreate()

    import spark.implicits._
    val getLines = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/segments.csv")
      .as[Line]

    getLines.createOrReplaceTempView("lines")

    val getSlope = spark.sql("SELECT id,CAST((y2-y1)/(x2-x1) AS DECIMAL(10,2)) AS slope FROM lines ORDER BY id")

    //getSlope.createOrReplaceTempView("slopes")

    //val slopeCount = spark.sql("SELECT slope,count(*) AS slp_cnt FROM slopes GROUP BY slope")
    val slopeCount = getSlope.groupBy("slope").count().select("slope","count")

    val lineCount = getSlope.join(slopeCount, getSlope("slope") <=> slopeCount("slope")).orderBy(desc("count"),asc("id")).withColumn("fin_cnt",$"count"-1).select("id","fin_cnt")

    println("id,count")
    val fin = lineCount.collect()
    for ( i <- fin) {
      println(i(0)+","+i(1))
    }

  }
}