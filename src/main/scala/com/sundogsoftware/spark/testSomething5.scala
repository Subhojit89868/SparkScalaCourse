package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import scala.util.control.Breaks._

object testSomething5 {

  case class Marvel(id:Int, name:String)
  case class Name(value:String)

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("testSomething5")
      .getOrCreate()

    val hero = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val heroName = spark.read
      .schema(hero)
      .option("sep"," ")
      .csv("C:\\Users\\subho\\GIT\\SparkScalaCourse\\data\\Marvel-names.txt")
      .as[Marvel]
    val graph = spark.read
      .text("C:\\Users\\subho\\GIT\\SparkScalaCourse\\data\\Marvel-graph.txt")
      .as[Name]

    val connections = graph.withColumn("id",split(col("value")," ")(0))
      .withColumn("conCount",size(split(col("value")," ")) -1)
      .groupBy("id").agg(sum("conCount").alias("fin_count"))

    val fin = connections.alias("df1")
      .join(heroName.alias("df2"),$"df1.id" === $"df2.id")
      .orderBy("fin_count")
      .select("name","fin_count")

    println("Name,connections")

    var prev = fin.first()(1)

    breakable {
      for (i <- fin.collect()) {
        if (prev != i(1))
          break
        println(i(0) + "," + i(1))
        prev = i(1)
      }
    }
    spark.stop()
  }
}