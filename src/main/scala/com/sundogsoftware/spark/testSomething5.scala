package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object testSomething5 {

  case class Marvel(id:Int, name:String)
  case class Name(value:String)

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("testSomething5")
      .getOrCreate()

    val hero = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val heroName = spark.read
      .schema(hero)
      .option("sep"," ")
      .csv("data/Marvel-names.txt")
      .as[Marvel]
    val graph = spark.read
      .text("data/Marvel-graph.txt")
      .as[Name]


  }
}