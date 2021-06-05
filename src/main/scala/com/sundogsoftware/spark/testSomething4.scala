package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object testSomething4 {

  case class Pet(id:Int, owner_id:Int, breed:String, sex:String)
  case class Owner(id:Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("testSomething3")
      .getOrCreate()

    import spark.implicits._
    val getId = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/Pet.csv")
      .as[Pet]
    val getcustId = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/Owner.csv")
      .as[Owner]

    val petCount = getId.as("df1")
      .join(getId.as("df2"), !($"df1.id" <=> $"df2.id")
        && !($"df1.owner_id" <=> $"df2.owner_id")
        && $"df1.breed" <=> $"df2.breed"
        && !($"df1.sex" <=> $"df2.sex"))
      .select($"df1.owner_id")
      .groupBy("owner_id").count()

    val ownerCount = getcustId
      .join(petCount, petCount("owner_id") <=> getcustId("id"),"left")
      .select("id","count")
      .orderBy(desc("count"),asc("id"))

    ownerCount.createOrReplaceTempView("owner_count")

    val final_count = spark.sql("SELECT id, COALESCE(count,0) AS count FROM owner_count")

    println("owner_id,count")
    val fin = final_count.collect()
    for ( i <- fin) {
      println(i(0)+","+i(1))
    }

  }
}