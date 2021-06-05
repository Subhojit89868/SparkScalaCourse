package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object testSomething2 {

  case class Friend(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("testSomething2")
      .getOrCreate()

    import spark.implicits._
    val schemaFriend = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      .as[Friend]

    schemaFriend.printSchema()

    //schemaFriend.createOrReplaceTempView("Friend")

    //spark.sql("SELECT age,CAST(AVG(friends) AS DECIMAL(27,2)) AS friend_avg FROM Friend GROUP BY age").sort("age").show()

    schemaFriend.select("age","friends")
      .groupBy("age")
      .agg(round(avg("friends"),2).alias("avg_friend"))
      .sort("age").show()

    spark.stop()

  }
}