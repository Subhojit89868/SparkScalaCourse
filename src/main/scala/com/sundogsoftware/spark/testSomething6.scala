package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object testSomething6 {

  case class Products(product_id: Int, product_name: String, price: Int)
  case class Sales(order_id: Int, product_id: Int, date: String, num_pieces_sold: Int)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("testSomething6")
      .getOrCreate()

    import spark.implicits._
    val products = spark.read
      .option("sep"," ")
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/products.csv")
      .as[Products]

    val sales = spark.read
      .option("sep"," ")
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/sales.csv")
      .as[Sales]

    val total_products = sales.select("product_id","num_pieces_sold")
      .groupBy("product_id")
      .agg(sum("num_pieces_sold").as("product_count"))

    val revenue = products.as("df1")
      .join(total_products.as("df2"), $"df1.product_id" === $"df2.product_id","right")
      .select($"df2.product_id",
        ($"df2.product_count" * $"df1.price").as("total_product_price"),
        $"df2.product_count")
      .orderBy(desc("total_product_price"))
      .cache()

    revenue.show()

    revenue.agg(sum("total_product_price").as("total_revenue"),
      sum("product_count").as("total_count"))
      .select(($"total_revenue"/$"total_count").as("average_revenue"))
      .show()

    println(revenue.first())
  }
}