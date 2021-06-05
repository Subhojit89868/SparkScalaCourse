package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object testSomething {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","testSomething")

    def parseLine(data : String) : (Int, Float) = {

      val fields = data.split(",")

      val customer = fields(0).toInt
      val price = fields(2).toFloat

      (customer, price)

    }

    val data = sc.textFile("data/customer-orders.csv")

    val tupleData = data.map(parseLine)

    val customerSum = tupleData.reduceByKey( (x,y) => x + y).map(x => (x._2, x._1)).sortByKey()

    val cust_d = customerSum.collect()

    for (r <- cust_d){
      println(r._2 + " : " + r._1)
    }
  }

}