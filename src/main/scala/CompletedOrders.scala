package net.atos.spark

import org.apache.spark.SparkContext._

import org.apache.spark.SparkContext
import org.apache.log4j._

object CompletedOrders {

	def orderParser(line:String) ={
		val orderItems = line.split(',')
		(orderItems(1),orderItems(2),orderItems(3))
	}

	def main(args:Array[String]):Unit = {

		Logger.getLogger("org").setLevel(Level.ERROR)
		// Create input and outpu paths
		val input = args(0)
		val output = args(1)
		// Create a sparkcontext

		val sc = new SparkContext()

		val orders = sc.textFile(input)

		val orderDateStatus = orders.map(orderParser)

		val ordersFiltered = orderDateStatus.
		filter(order=> (order._3 == "COMPLETE") || (order._3 == "PROCESSING"))

		ordersFiltered.map(order => order._1+", "+order._2+", "+order._3).coalesce(1).saveAsTextFile(output)
	}
}


