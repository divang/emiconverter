package com.training
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object EMIConverter {

	def main(args: Array[String]) {
		startKafkaStream()
	}

	def startKafkaStream() {

		val spark = SparkSession
				.builder().master("local")
				.appName("Spark Hive Example")
				//chmod +777 /tmp/hive
				.config("spark.sql.warehouse.dir", "/tmp/hive/warehouse")
				.enableHiveSupport()
				.getOrCreate()

				// Create context with 2 second batch interval
				val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
				ssc.sparkContext.setLogLevel("ERROR")

				import spark.implicits._
				import spark.sql

				sql("DROP table emi_convertor")
				sql("CREATE TABLE emi_convertor (accountId string, accountHolderName string, amount float, transactionTime string)")

				val Array(brokers, groupId, topics) = Array("localhost:9092","group1","transcations")

				// Create direct kafka stream with brokers and topics
				val topicsSet = topics.split(",").toSet
				val kafkaParams = Map[String, Object](
						ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
						ConsumerConfig.GROUP_ID_CONFIG -> groupId,
						ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
						ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
				val messages = KafkaUtils.createDirectStream[String, String](
						ssc,
						LocationStrategies.PreferConsistent,
						ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

				messages.foreachRDD { rdd =>
				val df = spark.read.json(rdd.map(x => x.value()))
				if(!df.head(1).isEmpty) {
					val filterDF = df.filter($"amount" > 10000)
							if(!filterDF.isEmpty) {
								filterDF.write.insertInto("emi_convertor")
								println("Eligible EMI convertor Customer total List:")
								sql("select * from emi_convertor").show()
							}
				}
		}

		// Start the computation
		ssc.start()
		ssc.awaitTermination()
	}
}