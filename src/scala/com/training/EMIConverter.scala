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

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    consumer-group topic1,topic2
 */
object EMIConverter {

	def createEMIConvertTable() {

		val spark = SparkSession
				.builder().master("local")
				.appName("Spark Hive Example")
			  .config("spark.sql.warehouse.dir", "/hive/warehouse")
				.enableHiveSupport()
				.getOrCreate()

				import spark.implicits._
				import spark.sql
				sql("CREATE TABLE IF NOT EXISTS emi_convertor (accountId string,amount double,accountHolderName string) USING hive")
				sql("SELECT * FROM emi_convertor").show()
	}


	def main(args: Array[String]) {
		createEMIConvertTable()
	}

	def startKafkaStream() {
		val sqlSparkSession = SparkSession
				.builder()
				.appName("EMI Convertor SQL integration")
				.getOrCreate()

				val Array(brokers, groupId, topics) = Array("localhost:9092","group1","transcations")

				// Create context with 2 second batch interval
				val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local")
				val ssc = new StreamingContext(sparkConf, Seconds(2))

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

				// Get the lines, split them into words, count the words and print
				val lines = messages.map(_.value)

				// For implicit conversions like converting RDDs to DataFrames
				import sqlSparkSession.implicits._

				val words = lines.flatMap(_.split(" "))
				val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
				wordCounts.print()

				// Start the computation
				ssc.start()
				ssc.awaitTermination()
	}
}
// scalastyle:on println
