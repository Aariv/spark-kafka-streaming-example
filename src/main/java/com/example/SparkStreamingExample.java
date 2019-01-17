/**
 * 
 */
package com.example;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import kafka.serializer.StringDecoder;
/**
 * @author zentere
 *
 */
public class SparkStreamingExample {

	public static JavaSparkContext sc;

	public static void main(String[] args) {
		String brokers = "localhost:9092,localhost:9093";

		String topics = "votes";

		SparkConf sparkConf = new SparkConf();

		sparkConf.setMaster("local[2]");

		sparkConf.setAppName("SparkStreamingExample");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
	}
}
