package com.ddos.spark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class DetectDOSAttack {

	public static void main(String[] args) {

		if (args.length < 3) {

			System.out.println(
					"<Usage>: DetectDOSAttack <ZK quorum> <consumer group id> <topic_name> ");
			return;
		}

		String zkQuorum = args[0];
		String groupId = args[1];
		String topics = args[2];

		Map<String, Integer> topicsMap = new HashMap<String, Integer>();
		topicsMap.put(topics, 2);

		SparkConf sparkConf = new SparkConf().setAppName("DDOS").setMaster("local[2]")
				.set("spark.executor.memory", "1g").set("spark.network.timeout", "600s")
				.set("spark.executor.heartbeatInterval", "30s")
				.set("spark.yarn.appMasterEnv.JAVA_HOME", "/home/cloudera/Downloads/jdk1.8.0_201");

		// create a Spark Java streaming context, with stream batch interval
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(1000L));

		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc, zkQuorum, groupId,
				topicsMap);

		JavaDStream<String> lines = kafkaStream.map(arg0 -> arg0._2());
		JavaDStream<String> ipAddress = lines.flatMap(new FlatMapFunction<String, String>() {

			Map<String, String> recordMap = new HashMap<>();
			Set<String> ddosIP = new HashSet<>();

			@Override
			public Iterable<String> call(String msg) throws Exception {
				// System.out.println("Message in words call... : " + msg);
				String[] msgList = msg.split(" ");
				String keyIP = msgList[0].trim();
				String valTS = msgList[3].trim().substring(1);

				// Main DDOS detect logic
				if (recordMap.containsKey(keyIP)) {
					// Simple Logic: two simultaneous hits from same source are
					// dos attack
					if (recordMap.get(keyIP).equals(valTS)) {
						ddosIP.add(keyIP);

					} else {
						recordMap.put(keyIP, valTS);
					}
				} else {
					recordMap.put(keyIP, valTS);
				}

				return (ddosIP);
			}
		});

		Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0;
			}
		};

		JavaPairDStream<String, Integer> ddosIPAddress = ipAddress.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String pfString) throws Exception {
				System.out.println("pfString: " + pfString);
				return new Tuple2<String, Integer>(pfString, 1);
			}
		}).reduceByKey(reduceFunc);

		ddosIPAddress.print();
		
		//ddosIPAddress.saveAsHadoopFiles("result", "txt", Text.class, IntWritable.class, TextOutputFormat.class);
		

		jssc.start();
		jssc.awaitTermination();
	}

}