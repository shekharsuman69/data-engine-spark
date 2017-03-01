package com.abc.de.spark.handler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;
import com.abc.de.spark.functions.MetricsFunction;
import com.abc.de.spark.functions.TupleFunction;
import com.abc.de.spark.utils.EnvironmentMetadata;
import com.abc.de.spark.utils.MetricsMetadata;

import kafka.serializer.StringDecoder;

/**
 * This class serves as the handler for metrics data. It extends the BaseHandler
 * class and reads data of Metrics Kafka topic at regular batch interval.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class MetricsHandlerV1 extends BaseHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsHandlerV1.class);

	/**
	 * user defined constructor
	 * 
	 * @param config
	 *            configuration object
	 */
	public MetricsHandlerV1(Configuration config) {
		super(config);
	}

	/**
	 * Entry point for the streaming application
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String confPath = System.getProperty(Constants.DATA_ENGINE_PROPERTIES);
		final Configuration config = new Configuration(confPath);
		MetricsHandlerV1 handler = new MetricsHandlerV1(config);
		handler.execute();
	}

	@Override
	protected void execute() {
		/*
		 * Setup the SparkConf object and Java streaming context with specified
		 * duration
		 */
		SparkConf conf = new SparkConf().setAppName(appName);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(duration));

		/*
		 * Print all the application related configurations that are passed as
		 * arguments or property file
		 */
		printApplicationInfo();

		/*
		 * Read the 'METRICMETA' phoenix table and broadcast the mapping to all
		 * executors. The meta data is a mapping of metric name to metric Id.
		 */
		final Broadcast<Map<String, Integer>> metricsMeta = sc.broadcast(MetricsMetadata.load(config));

		/*
		 * Load the environment hierarchy from 'APMLookup' HBase table and
		 * create 2 mappings:(i) Mapping of host to application (ii) Mapping of
		 * host to environment. The mapping is used for reverse lookup in case
		 * the metrics data received does not have 'environment' or
		 * 'application' populated.
		 */
		EnvironmentMetadata.load(config);

		/*
		 * Broadcast the 2 mappings created in the above step to all executors.
		 * This would avoid unnecessary multiple calls to HBase.
		 * 
		 */
		final Broadcast<Map<String, String>> hostToAppMap = sc.broadcast(EnvironmentMetadata.getHostAppMapping());
		final Broadcast<Map<String, String>> hostToEnvMap = sc.broadcast(EnvironmentMetadata.getHostEnvMapping());

		LOGGER.info("Host and App Mapping:" + hostToAppMap.getValue());

		LOGGER.info("Host and Env Mapping:" + hostToEnvMap.getValue());

		/*
		 * Setup Kafka configuration required to create direct stream against
		 * Kafka topics.
		 * 
		 */
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put(Constants.BROKER_LIST, brokers);
		kafkaParams.put(Constants.GROUP_ID, consumerGroup);
		Set<String> topics = Collections.singleton(topic);

		/*
		 * Raw stream of data from Kafka topic in the form of Key:Value tuples
		 */
		JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		/*
		 * The raw data is in Base64 compressed format. Extract just the value
		 * and uncompress it.
		 */
		JavaDStream<String> metricStream = kafkaStream.map(new TupleFunction());

		/*
		 * Iterate through each RDD in the DStream of uncompressed data
		 * partition-wise and apply the processing logic of - parsing,
		 * formatting and writing into Phoenix tables. Creating connection
		 * object partition-wise is an efficient way to dealing with
		 * connections.
		 */
		metricStream.foreachRDD(new MetricsFunction(this.config, metricsMeta.getValue(), hostToAppMap.getValue(),
				hostToEnvMap.getValue()));

		metricStream.print();

		ssc.start();
		ssc.awaitTermination();
	}
}
