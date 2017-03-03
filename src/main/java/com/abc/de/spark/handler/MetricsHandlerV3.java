package com.abc.de.spark.handler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;
import com.abc.de.spark.functions.MetricsRowFunctionV2;
import com.abc.de.spark.functions.MetricsWriterFunctionV2;
import com.abc.de.spark.functions.TupleFunctionV2;
import com.abc.de.spark.utils.EnvironmentMetadata;
import com.abc.de.spark.utils.MetricsMetadata;

import kafka.serializer.StringDecoder;

/**
 * This class serves as the handler for metrics data. It extends the BaseHandler
 * class and reads data of Metrics Kafka topic at regular batch interval.Version
 * 2.0 - (i) Uses mapPartition() function to format the data as it is efficient
 * than map(). (ii) Creates DStream of JSON string objects to provide schema
 * around the data. (iii) Uses spark-phoenix connector to write the data into
 * database.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class MetricsHandlerV3 extends BaseHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsHandlerV3.class);

	/**
	 * user defined constructor
	 * 
	 * @param config
	 *            configuration object
	 */
	public MetricsHandlerV3(Configuration config) {
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
		MetricsHandlerV3 handler = new MetricsHandlerV3(config);
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
		final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

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
		 * and uncompress it. Use mapPartition() instead of map() - map()
		 * exercises the function being utilized at a per element level while
		 * mapPartitions() exercises the function at the partition level. The
		 * entire data set is passed to the mapPartition() function as an
		 * Iterator. An Iterable is returned as an output. mapPartition() is
		 * preferred over map() in case expensive operation is done in the
		 * function. Example: Parser object is created, better way is to create
		 * parser object for all elements at once, rather than element-wise.
		 */
		JavaDStream<String> metricStream = kafkaStream.mapPartitions(new TupleFunctionV2());

		/*
		 * Iterate through each partition, parse the JSON and create JSON
		 * string. JSON string represents one row of output and is tied to a
		 * schema. The final output is a DStream of Row.
		 */
		JavaDStream<String> rowStream = metricStream.mapPartitions(
				new MetricsRowFunctionV2(metricsMeta.getValue(), hostToAppMap.getValue(), hostToEnvMap.getValue()));

		metricStream.print(1);
		rowStream.print(5);

		/*
		 * Iterate through each RDD in the DStream of JSON string and write it
		 * to Phoenix table using spark-phoenix connector.
		 * 
		 */
		rowStream.foreachRDD(new MetricsWriterFunctionV2(this.config, sqlContext));

		ssc.start();
		ssc.awaitTermination();
	}
}
