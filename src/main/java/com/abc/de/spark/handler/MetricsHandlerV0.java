package com.abc.de.spark.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;
import com.abc.de.spark.utils.EnvironmentMetadata;
import com.abc.de.spark.utils.MetricsMetadata;
import com.abc.de.utils.DataFormatter;
import com.abc.de.utils.MetricsParser;
import com.abc.de.vo.MetricsData;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * This class serves as the handler of metrics data. Draft version - all logic
 * embedded in this class.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class MetricsHandlerV0 {

	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsHandlerV0.class);

	private MetricsHandlerV0() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		/*
		 * Load the configuration and initialize the application related
		 * properties viz. application name, Kafka broker list, Kafka topic,
		 * streaming duration etc.
		 */
		String confPath = System.getProperty(Constants.DATA_ENGINE_PROPERTIES);
		final Configuration config = new Configuration(confPath);
		String appName = config.getStringValue(Constants.APPNAME, "Metrics Handler");
		long duration = config.getLongValue(Constants.DURATION, 500);
		String brokers = config.getStringValue(Constants.KAFKA_BROKER, "localhost:9092");
		String topic = config.getStringValue(Constants.KAFKA_TOPIC, "Metrics");
		String groupId = config.getStringValue(Constants.KAFKA_GROUP_ID, "Metrics-Handler");

		LOGGER.info("**********************Application Configuration***********************");

		LOGGER.info("Application Name:                {}", appName);
		LOGGER.info("Kafka Topic Poll Duration:       {}", duration);
		LOGGER.info("Kafka Broker List:               {}", brokers);
		LOGGER.info("Kafka Topic:                     {}", topic);
		LOGGER.info("Kafka Consumer Group:            {}", groupId);

		LOGGER.info("**********************************************************************");

		/*
		 * Setup the SparkConf object and Java streaming context with specified
		 * duration
		 */
		SparkConf conf = new SparkConf().setAppName(appName);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(duration));

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

		System.out.println("Host and App Mapping:" + hostToAppMap.getValue());

		System.out.println("Host and Env Mapping:" + hostToEnvMap.getValue());

		/*
		 * Setup Kafka configuration required to create direct stream against
		 * Kafka topics.
		 * 
		 */
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put(Constants.BROKER_LIST, brokers);
		kafkaParams.put(Constants.GROUP_ID, groupId);
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
		JavaDStream<String> metricStream = kafkaStream.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = -2799895985926065723L;

			@Override
			public String call(Tuple2<String, String> tuple) throws Exception {
				String value = null;
				try {
					value = DataFormatter.uncompress(tuple._2());
					System.out.println("Value:" + value);
				} catch (Exception e) {
					LOGGER.error("Error processing message", e);
				}
				return value;
			}
		});

		/*
		 * Iterate through each RDD in the DStream of uncompressed data
		 * partition-wise and apply the processing logic of - parsing,
		 * formatting and writing into Phoenix tables. Creating connection
		 * object partition-wise is an efficient way to dealing with
		 * connections.
		 */
		metricStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			static final long serialVersionUID = -2799895985926065723L;

			@Override
			public void call(JavaRDD<String> metricRdd) throws Exception {

				metricRdd.foreachPartition(new VoidFunction<Iterator<String>>() {
					static final long serialVersionUID = -2799895985926065723L;

					@Override
					public void call(Iterator<String> jsonList) throws Exception {
						JSONParser parser = new JSONParser();
						Connection conn = getPhoenixConnection(config);
						MetricsParser metricsParser = new MetricsParser();
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

						while (jsonList.hasNext()) {
							System.out.println("Parser Object:" + parser);
							String jsonString = jsonList.next();
							JSONArray jsonArray = (JSONArray) parser.parse(jsonString);
							for (int j = 0; j < jsonArray.size(); j++) {
								JSONObject json = (JSONObject) jsonArray.get(j);
								try {
									if (json != null)
										System.out.println("JSON:" + json);
									processEvent(json, metricsParser, sdf, metricsMeta.getValue(),
											hostToAppMap.getValue(), hostToEnvMap.getValue());
								} catch (Exception ex) {
									LOGGER.error("Error in processEvent():" + json, ex);
								}
							}
						}

					}
				});
			}
		});

		metricStream.print();

		kafkaStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {
				System.out.println("Debug String:" + rdd.toDebugString());
				System.out.println("Number of partitions:" + rdd.getNumPartitions());
				System.out.println("Metrics in executor:" + metricsMeta.getValue());
			}
		});

		ssc.start();
		ssc.awaitTermination();
	}

	private static void processEvent(JSONObject json, MetricsParser metricsParser, SimpleDateFormat sdf,
			Map<String, Integer> metricsMeta, Map<String, String> hostToAppMap, Map<String, String> hostToEnvMap)
			throws Exception {
		metricsParser.reset();
		metricsParser.parse(json.toJSONString());

		Map<String, Map<String, MetricsData>> metricsCollection = metricsParser.getMetricsCollection();

		String env = (String) json.get(Constants.CONFIG_ENVIRONMENT);
		String appname = (String) json.get(Constants.CONFIG_APPNAME);
		Set<String> buckets = metricsCollection.keySet();
		for (String rowkey : buckets) {
			Map<String, MetricsData> collection = metricsCollection.get(rowkey);

			if (collection != null && !collection.isEmpty() && rowkey != null) {
				int idx = rowkey.indexOf('|');
				String timeBucket = null;
				if (idx > 0)
					timeBucket = rowkey.substring(0, idx);

				Date date = sdf.parse(timeBucket);
				long reverseTime = Long.MAX_VALUE - date.getTime();

				Set<String> metricsNameSet = collection.keySet();
				for (String metricName : metricsNameSet) {
					Integer metricId = metricsMeta.get(metricName);
					if (metricId != null) {
						MetricsData data = collection.get(metricName);
						String host = data.getHost();
						String instance = data.getInstance();

						if (env == null) {
							env = hostToEnvMap.get(host);
						}
						if (appname == null) {
							appname = hostToAppMap.get(host);
						}

						System.out.println("***Final data****");
						System.out.println("metricId:" + metricId + "===reverseTime:" + reverseTime + "===env:" + env
								+ "===appname:" + appname + "===host:" + host + "===instance:" + instance + "===data:"
								+ data);
						/*
						 * updateDatastore(metricId, reverseTime, env, appname,
						 * host, instance,
						 * DataFormatter.serializeWithKryo(data.toBaseMetrics(),
						 * kryo));
						 */
					}
				}
				// postProcessing();
			}
		}
		// processDatasourceMetrics(env, appname);
	}

	private static Connection getPhoenixConnection(Configuration config) throws Exception {
		String phoenixURL = null;
		try {
			phoenixURL = config.getStringValue(Constants.PHOENIX_JDBC_URL, null);
			Class.forName(Constants.PHOENIX_DRIVER);
		} catch (Exception e) {
			LOGGER.error("Error getting Phoenix Connection", e);
		}
		return DriverManager.getConnection(phoenixURL);
	}

}
