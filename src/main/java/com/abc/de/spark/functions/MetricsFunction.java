package com.abc.de.spark.functions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;
import com.abc.de.utils.ConnectionUtil;
import com.abc.de.utils.DataFormatter;
import com.abc.de.utils.MetricsParser;
import com.abc.de.vo.MetricsData;
import com.esotericsoftware.kryo.Kryo;

/**
 * Metrics function processes JavaRDD of String. The input string is a JSON
 * object, which is parsed, processed and inserted into Phoenix table.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class MetricsFunction implements VoidFunction<JavaRDD<String>> {

	private static final long serialVersionUID = 181698026160149711L;

	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsFunction.class);

	private Configuration config;
	private Map<String, Integer> metricsMeta;
	private Map<String, String> hostToAppMap;
	private Map<String, String> hostToEnvMap;

	/**
	 * Constructor initializing the instance variables.
	 * 
	 * @param config
	 *            configuration object
	 * @param metricsMeta
	 *            metric name to metric id mapping
	 * @param hostToAppMap
	 *            host to application mapping
	 * @param hostToEnvMap
	 *            host to environment mapping
	 */
	public MetricsFunction(Configuration config, Map<String, Integer> metricsMeta, Map<String, String> hostToAppMap,
			Map<String, String> hostToEnvMap) {
		this.config = config;
		this.metricsMeta = metricsMeta;
		this.hostToAppMap = hostToAppMap;
		this.hostToEnvMap = hostToEnvMap;
	}

	/**
	 * No-arg constructor, otherwise an exception would be thrown:
	 * java.io.InvalidClassException: no valid constructor
	 */
	public MetricsFunction() {
		/*
		 * Added to suppress InvalidClassException
		 */
	}

	@Override
	public void call(JavaRDD<String> rdd) throws Exception {
		rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
			static final long serialVersionUID = -2799895985926065723L;

			@Override
			public void call(Iterator<String> jsonList) throws Exception {
				try {
					/*
					 * foreachPartition is an action invoked partition-wise. If
					 * the object creation is costly, create the object before
					 * the iteration. Example: Connection objects can be created
					 * once for the partition, process the records and then
					 * close the connection at the end. Kryo is used to
					 * serialize the data before storing it in Phoenix table.
					 */
					JSONParser parser = new JSONParser();
					Connection conn = ConnectionUtil.getPhoenixConnection(config);
					String metricsTable = config.getStringValue(Constants.CONFIG_TABLE_NAME_METRICS, null);
					String phoenixQuery = " UPSERT INTO " + Constants.METRICS_TABLE_NAME + " VALUES(?,?,?,?,?,?,?) ";
					PreparedStatement ps = conn
							.prepareStatement(phoenixQuery.replaceAll(Constants.METRICS_TABLE_NAME, metricsTable));
					MetricsParser metricsParser = new MetricsParser();
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
					Kryo kryo = new Kryo();
					kryo.register(com.abc.de.vo.BaseMetrics.class);

					while (jsonList.hasNext()) {
						String jsonString = jsonList.next();
						JSONArray jsonArray = (JSONArray) parser.parse(jsonString);
						for (int j = 0; j < jsonArray.size(); j++) {
							JSONObject json = (JSONObject) jsonArray.get(j);
							try {
								if (json != null) {
									LOGGER.info("JSON:" + json);
									metricsParser.reset();
									metricsParser.parse(json.toJSONString());
									/*
									 * Process all metrics except datasource
									 * metrics.
									 */
									processMetrics(json, metricsParser, sdf, metricsMeta, hostToAppMap, hostToEnvMap,
											kryo, ps);
									/*
									 * Process datasource metrics, which is a
									 * JSONArray.
									 */
									processDatasourceMetrics(json, metricsParser, sdf, metricsMeta, kryo, ps);
								}

							} catch (Exception ex) {
								LOGGER.error("Error processing the JSON:" + json, ex);
							}
						}
					}
					ps.clearBatch();
					ps.close();
					conn.commit();
					LOGGER.info("Successful commit");
					conn.close();
				} catch (Exception e) {
					LOGGER.error("Error processing data in the partition", e);
				}
			}
		});
	}

	private void processMetrics(JSONObject json, MetricsParser metricsParser, SimpleDateFormat sdf,
			Map<String, Integer> metricsMeta, Map<String, String> hostToAppMap, Map<String, String> hostToEnvMap,
			Kryo kryo, PreparedStatement ps) throws Exception {
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

						LOGGER.info("***Final data****");
						LOGGER.info("metricId:" + metricId + "===reverseTime:" + reverseTime + "===env:" + env
								+ "===appname:" + appname + "===host:" + host + "===instance:" + instance + "===data:"
								+ data);

						updateDatastore(ps, metricId, reverseTime, env, appname, host, instance,
								DataFormatter.serializeWithKryo(data.toBaseMetrics(), kryo));
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void processDatasourceMetrics(JSONObject json, MetricsParser metricsParser, SimpleDateFormat sdf,
			Map<String, Integer> metricsMeta, Kryo kryo, PreparedStatement ps) throws Exception {
		Map<String, Map<String, List<JSONObject>>> dsMetricsCollection = metricsParser.getDSMetricsCollection();
		Set<String> dsBuckets = dsMetricsCollection.keySet();
		Integer metricId = metricsMeta.get("dsmetrics");
		String env = (String) json.get(Constants.CONFIG_ENVIRONMENT);
		String appname = (String) json.get(Constants.CONFIG_APPNAME);
		for (String rowkey : dsBuckets) {
			Map<String, List<JSONObject>> collection = dsMetricsCollection.get(rowkey);
			Set<String> metricNames = collection.keySet();
			if (collection != null && collection.size() > 0 && rowkey != null) {
				String[] rowkeySplit = rowkey.split("\\|");
				if (rowkeySplit.length < 3)
					break;
				int idx = rowkey.indexOf('|');
				String timeBucket = null;
				if (idx > 0)
					timeBucket = rowkey.substring(0, idx);

				Date date = sdf.parse(timeBucket);
				long reverseTime = Long.MAX_VALUE - date.getTime();

				String host = rowkeySplit[1];
				String instance = rowkeySplit[2];

				JSONArray jsonArray = new JSONArray();
				for (String metricName : metricNames) {
					List<JSONObject> metrics = collection.get(metricName);
					for (JSONObject metric : metrics)
						jsonArray.add(metric);
				}

				updateDatastore(ps, metricId, reverseTime, env, appname, host, instance,
						DataFormatter.serializeWithKryo(jsonArray.toJSONString(), kryo));
			}
		}
	}

	private void updateDatastore(PreparedStatement ps, int metricId, long reverseTime, String env, String appname,
			String host, String instance, byte[] metric) {

		try {
			int index = 1;
			if (env == null)
				env = "MCOM";
			ps.setInt(index++, metricId);
			ps.setLong(index++, reverseTime);
			ps.setString(index++, DataFormatter.safeTrim(env));
			ps.setString(index++, DataFormatter.safeTrim(appname));
			ps.setString(index++, DataFormatter.safeTrim(host));
			ps.setString(index++, DataFormatter.safeTrim(instance));
			ps.setBytes(index, metric);

			long s = System.currentTimeMillis();
			ps.execute();
			long e = System.currentTimeMillis();
			LOGGER.info("Elapsed time in phoenix execute of metrics:" + (e - s) + " ");
		} catch (Exception e) {
			LOGGER.info("Error updating datastore", e);
		}
	}

}
