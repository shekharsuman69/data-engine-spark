package com.abc.de.spark.functions;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Constants;
import com.abc.de.utils.MetricsParser;
import com.abc.de.vo.MetricsData;
import com.esotericsoftware.kryo.Kryo;

/**
 * MetricsRow function parses the JSON string and creates a JSON string which
 * represents row in a Phoenix table(mapPartition implementation)
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class MetricsRowFunctionV2 implements FlatMapFunction<Iterator<String>, String> {

	private static final long serialVersionUID = -7295965590916588108L;
	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsRowFunctionV2.class);

	private Map<String, Integer> metricsMeta;
	private Map<String, String> hostToAppMap;
	private Map<String, String> hostToEnvMap;

	/**
	 * Constructor initializing the instance variables.
	 * 
	 * @param metricsMeta
	 *            metric name to metric id mapping
	 * @param hostToAppMap
	 *            host to application mapping
	 * @param hostToEnvMap
	 *            host to environment mapping
	 */
	public MetricsRowFunctionV2(Map<String, Integer> metricsMeta, Map<String, String> hostToAppMap,
			Map<String, String> hostToEnvMap) {
		this.metricsMeta = metricsMeta;
		this.hostToAppMap = hostToAppMap;
		this.hostToEnvMap = hostToEnvMap;
	}

	/**
	 * No-arg constructor, otherwise an exception would be thrown:
	 * java.io.InvalidClassException: no valid constructor
	 */
	public MetricsRowFunctionV2() {
		/*
		 * Added to suppress InvalidClassException
		 */
	}

	@Override
	public Iterable<String> call(Iterator<String> jsonList) throws Exception {
		List<String> rows = new ArrayList<>();
		try {
			JSONParser parser = new JSONParser();
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
							 * Parse all metrics except datasource metrics into
							 * a Row object
							 */
							List<String> mRow = parseMetrics(json, metricsParser, sdf, kryo);
							/*
							 * Parse datasource metrics, which is a JSONArray
							 * into a Row object
							 */
							List<String> dRow = parseDatasourceMetrics(json, metricsParser, sdf, kryo);

							rows.addAll(mRow);
							rows.addAll(dRow);
						}

					} catch (Exception ex) {
						LOGGER.error("Error processing the JSON:" + json, ex);
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error uncompressing tuple", e);
		}
		return rows;
	}

	@SuppressWarnings("unchecked")
	private List<String> parseMetrics(JSONObject json, MetricsParser metricsParser, SimpleDateFormat sdf, Kryo kryo)
			throws ParseException, IOException {
		Map<String, Map<String, MetricsData>> metricsCollection = metricsParser.getMetricsCollection();
		String env = (String) json.get(Constants.CONFIG_ENVIRONMENT);
		String appname = (String) json.get(Constants.CONFIG_APPNAME);
		Set<String> buckets = metricsCollection.keySet();
		List<String> rows = new ArrayList<>();
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

						if (env != null && !env.isEmpty() && host != null && !host.isEmpty()) {
							LOGGER.info("***Final data****");
							LOGGER.info("*metricId:" + metricId + "*reverseTime:" + reverseTime + "*env:" + env
									+ "*appname:" + appname + "*host:" + host + "*instance:" + instance + "*data:"
									+ data);
							JSONObject row = new JSONObject();
							row.put("MID", metricId);
							row.put("RTIME", reverseTime);
							row.put("ENV", env);
							row.put("APP", appname);
							row.put("HOST", host);
							row.put("INSTANCE", instance);
							/*
							 * Putting METRIC in the JSON, which is of type 'Binary'
							 * throws an exception. Spark is not able to create a
							 * dataframe, all the fields are populated as null. |APP
							 * |RTIME|INSTANCE|METRIC|HOST|MID |ENV |
							 * +----+-----+--------+------+----+----+----+ |null|null
							 * |null |null |null|null|null|
							 * +----+-----+--------+------+----+----+----+ . By
							 * excluding 'METRIC' field, the code works. Fields with
							 * regular data type works.
							 */
							//row.put("METRIC", DataFormatter.serializeWithKryo(data, kryo));
							LOGGER.info("Metrics Json row:" + row.toJSONString());

							rows.add(row.toJSONString());
						}
					}
				}
			}
		}
		return rows;
	}

	@SuppressWarnings("unchecked")
	private List<String> parseDatasourceMetrics(JSONObject json, MetricsParser metricsParser, SimpleDateFormat sdf,
			Kryo kryo) throws ParseException, IOException {
		Map<String, Map<String, List<JSONObject>>> dsMetricsCollection = metricsParser.getDSMetricsCollection();
		Set<String> dsBuckets = dsMetricsCollection.keySet();
		Integer metricId = metricsMeta.get("dsmetrics");
		String env = (String) json.get(Constants.CONFIG_ENVIRONMENT);
		String appname = (String) json.get(Constants.CONFIG_APPNAME);
		List<String> rows = new ArrayList<>();
		for (String rowkey : dsBuckets) {
			Map<String, List<JSONObject>> collection = dsMetricsCollection.get(rowkey);
			Set<String> metricNames = collection.keySet();
			if (!collection.isEmpty() && rowkey != null) {
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
					jsonArray.addAll(metrics);
				}
				if (host != null && !host.isEmpty()) {
					JSONObject row = new JSONObject();
					row.put("MID", metricId);
					row.put("RTIME", reverseTime);
					row.put("ENV", env);
					row.put("APP", appname);
					row.put("HOST", host);
					row.put("INSTANCE", instance);
					/*
					 * Putting METRIC in the JSON, which is of type 'Binary'
					 * throws an exception. Spark is not able to create a
					 * dataframe, all the fields are populated as null. |APP
					 * |RTIME|INSTANCE|METRIC|HOST|MID |ENV |
					 * +----+-----+--------+------+----+----+----+ |null|null
					 * |null |null |null|null|null|
					 * +----+-----+--------+------+----+----+----+ . By
					 * excluding 'METRIC' field, the code works. Fields with
					 * regular data type works.
					 */
					//row.put("METRIC", DataFormatter.serializeWithKryo(jsonArray.toJSONString(), kryo));

					rows.add(row.toJSONString());
				}
			}
		}
		return rows;
	}
}
