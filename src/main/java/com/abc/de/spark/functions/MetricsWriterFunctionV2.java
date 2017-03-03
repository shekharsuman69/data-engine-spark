package com.abc.de.spark.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;

/**
 * Writer implementation leveraging dataframe and spark-phoenix.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class MetricsWriterFunctionV2 implements VoidFunction<JavaRDD<String>> {

	private static final long serialVersionUID = -8384212971616625152L;
	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsWriterFunctionV2.class);

	private Configuration config;
	private SQLContext sqlContext;

	/**
	 * Constructor initializing the instance variables.
	 * 
	 * @param config
	 *            configuration object
	 * @param sqlContext
	 *            metric name to metric id mapping
	 */
	public MetricsWriterFunctionV2(Configuration config, SQLContext sqlContext) {
		this.config = config;
		this.sqlContext = sqlContext;
	}

	/**
	 * No-arg constructor, otherwise an exception would be thrown:
	 * java.io.InvalidClassException: no valid constructor
	 */
	public MetricsWriterFunctionV2() {
		/*
		 * Added to suppress InvalidClassException
		 */
	}

	@Override
	public void call(JavaRDD<String> jsonRdd) throws Exception {
		StructType schema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("MID", DataTypes.IntegerType, false),
						DataTypes.createStructField("RTIME", DataTypes.LongType, false),
						DataTypes.createStructField("ENV", DataTypes.StringType, false),
						DataTypes.createStructField("APP", DataTypes.StringType, true),
						DataTypes.createStructField("HOST", DataTypes.StringType, false),
						DataTypes.createStructField("INSTANCE", DataTypes.StringType, true) });

		LOGGER.info("Creating dataframe...with schema:" + schema.json());
		DataFrame dataFrame = sqlContext.read().schema(schema).json(jsonRdd);
		dataFrame.show(1, false);
		Map<String, String> options = new HashMap<>();
		String metricsTable = config.getStringValue(Constants.CONFIG_TABLE_NAME_METRICS, null);
		String zkQuorum = config.getStringValue(Constants.ZOOKEEPER_QUORUM, "localhost");
		String zkPort = config.getStringValue(Constants.ZOOKEEPER_PORT, "2181");
		String zkRoot = config.getStringValue(Constants.ZOOKEEPER_ROOT, "/hbase-unsecure");
		String zkUrl = zkQuorum + ":" + zkPort + ":" + zkRoot;
		options.put("table", metricsTable);
		options.put("zkUrl", zkUrl);
		LOGGER.info("Writing dataframe to Pheonix table...");
		dataFrame.write().format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).options(options).save();
		LOGGER.info("Finished writing dataframe to Pheonix table!");
	}
}
