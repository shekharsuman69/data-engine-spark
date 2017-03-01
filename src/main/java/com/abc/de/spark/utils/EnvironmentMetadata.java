package com.abc.de.spark.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;
import com.abc.de.hbase.dao.HBaseDAO;
import com.abc.de.hbase.dao.HBaseResultSet;
import com.abc.de.utils.DataFormatter;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public final class EnvironmentMetadata {
	
	private EnvironmentMetadata(){
		
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentMetadata.class);
	private static Map<String, String> hostAppMapping = new HashMap<>();
	private static Map<String, String> hostEnvMapping = new HashMap<>();

	/**
	 * @param config
	 */
	public static void load(Configuration config) {
		HBaseResultSet[] resultSet = null;
		HBaseResultSet rs = null;
		String[] environments = null;
		String[] columnNames = null;
		List<String> applications = new ArrayList<>();
		String columnFamily = "cf_data";

		try {
			HBaseDAO lookupDao = getHBaseConnection(config);
			rs = lookupDao.read(Constants.CONFIG_ENVIRONMENT, columnFamily);
			environments = rs.getColumnsInColumnFamily(columnFamily);

			if (environments != null && environments.length > 0) {
				// Get the list of applications against each environment
				List<String> appKeys = new ArrayList<>();
				for (String env : environments) {
					String e = DataFormatter.safeTrim(env);
					if (e.length() > 0)
						appKeys.add(e + "." + Constants.CONFIG_APPNAME);
				}

				resultSet = lookupDao.read(appKeys, columnFamily);
				for (HBaseResultSet obj : resultSet) {
					if (obj != null) {
						columnNames = obj.getColumnsInColumnFamily(columnFamily);

						if (columnNames != null && columnNames.length > 0) {
							for (String appname : columnNames) {
								String env = obj.getRowKey().split("\\.")[0];
								if (DataFormatter.safeTrim(appname).length() > 0)
									applications.add(env + "." + appname);
							}
						}
					}

				}

				/*
				 * Get the list of hosts against each application per
				 * environment
				 */
				List<String> hostKeys = new ArrayList<>();
				for (String appname : applications) {
					hostKeys.add(appname + ".hosts");
				}
				resultSet = lookupDao.read(hostKeys, columnFamily);
				for (HBaseResultSet obj : resultSet) {
					if (obj != null) {
						columnNames = obj.getColumnsInColumnFamily(columnFamily);
						if (columnNames != null && columnNames.length > 0) {
							for (String host : columnNames) {
								String rowkey = obj.getRowKey();
								String env = rowkey.split("\\.")[0];
								String appname = rowkey.split("\\.")[1];
								hostEnvMapping.put(host, env);
								hostAppMapping.put(host, appname);
							}
						}
					}
				}
			}

			LOGGER.debug("Invoked loadEnvironmentMetadata:" + hostEnvMapping.values() + " other :"
					+ hostAppMapping.values());

		} catch (Exception e) {
			LOGGER.debug("Exception caught in loadEnvironmentMetadata:" + e);
		}
	}

	private static HBaseDAO getHBaseConnection(Configuration config) {
		HBaseDAO dao = null;
		try {
			String table = config.getStringValue(Constants.CONFIG_HBASE_TABLE_NAME, null);
			if (table == null)
				table = Constants.DEFAULT_HBASE_TABLE_NAME;

			Properties props = new Properties();
			String zk = config.getStringValue(Constants.ZOOKEEPER_QUORUM, null);
			String zkPort = config.getStringValue(Constants.ZOOKEEPER_PORT, null);
			String zkRoot = config.getStringValue(Constants.ZOOKEEPER_ROOT, null);
			props.setProperty(Constants.CONFIG_HBASE_ZOOKEEPER_QUORUM, zk);
			props.setProperty(Constants.CONFIG_HBASE_ZOOKEEPER_PORT, zkPort);
			props.setProperty(Constants.CONFIG_HBASE_ZOOKEEPER_ROOT, zkRoot);

			dao = new HBaseDAO(props, table);

		} catch (Exception e) {
			LOGGER.error("Error getting HBase Connection", e);
		}
		return dao;
	}

	public static Map<String, String> getHostAppMapping() {
		return hostAppMapping;
	}

	public static Map<String, String> getHostEnvMapping() {
		return hostEnvMapping;
	}

}
