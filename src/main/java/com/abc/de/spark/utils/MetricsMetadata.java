package com.abc.de.spark.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public final class MetricsMetadata {

	private MetricsMetadata() {
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentMetadata.class);

	/**
	 * @param config
	 * @return
	 */
	public static Map<String, Integer> load(Configuration config) {
		Map<String, Integer> metaMap = new HashMap<>();
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection conn = null;
		try {
			conn = getPhoenixConnection(config);
			ps = conn.prepareStatement("SELECT DISTINCT ID, NAME FROM METRICSMETA");
			rs = ps.executeQuery();
			while (rs.next()) {
				metaMap.put(rs.getString("NAME"), rs.getInt("ID"));
			}
			ps.close();
		} catch (Exception e) {
			LOGGER.error("Error in getting metrics metadata ", e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
				LOGGER.error("Error in closing resources", e);
			}
		}
		LOGGER.info("Metrics Meta:" + metaMap);
		return metaMap;
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
