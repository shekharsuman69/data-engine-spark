package com.abc.de.spark.handler;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Configuration;
import com.abc.de.config.Constants;

/**
 * Abstract base handler for different types of messages. Initializes the
 * basic application configuration. Any class that extends this abstract class
 * must implement 'execute' method.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public abstract class BaseHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(BaseHandler.class);

	/**
	 * This object serves as the main entry point for all Spark Streaming
	 * functionality.
	 */
	protected JavaStreamingContext jssc = null;
	protected String appName;
	protected long duration;
	protected String brokers;
	protected String topic;
	protected String zookeeper;
	/**
	 * Kafka scales topic consumption by distributing partitions among a
	 * consumer group, which is a set of consumers sharing a common group
	 * identifier. You could start multiple spark jobs reading from the same
	 * topic and specify the same consumer group to increase performance and
	 * ensure that each message is read only once.
	 */
	protected String consumerGroup;

	/**
	 * The full Phoenix JDBC URL
	 */
	protected String phoenixUrl;
	protected Configuration config;

	/**
	 * @param config
	 */
	public BaseHandler(Configuration config) {
		this.config = config;
		init(config);
	}

	protected void init(Configuration config) {
		try {
			this.setAppName(config.getStringValue(Constants.APPNAME, "Default Handler"));
			this.setDuration(config.getLongValue(Constants.DURATION, 500));
			this.setBrokers(config.getStringValue(Constants.KAFKA_BROKER, "localhost:9092"));
			this.setTopic(config.getStringValue(Constants.KAFKA_TOPIC, "Default"));
			this.setConsumerGroup(config.getStringValue(Constants.KAFKA_GROUP_ID, "Default-Handler"));
			this.setPhoenixUrl(config.getStringValue(Constants.PHOENIX_JDBC_URL, null));
		} catch (Exception e) {
			LOGGER.error("Exception initializing the application", e);
		}
	}

	protected abstract void execute();

	/**
	 * Prints the application configuration for debugging purposes
	 */
	public void printApplicationInfo() {
		LOGGER.info("**********************Application Configuration***********************");

		LOGGER.info("Application Name:                {}", appName);
		LOGGER.info("Kafka Topic Poll Duration:       {}", duration);
		LOGGER.info("Kafka Broker List:               {}", brokers);
		LOGGER.info("Kafka Topic:                     {}", topic);
		LOGGER.info("Kafka Consumer Group:            {}", consumerGroup);
		LOGGER.info("Phoenix JDBC URL:                {}", phoenixUrl);

		LOGGER.info("**********************************************************************");
	}

	public JavaStreamingContext getJssc() {
		return jssc;
	}

	public void setJssc(JavaStreamingContext jssc) {
		this.jssc = jssc;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public String getBrokers() {
		return brokers;
	}

	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(String zookeeper) {
		this.zookeeper = zookeeper;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getPhoenixUrl() {
		return phoenixUrl;
	}

	public void setPhoenixUrl(String phoenixUrl) {
		this.phoenixUrl = phoenixUrl;
	}

	public Configuration getConfig() {
		return config;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}
}
