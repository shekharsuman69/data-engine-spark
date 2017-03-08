package com.abc.de.spark.functions;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

/**
 * @author B003786
 *
 */
public class MetricsRowDeserializeFunction implements FlatMapFunction<Iterator<Row>, Row> {
	
	private static final long serialVersionUID = -7295965590916588108L;
	private static final Logger LOGGER = LoggerFactory.getLogger(MetricsRowDeserializeFunction.class);


	@Override
	public Iterable<Row> call(Iterator<Row> rowList) throws Exception {
		try {
			Kryo kryo = new Kryo();
			kryo.register(com.abc.de.vo.BaseMetrics.class);
			while (rowList.hasNext()) {
				
			
				return null;
			}
		} catch (Exception e) {
			LOGGER.error("Error uncompressing tuple", e);
		}
		return null;
	}
	
}
