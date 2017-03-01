package com.abc.de.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.utils.DataFormatter;

import scala.Tuple2;

/**
 * Tuple function processes [K:V] input and returns the formatted value string.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class TupleFunction implements Function<Tuple2<String, String>, String> {

	private static final long serialVersionUID = -7295965590916588108L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TupleFunction.class);

	@Override
	public String call(Tuple2<String, String> tuple) throws Exception {
		String value = null;
		try {
			value = DataFormatter.uncompress(tuple._2());
		} catch (Exception e) {
			LOGGER.error("Error uncompressing tuple", e);
		}
		return value;
	}
}
