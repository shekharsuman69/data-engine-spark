package com.abc.de.spark.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.utils.DataFormatter;

import scala.Tuple2;

/**
 * mapPartition() version of the Tuple Function.Processes [K:V] input and
 * returns the formatted value string as an Iterable.
 * 
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class TupleFunctionV2 implements FlatMapFunction<Iterator<Tuple2<String, String>>, String> {

	private static final long serialVersionUID = -7295965590916588108L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TupleFunctionV2.class);

	@Override
	public Iterable<String> call(Iterator<Tuple2<String, String>> tuples) throws Exception {
		List<String> messages = new ArrayList<>();
		try {
			while (tuples.hasNext()) {
				messages.add(DataFormatter.uncompress(tuples.next()._2));
			}
		} catch (Exception e) {
			LOGGER.error("Error uncompressing tuple", e);
		}
		return messages;
	}
}
