package com.abc.de.spark.functions;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollupFiveFunction implements Function2<Row,Row, Row>{
	
	private static final long serialVersionUID = -7295965590916588108L;
	private static final Logger LOGGER = LoggerFactory.getLogger(RollupFiveFunction.class);
	
	@Override
	public Row call(Row r1, Row r2) throws Exception{
		Row finalRow = null;
		
		return finalRow;
	}

}
