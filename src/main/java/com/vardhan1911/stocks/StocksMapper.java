package com.vardhan1911.stocks;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StocksMapper extends Mapper<LongWritable, Text, StockKey, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		
		String ticker = tokens[0].trim();
		Long timestamp = Long.parseLong(tokens[1].trim());
		Double price = Double.parseDouble(tokens[2].trim());

		StockKey sKey = new StockKey(ticker, timestamp);
		context.write(sKey, new DoubleWritable(price));
	}
}
