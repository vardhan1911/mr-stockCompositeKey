package com.vardhan1911.stocks;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StocksReducer extends Reducer<StockKey, DoubleWritable, Text, Text>{

	@Override
	public void reduce(StockKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		Double totalPrice = 0.0;
		int count = 0;
		
		for (DoubleWritable value: values) {
			count +=1;
			totalPrice += value.get();
		}
		
		context.write(new Text(key.getTicker()), new Text(Double.toString( totalPrice/count )));
	}
}
