package com.vardhan1911.stocks;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class StocksTickerPartitioner extends Partitioner<StockKey, DoubleWritable> {

	@Override
	public int getPartition(StockKey skey, DoubleWritable value, int numPartitions) {
		int hash = skey.getTicker().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}

}
