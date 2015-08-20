package com.vardhan1911.stocks;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StocksTickerComparator extends WritableComparator {
	protected StocksTickerComparator() {
		super(StockKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		StockKey skey1 = (StockKey)w1;
		StockKey skey2 = (StockKey)w2;
		
		int result = skey1.getTicker().compareTo(skey2.getTicker());
		return result;
	}
}
