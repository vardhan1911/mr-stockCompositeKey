package com.vardhan1911.stocks;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StocksCompositeComparator extends WritableComparator {

	protected StocksCompositeComparator() {
		super(StockKey.class, true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2) {
		StockKey skey1 = (StockKey)w1;
		StockKey skey2 = (StockKey)w2;
		
		int result = skey1.getTicker().compareTo(skey2.getTicker());
		if (0 == result) {
			result = -1*skey1.getTimestamp().compareTo(skey2.getTimestamp());
		}
		
		return result;
	}
}
