package com.vardhan1911.stocks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javassist.expr.Instanceof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StockKey implements WritableComparable<StockKey> {
	private String m_ticker;
	private Long m_timestamp;
	
	public StockKey(String ticker, long timestamp) {
		this.m_ticker = ticker;
		this.m_timestamp = timestamp;
	}
	
	public StockKey() {
		this.m_ticker = "";
		this.m_timestamp = new Long(-1);
	}
	
	public String getTicker() {
		return m_ticker;
	}
	
	public void setTicker(String ticker) {
		this.m_ticker = ticker;
	}
	
	public Long getTimestamp() {
		return this.m_timestamp;
	}
	
	public void setTimestamp(Long timestamp) {
		this.m_timestamp = timestamp;
	}

	@Override
	public boolean equals(Object other) {
		boolean result = false;
		if (other instanceof StockKey) {
			StockKey that = (StockKey)other;
			result = (this.m_ticker.equals(that.m_ticker) && this.m_timestamp == that.m_timestamp);
		}
		return result;
	}
	
	public void readFields(DataInput arg0) throws IOException {
		this.m_ticker = WritableUtils.readString(arg0);
		this.m_timestamp = arg0.readLong();
	}

	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, m_ticker);
		arg0.writeLong(m_timestamp);
	}

	public int compareTo(StockKey o) {
		int result = m_ticker.compareTo(o.m_ticker);
		if (0 == result) {
			result = m_timestamp.compareTo(o.m_timestamp);
		}
		return result;
	}
}
