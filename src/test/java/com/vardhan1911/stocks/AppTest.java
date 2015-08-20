package com.vardhan1911.stocks;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * See the <a href="https://cwiki.apache.org/confluence/display/MRUNIT/Index">MRUnit Wiki</a> for more information.
 */
public class AppTest {
   MapReduceDriver<LongWritable, Text, StockKey, DoubleWritable, Text, Text> mapReduceDriver;
   MapDriver<LongWritable, Text, StockKey, DoubleWritable> mapDriver;
   ReduceDriver<StockKey, DoubleWritable, Text, Text> reduceDriver;

   @Before
   public void setUp() {
	   StocksMapper mapper = new StocksMapper();
	   StocksReducer reducer = new StocksReducer();
	   mapDriver = new MapDriver<LongWritable, Text, StockKey, DoubleWritable>();
	   mapDriver.setMapper(mapper);
	   
//      WordCount.WordCountMapper mapper = new WordCount.WordCountMapper();
//      WordCount.WordCountReducer reducer = new WordCount.WordCountReducer();
//      mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
//      mapDriver.setMapper(mapper);
//      reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
//      reduceDriver.setReducer(reducer);
//      mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
//      mapReduceDriver.setMapper(mapper);
//      mapReduceDriver.setReducer(reducer);
   }

   @Test
   public void testMapper() throws IOException {
      mapDriver.withInput(new LongWritable(1), new Text("goog,1,100.0"));
      mapDriver.withOutput(new StockKey("goog", 1), new DoubleWritable(100.0));
//      mapDriver.withOutput(new Text("cat"), new LongWritable(1));
//      mapDriver.withOutput(new Text("dog"), new LongWritable(1));
      mapDriver.runTest();
   }

   @Test
   public void testReducer() throws IOException {
//      List<LongWritable> values = new ArrayList<LongWritable>();
//      values.add(new LongWritable(1));
//      values.add(new LongWritable(1));
//      reduceDriver.withInput(new Text("cat"), values);
//      reduceDriver.withOutput(new Text("cat"), new LongWritable(2));
//      reduceDriver.runTest();
   }

   @Test
   public void testMapReduce() throws IOException {
//      mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
//      mapReduceDriver.addOutput(new Text("cat"), new LongWritable(2));
//      mapReduceDriver.addOutput(new Text("dog"), new LongWritable(1));
//      mapReduceDriver.runTest();
   }
}