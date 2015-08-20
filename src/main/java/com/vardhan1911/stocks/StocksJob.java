package com.vardhan1911.stocks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce Job for Stock Prices
 * @author Vardhan
 */
public class StocksJob extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        ToolRunner.run(new Configuration(), new StocksJob(), args);
    }

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "StocksJob");
		
		job.setJarByClass(StocksJob.class);
		job.setSortComparatorClass(StocksCompositeComparator.class);
		job.setGroupingComparatorClass(StocksTickerComparator.class);
		job.setPartitionerClass(StocksTickerPartitioner.class);
		
		job.setMapOutputKeyClass(StockKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(StocksMapper.class);
		job.setReducerClass(StocksReducer.class);
		
		job.waitForCompletion(true);
		return 0;
	}
}
