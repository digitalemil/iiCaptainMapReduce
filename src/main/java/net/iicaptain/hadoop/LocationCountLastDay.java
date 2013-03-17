package net.iicaptain.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class LocationCountLastDay extends LocationCount {

	public static class Map extends
			org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException {
			LocationCount.map(LASTDAY, key, value, context);
		}
	}
}
