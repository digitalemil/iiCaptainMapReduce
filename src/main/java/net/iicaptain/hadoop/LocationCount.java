package net.iicaptain.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapreduce.*;

public class LocationCount {
	public static final int LASTHOUR = 1, LASTDAY = 2, LASTWEEK = 3, ALL = 4;
	public static long now = System.currentTimeMillis();

	private static final IntWritable one = new IntWritable(1);
	
	public static void map(int type, Object key, Text value, org.apache.hadoop.mapreduce.MapContext context)
			throws IOException {
		Text longitude = new Text();
		Text latitude = new Text();
		Text ret = new Text();

		String line = value.toString();
		// System.out.println("Mapper: "+line+" "+now);
		StringTokenizer tk = new StringTokenizer(line, ",");

		try {
			String k = tk.nextToken();
			String principal = tk.nextToken();
			String remoteip = tk.nextToken();
			Double d = Double.parseDouble(tk.nextToken());
			d *= 100;
			long l = Math.round(d);
			longitude.set("" + l / 100.0);
			d = Double.parseDouble(tk.nextToken());
			d *= 100;
			l = Math.round(d);
			latitude.set("" + l / 100.0);
			for (int i = 0; i < 6; i++)
				tk.nextToken();
			String ts = tk.nextToken();
			long timestamp = new Long(ts);
			long delta = 60 * 60 * 1000;
			switch (type) {
			case LASTDAY:
				delta *= 24;
				break;
			case LASTWEEK:
				delta *= 24 * 7;
				break;
			case ALL:
				delta = now;
				break;
			}

			if (timestamp > now - delta) {
				ret.set(latitude + "/" + longitude);
				context.write(ret, one);
			} else {
				// System.out.println("Omitting: "+line+" "+timestamp+"<"+(now -
				// delta)+" "+delta);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class Map extends
			org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException {
			LocationCount.map(LASTHOUR, key, value, context);
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private Text ret = new Text();


		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			IntWritable iret = new IntWritable();
			iret.set(sum);
			ret.set(key + "/");
			context.write(ret, iret);
			// System.out.println("Reduce: "+key);
		}
	}

}
