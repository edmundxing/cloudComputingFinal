package job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


public class PrecisionRecall {
	
	public PrecisionRecall(){
		// default constructor
	}
	
	// to delete the folders if exists
	private static void DeleteOutputDirectory(String outputString, Configuration conf)
			throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputString))) {
			fs.delete(new Path(outputString), true);
		}
	}

	static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
        protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                return key.toString();
        }
	}
	
	public JobConf createJob(String inputString, String outputString)
			throws IOException
			{
			JobConf conf = new JobConf();
			conf.setJarByClass(getClass());
			
			conf.setMapperClass(mapred.PRMapper.class);
			conf.setCombinerClass(mapred.PRCombiner.class);
			conf.setReducerClass(mapred.PRReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			
			conf.setOutputFormat(TextOutputFormat.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(conf, new Path[] {new Path(inputString) });
			
			// Delete output directory if it exists.
			DeleteOutputDirectory(outputString,conf);
			
			FileOutputFormat.setOutputPath(conf, new Path(outputString));
			return conf;
	}
	
	
	
}
