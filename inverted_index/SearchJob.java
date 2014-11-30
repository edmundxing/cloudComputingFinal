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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;



public class SearchJob {
	// to delete the folders if exists
	public static void DeleteOutputDirectory(String outputString, Configuration conf)
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
	
	public static void main(String[] args) throws IOException {
		if(args.length<4)
		{
			System.out.println("Invalid number of arguments, usage : SearchJob input output dictionary invertedindex");return;
		}
		String inputString = args[0];
		String outputString = args[1];
		String invertedindexfile = args[3];
		String dictionaryfile = args[2];
		
		Long startTime = System.currentTimeMillis();
		Double completionTime=0.0;
		RunningJob runningJob;
		JobClient client = new JobClient();
		JobConf conf = new JobConf(SearchJob.class);

		// Distributed caching for the invertedindexfile
		DistributedCache.addCacheFile(new Path(invertedindexfile).toUri(), conf);
		DistributedCache.addCacheFile(new Path(dictionaryfile).toUri(), conf);
		
		
		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, args[0]);

		if (args[1] != null)
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// specify a mapper
		conf.setMapperClass(mapred.SearchMapper.class);
				
		// specify a reducer
		conf.setReducerClass(mapred.SearchReducer.class);
		
		//conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
		conf.setOutputFormat(MultiFileOutput.class);

		// delete the output folder if it exists
		//DeleteOutputDirectory(outputString,conf);

		client.setConf(conf);
		
		
		try {
			runningJob = JobClient.runJob(conf);
			runningJob.waitForCompletion();
			completionTime=Double.valueOf((System.currentTimeMillis() - startTime) / 1000.0D);
			System.out.println("Searching Job completed in " + completionTime + " seconds");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// create the precision recall job and run it
		String prout="prout";
		PrecisionRecall prjob=new PrecisionRecall();
		JobConf prconf = prjob.createJob(outputString, prout);
		
		//DistributedCache.addCacheFile(new Path(featuresfile).toUri(), conf);
		//DistributedCache.addCacheFile(new Path(dictionaryfile).toUri(), conf);
		//DistributedCache.addCacheFile(new Path(labelfile).toUri(), conf);
		
		client.setConf(prconf);
		
		// timing for pr job
		startTime = System.currentTimeMillis();
		runningJob = JobClient.runJob(prconf);
		runningJob.waitForCompletion();
		
		completionTime=Double.valueOf((System.currentTimeMillis() - startTime) / 1000.0D);
		System.out.println("PR Job completed in " + completionTime + " seconds");
		
	}

}
