
/*
 * IndexJob.java
 *
 * Created on May 15, 2012, 6:49:49 AM
 */

package job;


import writeable.DocSumWritable;

import java.util.concurrent.Callable;

import javax.xml.soap.Text;



// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;


/**
 *
 * @author arifn
 * Modified by: Manish Sapkota to create inverted indexing of the histogram features of image
 */
public class IndexJob implements Callable<String> {

	private String inputPaths;
	public static String dictionaryPaths;
	
		
	public void setInputPaths(String inputPaths) {
		this.inputPaths = inputPaths;
	}

	public String getInputPaths() {
		return inputPaths;
	}

	private String outputPath;

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public String getDictionaryPath(){
		return dictionaryPaths;
	}
	private RunningJob runningJob;

	public RunningJob getRunningJob() {
		return runningJob;
	}

	public String call() throws Exception {
		JobConf job = new JobConf();
		job.setJarByClass(getClass());

		/* Autogenerated initialization. */
		initJobConf(job);

		/* Custom initialization, if any. */
		initCustom(job);

		/* This is an example of how to set input and output. */
		FileInputFormat.setInputPaths(job, getInputPaths());
		if (getOutputPath() != null)
			FileOutputFormat.setOutputPath(job, new Path(getOutputPath()));

		SearchJob.DeleteOutputDirectory(getOutputPath(),job);
		
		/* You can now do any other job customization. */
		// job.setXxx(...);

		/* And finally, we submit the job.
		 * If you run the job from within Karmasphere Studio, this returned
		 * RunningJob or JobID is given to the monitoring UI. */
		JobClient client = new JobClient(job);
		this.runningJob = client.submitJob(job);
		return runningJob.getID().toString();
	}

	/**
	 * This method is executed by the workflow
	 */
	public static void initCustom(JobConf conf) {
		// Add custom initialisation here, you may have to rebuild your project before
		// changes are reflected in the workflow.
		conf.setOutputValueClass(DocSumWritable.class);

	}

	/** This method is called from within the constructor to
	 * initialize the job.
	 * WARNING: Do NOT modify this code. The content of this method is
	 * always regenerated by the Job Editor.
	 */
	@SuppressWarnings("unchecked")
	// <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initJobConf
	public static void initJobConf(JobConf conf) {
		
		// CG_INPUT_HIDDEN
		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);

		// CG_MAPPER_HIDDEN
		conf.setMapperClass(mapred.IndexMapper.class);

		// CG_MAPPER
		conf.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
		conf.setMapOutputValueClass(org.apache.hadoop.io.Text.class);

		// CG_PARTITIONER_HIDDEN
		conf.setPartitionerClass(org.apache.hadoop.mapred.lib.HashPartitioner.class);

		// CG_PARTITIONER

		// CG_COMPARATOR_HIDDEN
		conf.setOutputKeyComparatorClass(org.apache.hadoop.io.Text.Comparator.class);
		
		DistributedCache.addCacheFile(new Path(dictionaryPaths).toUri(), conf);
		
		// CG_REDUCER_HIDDEN
		conf.setReducerClass(mapred.IndexReducer.class);

		// CG_REDUCER
		//conf.setNumReduceTasks(1);
		conf.setOutputKeyClass(Text.class);


		// CG_OUTPUT_HIDDEN
		conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

		// CG_OUTPUT

		// Others
	}

	// </editor-fold>//GEN-END:initJobConf

	/**
	 * The main method.
	 *
	 * You can add custom argument parsing here.
	 */
	public static void main(String[] args) throws Exception {
		IndexJob job = new IndexJob();
		
		if(args.length<3)
		{
			System.out.println("Invalid number of arguments used syntax: IndexJob inputpath outputpath dictionarypath");
			return;
		}
		
		if (args.length >= 1)
			job.setInputPaths(args[0]);
		if (args.length >= 2)
			job.setOutputPath(args[1]);
		if (args.length >= 3)
			IndexJob.dictionaryPaths=args[2];
		
		Long startTime = System.currentTimeMillis();
		Double completionTime=0.0;
				
		job.call();
		
		/* Wish we didn't have to reproduce code from runJob() here. */
		job.getRunningJob().waitForCompletion();
		
		completionTime=Double.valueOf((System.currentTimeMillis() - startTime) / 1000.0D);
		System.out.println("Indexing Job completed in " + completionTime + " seconds");
	}

}
