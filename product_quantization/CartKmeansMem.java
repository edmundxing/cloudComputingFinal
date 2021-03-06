

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// implementation of cartesian kmeans in mare/reduce framework
// author: Fujun Liu fujunliu@ufl.edu 11-24-2014
public class CartKmeansMem {
	// hadoop counters are used for converge test
	private final static double stopRule = 1e-10;
	private final static double counterNorm = 1e10; 
	private static enum kmeansCounter {SumDist};
	private static Map<String, ArrayList<Double>> currcents = new HashMap<String, ArrayList<Double>>();
	private static Map<String, ArrayList<Double>> newcents = new HashMap<String, ArrayList<Double>>();
	private static int featDim, nGroup, nCluster;
	// define map class
	public static class cKmeansMapper extends Mapper<Object, Text, Text, Text>{
		private double dist = .0;
		//private int initCount = 0;
		public int findCenter(int gid, double[] subFeat){
			double min_dist = Double.MAX_VALUE;
			int min_index = -1;
			int featlen = subFeat.length;
			for (int cid = 0; cid < nCluster; ++ cid){
				double dist = .0;
				String subcentID = gid + "-" + cid;
				if (! currcents.containsKey(subcentID)){
					continue;
				}
				ArrayList<Double> subcent = currcents.get(subcentID);
				for (int i = 0; i < featlen; ++ i){
					double diff = subFeat[i] - subcent.get(i);
					dist += diff*diff;
				}
				if (dist < min_dist){
					min_dist = dist;
					min_index = cid;
				}
			}
			this.dist = min_dist;
			return min_index;
		}
		// in map, for each data, find its center and send it to reducer
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// input format: image name and features
			String[] feats = value.toString().split("\\s+");
			if (feats.length != featDim + 1){ // skip bad records
				return;
			}
			double[] featVals = new double[featDim];
			for (int i = 1; i < feats.length; ++i){
				featVals[i-1] = Double.parseDouble(feats[i]);
			}
			// 
			int subFeatLen = featDim/nGroup;
			Random generator = new Random();
			for (int gid = 0; gid < nGroup; ++ gid){
				int startIndex = gid*subFeatLen;
				int endIndex = startIndex + subFeatLen - 1;
				if (gid == nGroup-1){// last group might have different length
					endIndex = Math.max(endIndex, featDim-1);
				}
		
				
				double[] subFeat = new double[endIndex-startIndex+1];
				String subFeatStr = "";
				for (int index = startIndex; index <= endIndex; ++ index){
					subFeat[index - startIndex] = featVals[index];
					subFeatStr += featVals[index] + " ";
				}
				// if no centers found, for first iteration, random init is used
				int clusterID = generator.nextInt()%nCluster;
				//int clusterID = initCount++;
				//initCount = initCount%nCluster;
				if (currcents.size() > 0){ // assign data point to the closest cluster center 
					clusterID = findCenter(gid, subFeat);
				}
				
				context.getCounter(CartKmeansMem.kmeansCounter.SumDist).increment((long) (this.dist * CartKmeansMem.counterNorm));
				context.write(new Text(gid + "-" + clusterID), new Text(subFeatStr));
			}
			
		}
	}
	// define reduce class
	public static class combiner extends Reducer<Text, Text, Text, Text>{
		// reducer compute the mean for each cluster
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double[] center = null;
			int count = 0;
			for (Text value:values){
				String[] featstr = value.toString().split("\\s+");
				if (center == null){
					center = new double[featstr.length];
					Arrays.fill(center, 0);
				}
				for (int i = 0; i < featstr.length; ++ i){
					center[i] += Double.parseDouble(featstr[i]);
				}
				count += 1;
			}
			String cstr = Integer.toString(count);
			for (int i = 0; i < center.length; ++ i){
				cstr += "  " + center[i]; 
			}
			context.write(key, new Text(cstr));
		}
	}
	
	// define reduce class
	public static class cKmeansReducer extends Reducer<Text, Text, Text, Text>{
		// reducer compute the mean for each cluster
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double[] center = null;
			double count = 0;
			for (Text value:values){
				String[] featstr = value.toString().split("\\s+");
				int subcount = Integer.parseInt(featstr[0]);
				
				if (center == null){
					center = new double[featstr.length - 1];
					Arrays.fill(center, 0);
				}
				for (int i = 0; i < center.length; ++ i){
					center[i] += Double.parseDouble(featstr[i+1]);
				}
				count += subcount;
			}
			ArrayList<Double> newcenter = new ArrayList<Double>();
			String cstr = "";
			for (int i = 0; i < center.length; ++ i){
				center[i] /= count;
				newcenter.add(center[i]);
				cstr += center[i] + " "; 
			}
			newcents.put(key.toString(), newcenter);
			context.write(key, new Text(cstr));
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// parse parameters
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    
		if (otherArgs.length < 7) {
		      System.err.println("Usage: CartKmeans inputPath outputPath featDim nGroup nCluster nR [nIter]");
		      System.exit(2);
		}
		int nR = Integer.parseInt(otherArgs[6]);
		int nIter = 100;
		if (otherArgs.length == 8){
			nIter = Integer.parseInt(otherArgs[7]);
		}
		featDim = Integer.parseInt(otherArgs[3]);
		nGroup = Integer.parseInt(otherArgs[4]);
		nCluster = Integer.parseInt(otherArgs[5]);
		String inputPath = otherArgs[1], clusterCenterPath = otherArgs[2];
		
		/*String inputPath = "cnn_feat_500.txt", clusterCenterPath = "centers-mem";
		featDim = 500;
		nGroup = 8;
		nCluster = 256;
		int nIter = 100;
		int nR = 1;*/
		
		int iter = 0;
		
		String outputPath = "";
		double preSumDist = 0;
		while(iter < nIter){
			Job job = new Job(conf, "cartesian kmeans");
		    job.setJarByClass(CartKmeansMem.class);
		    job.setMapperClass(cKmeansMapper.class);
		    job.setCombinerClass(combiner.class);
		    job.setReducerClass(cKmeansReducer.class);
		    job.setNumReduceTasks(nR);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    //job.setOutputKeyClass(NullWritable.class);
		    //job.setOutputValueClass(NullWritable.class);
		    
		    
		    outputPath = clusterCenterPath + "-tmp-" + iter;
	    	FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job, new Path(outputPath));
		    
		    
		    job.waitForCompletion(true);
			
		    currcents = new HashMap<String, ArrayList<Double>>();
		    for(String key:newcents.keySet()){
		    	ArrayList<Double> val = newcents.get(key);
		    	currcents.put(key, val);
		    }
		    newcents = new HashMap<String, ArrayList<Double>>();
		    
		    // check for convergence
		    /*double currSumDist = ((double) job.getCounters().findCounter(CartKmeansMem.kmeansCounter.SumDist).getValue()) / counterNorm;
		    if (iter > 0 && Math.abs(currSumDist - preSumDist) < stopRule){
		    	break;
		    }
		    preSumDist = currSumDist;*/
		    // 
		    
		    ++ iter;
		}
		
		// merge several files together, in this way, we only have a single file containing all centers
		FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), new Configuration());
		OutputStreamWriter os = new OutputStreamWriter(fs.create(new Path(clusterCenterPath)));
		BufferedWriter outWriter = new BufferedWriter(os);
		for(String key:currcents.keySet()){
	    	ArrayList<Double> val = currcents.get(key);
	    	String line = key;
	    	for (int i = 0; i < val.size(); ++ i){
	    		line += " " + val.get(i).toString();
	    	}
	    	outWriter.write(line);
			outWriter.newLine();
	    }
		outWriter.close();
	}

}
