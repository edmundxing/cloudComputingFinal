
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
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
public class CartesianKmeans {
	// hadoop counters are used for converge test
	private final static double stopRule = 1e-10;
	private final static double counterNorm = 1e10; 
	private static enum kmeansCounter {SumDist};
	
	// define map class
	public static class cKmeansMapper extends Mapper<Object, Text, Text, Text>{
		// read center files
		// center file: key is: groupid-clusterid, value is: double vector
		// by this data structure, the subcenters can have different dimension
		private HashMap<String, ArrayList<Double>> centers = new HashMap<String, ArrayList<Double>>();
		private int featDim, nGroup, nCluster;
		private double dist = .0;
		private Path[] centerFiles = null;
		//private int initCount = 0;
		
		/*protected void cleanup(Context context) throws IOException, InterruptedException{
			// I first tried the purge method, error happened on AWS
			// DistributedCache.purgeCache(context.getConfiguration()); // error happened on AWS
			// delete 
			Path[] centerFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (centerFiles != null && centerFiles.length > 0){
				// get file system
				FileSystem fs = FileSystem.get(centerFiles[0].toUri(), context.getConfiguration());
				for (Path centerfile:centerFiles){
					fs.delete(centerfile, true);
				}
			}
		}*/
		protected void setup(Context context) throws IOException{
			// find cache files containing centers
			Configuration conf = context.getConfiguration();
			String cKmeansParas = conf.get("cKmeansParas");
			String[] tmp = cKmeansParas.split("-"); // paras are seperated by spaces
			if (tmp.length < 3){
				System.out.println("the cKmeans algorithms are not set correctly");
				return;
			}
			featDim = Integer.parseInt(tmp[0]);
			nGroup = Integer.parseInt(tmp[1]);
			nCluster = Integer.parseInt(tmp[2]);
			// load center files from distributed cache
			centerFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (centerFiles != null && centerFiles.length > 0){ // chech if cache file exists
				ArrayList<String> centerPaths = new ArrayList<String>();
				
				for (Path centerFile:centerFiles){
					centerPaths.add(centerFile.toString());
				}
				try {
					FileParse.centerParser(centerPaths, centers);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
		public int findCenter(int gid, double[] subFeat){
			double min_dist = Double.MAX_VALUE;
			int min_index = -1;
			int featlen = subFeat.length;
			for (int cid = 0; cid < nCluster; ++ cid){
				double dist = .0;
				String subcentID = gid + "-" + cid;
				if (! centers.containsKey(subcentID)){
					continue;
				}
				ArrayList<Double> subcent = centers.get(subcentID);
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
				if (centers != null && centers.size() > 0){ // assign data point to the closest cluster center 
					clusterID = findCenter(gid, subFeat);
				}
				
				context.getCounter(CartesianKmeans.kmeansCounter.SumDist).increment((long) (this.dist * CartesianKmeans.counterNorm));
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
			String cstr = "";
			for (int i = 0; i < center.length; ++ i){
				center[i] /= count;
				cstr += center[i] + " "; 
			}
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
		String featDim = otherArgs[3], nGroup = otherArgs[4], nCluster = otherArgs[5];
		String inputPath = otherArgs[1], clusterCenterPath = otherArgs[2];
		conf.set("cKmeansParas", featDim + "-" + nGroup + "-" + nCluster);
		int iter = 0;
		ArrayList<URI> centerFiles = new ArrayList<URI>();
		String outputPath = "";
		double preSumDist = 0;
		while(iter < nIter){
			Job job = new Job(conf, "cartesian kmeans");
		    job.setJarByClass(CartesianKmeans.class);
		    job.setMapperClass(cKmeansMapper.class);
		    job.setCombinerClass(combiner.class);
		    job.setReducerClass(cKmeansReducer.class);
		    job.setNumReduceTasks(nR);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    outputPath = clusterCenterPath + "-tmp-" + iter;
	    	FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job, new Path(outputPath));
		    
		    for (URI path:centerFiles){
		    	DistributedCache.addCacheFile(path, job.getConfiguration());
		    }
		    
		    job.waitForCompletion(true);
			
		    // check for convergence
		    double currSumDist = ((double) job.getCounters().findCounter(CartesianKmeans.kmeansCounter.SumDist).getValue()) / counterNorm;
		    if (iter > 0 && Math.abs(currSumDist - preSumDist) < stopRule){
		    	break;
		    }
		    preSumDist = currSumDist;
		    
		    // add the center files to centerFiles
		    centerFiles.clear();
		    // list all files under path
		    //get the centers
		    FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), job.getConfiguration());
		    FileStatus[] status = fs.listStatus(new Path(outputPath));
		    // add them into center files for next iteration
		    for(FileStatus s:status){
		    	String filename = s.getPath().getName();
		    	if (filename.contains("part")){
		    		centerFiles.add(s.getPath().toUri());
		    	}
		    }
			++ iter;
		}
		
		// merge several files together, in this way, we only have a single file containing all centers
		FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), new Configuration());
		
		FileStatus[] status = fs.listStatus(new Path(outputPath));
		OutputStreamWriter os = new OutputStreamWriter(fs.create(new Path(clusterCenterPath)));
		BufferedWriter outWriter = new BufferedWriter(os);
		for (FileStatus s:status){
			if (!s.getPath().getName().contains("part")){
				continue;
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
			String line = null;
			while((line = br.readLine()) != null){
				outWriter.write(line);
				outWriter.newLine();
			}
			br.close();
		}
		outWriter.close();
	}

}
