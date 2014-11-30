import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
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
/*
 * This is for building code book
 * Author: Fujun Liu
 */
public class BuildCodeBook {
	public static class buildMapper extends Mapper<Object, Text, Text, Text>{
		// read center files
		private HashMap<String, ArrayList<Double>> centers = new HashMap<String, ArrayList<Double>>();
		private int featDim, nGroup, nCluster;

		public void setup(Context context) throws IOException{
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
			
			Path[] centerFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
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
			//this.dist = min_dist;
			return min_index;
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// input format: image name and features
			String[] feats = value.toString().split("\\s+");
			if (feats.length != featDim + 1){ // skip bad records
				return;
			}
			String imgID = feats[0];
			double[] featVals = new double[featDim];
			for (int i = 1; i < feats.length; ++i){
				featVals[i-1] = Double.parseDouble(feats[i]);
			}
			// 
			int subFeatLen = featDim/nGroup;
			String code = "";
			for (int gid = 0; gid < nGroup; ++ gid){
				int startIndex = gid*subFeatLen;
				// figure out sub center length
				for (int cid = 0; cid < nCluster; ++ cid){
					if (centers.containsKey(gid+"-"+cid)){
						subFeatLen = centers.get(gid+"-"+cid).size();
						break;
					}
				}
				
				
				int endIndex = startIndex + subFeatLen - 1;
				//if (gid == nGroup-1){// last group might have different length
					//endIndex = Math.max(endIndex, featDim-1);
				//}
				// put sub-feat into arraywritable
				double[] subFeat = new double[endIndex-startIndex+1];
				for (int index = startIndex; index <= endIndex; ++ index){
					subFeat[index - startIndex] = featVals[index];
				}
				int clusterID = findCenter(gid, subFeat);
				code += clusterID + "  ";
			}
			context.write(new Text(imgID), new Text(code));
			
		}
	}
	public static class buildReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text value:values){
				context.write(key, value);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// parse parameters
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    
		if (otherArgs.length < 6) {
		      System.err.println("Usage: buildCodeBook inputPath centerPath outputPath featDim nGroup nCluster");
		      System.exit(2);
		}
	
		String featDim = otherArgs[4], nGroup = otherArgs[5], nCluster = otherArgs[6];
		String inputPath = otherArgs[1], centerPath = otherArgs[2], codePath = otherArgs[3];
		conf.set("cKmeansParas", featDim + "-" + nGroup + "-" + nCluster);
		String outputPath = inputPath + "-codes";
		Job job = new Job(conf, "build code book");
	    job.setJarByClass(BuildCodeBook.class);
	    job.setMapperClass(buildMapper.class);
	    job.setReducerClass(buildReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
    	FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    DistributedCache.addCacheFile(new Path(centerPath).toUri(), job.getConfiguration());
	    job.waitForCompletion(true);
	    
	    // put code files together
	    // merge several files together
	    FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), new Configuration());
	 	FileStatus[] status = fs.listStatus(new Path(outputPath));
	 	OutputStreamWriter os = new OutputStreamWriter(fs.create(new Path(codePath)));
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
