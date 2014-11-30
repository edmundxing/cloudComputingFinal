import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class ParaSelect {
	private static int nCluster = 256;
	private static int nClass1 = 2730;
	private static int nClass2 = 2715;
	//private static enum paraCounter {hbCount};
	public static class paraMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// map is responsible to distribute the load
			// only value is useful
			context.write(value, value);
		}
	}
	
	public static class paraReducer extends Reducer<Text, Text, Text, Text>{
		private PQSearch pq;
		private HashMap<String, String> labels = new HashMap<String, String>();
		
		public void setup(Context context) throws IOException {
			// find cache files containing centers
			ArrayList<String> centerPaths = new ArrayList<String>();
			ArrayList<String> codePaths = new ArrayList<String>();
			Path[] labelFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			for (Path path:labelFiles){
				if (path.getName().toLowerCase().contains("center")){// this is label file
					centerPaths.add(path.toString());
				}
				if (path.getName().toLowerCase().contains("code")){// this is label file
					codePaths.add(path.toString());
				}
				if (path.getName().toLowerCase().contains("label")){// this is label file
					BufferedReader br = new BufferedReader(new FileReader(path.toString()));
					String line = null;
					while ((line = br.readLine()) != null){
						String[] tmp = line.split("\\s+");
						labels.put(tmp[0], tmp[1]);
					}
				}
			}
			try {
				pq = new PQSearch(centerPaths, codePaths, nCluster);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text value:values){
				int topk = Integer.parseInt(value.toString());
				
				
				double acc = 0;
				double recall = 0;
				int rightclassify = 0;
				int count = 0;
				
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				for (Path path:paths){
					if (!path.getName().toLowerCase().contains("feat")){// this is label file
						continue;
					}
					
					BufferedReader br = new BufferedReader(new FileReader(path.toString()));
					String line = null;
					
					while ((line = br.readLine()) != null){
						String[] tmp = line.split("\\s+");
						String imgID = tmp[0];
						double[] feat = new double[tmp.length - 1];
						for (int i = 0; i < feat.length; ++ i){
							feat[i] = Double.parseDouble(tmp[i+1]);
						}
						ArrayList<String> nnlist = pq.topKSearch(feat, topk);
						++ count;
						int rights = 0;
						for (String nn:nnlist){
							if (!imgID.equals(nn)){
								if (labels.get(imgID).equals(labels.get(nn))){
									++ rights;
								}
							}
						}
						double ratio = 1.0*rights/topk;
						acc += ratio;
						if (ratio > 0.5){// majority voting for classification
							++ rightclassify;
						}
						if (labels.equals("1")){
							recall += 1.0*rights/nClass1;
						}else{
							recall += 1.0*rights/nClass2;
						}
						//context.getCounter(paraCounter.hbCount).increment((long)1);
						//reporter.incrCounter(paraCounter.hbCount, (long)1);
						/*if (count%10 == 0){
							//context.getCounter(paraCounter.hbCount).increment((long)1);
							reporter.incrCounter(paraCounter.hbCount, (long)1);
							//reporter.progress();
						}*/
						
					}
				}
				acc /= count;
				recall /= count;
				double classifyacc = (double)rightclassify/count;
				String ret = acc + "  " + recall + "  " + classifyacc;
				context.write(key, new Text(ret));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// parse parameters
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    
		if (otherArgs.length < 8) {
		      System.err.println("Usage:evaluate input featPath labelPath centerPath codePath outputPath nCluster");
		      System.exit(2);
		}
	
		String inputPath = otherArgs[1], featPath = otherArgs[2], labelPath =  otherArgs[3];
		String centerPath = otherArgs[4], codePath = otherArgs[5];
		String outputPath = otherArgs[6];
		nCluster = Integer.parseInt(otherArgs[7]);
		
		/*String inputPath = "topkparas.txt", featPath = "llc_feat_21504.txt", labelPath =  "llc_label.txt";
		String centerPath = "matlab/centers-llc.txt", codePath = "matlab/codes-llc";
		String outputPath = "matlab/pr-llc.txt";
		nCluster = 256;*/
		
		
		{
			long milliSeconds = 2*1000*60*60;
		    conf.setLong("mapred.task.timeout", milliSeconds);
		    
			Job job = new Job(conf, "evaluate");
		    job.setJarByClass(ParaSelect.class);
		    job.setMapperClass(paraMapper.class);
		    job.setReducerClass(paraReducer.class);
		    //job.setNumReduceTasks(8);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
	    	FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job, new Path(outputPath));
		    DistributedCache.addCacheFile(new Path(featPath).toUri(), job.getConfiguration());
		    DistributedCache.addCacheFile(new Path(labelPath).toUri(), job.getConfiguration());
		    DistributedCache.addCacheFile(new Path(centerPath).toUri(), job.getConfiguration());
		    DistributedCache.addCacheFile(new Path(codePath).toUri(), job.getConfiguration());
		    
		    job.waitForCompletion(true);
		}
		
	    // merge several files into one single file
	    /*{
	    	FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), new Configuration());
	 	 	FileStatus[] status = fs.listStatus(new Path(outputPath));
	 	 	OutputStreamWriter os = new OutputStreamWriter(fs.create(new Path(outputPath)));
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
	    }*/
	}
}
