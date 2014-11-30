import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class EvaluateSearch {
	private static enum evaluateCounter {all, rights};
	// map compute its top-k neighbors
	public static class evaluateMapper extends Mapper<Object, Text, Text, Text>{
		private PQSearch pq;
		private int topk;
		private int featDim, nCluster;
		
		public void setup(Context context) throws IOException {
			// find cache files containing centers
			Configuration conf = context.getConfiguration();
			String cKmeansParas = conf.get("cKmeansParas");
			String searchpara = conf.get("topk");
			topk = Integer.parseInt(searchpara);
			String[] tmp = cKmeansParas.split("-"); // paras are seperated by spaces
			if (tmp.length < 3){
				System.out.println("the cKmeans algorithms are not set correctly");
				return;
			}
			featDim = Integer.parseInt(tmp[0]);
			//nGroup = Integer.parseInt(tmp[1]);
			nCluster = Integer.parseInt(tmp[2]);
			
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
			}
			/*HashMap<String, ArrayList<Double>> centers = new HashMap<String, ArrayList<Double>>();
			ArrayList<String> imgIDs = new ArrayList<String>();
			ArrayList<ArrayList<Integer> > codes = new ArrayList<ArrayList<Integer>>();
			try {
				FileParse.centerParser(centerPaths, centers);
				FileParse.codeParser(codePaths, imgIDs, codes);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			pq = new PQSearch(codes, centers, imgIDs, nCluster);*/
			try {
				pq = new PQSearch(centerPaths, codePaths, nCluster);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] tmp = value.toString().split("\\s+");
			String imgID = tmp[0];
			double[] feat = new double[featDim];
			for (int i = 0; i < featDim; ++ i){
				feat[i] = Double.parseDouble(tmp[i+1]);
			}
			ArrayList<String> nnlist = pq.topKSearch(feat, topk);
			String nns = "";
			for (String nn:nnlist){
				nns += nn + "  ";
			}
			context.write(new Text(imgID), new Text(nns));
		}
		
	}
	// reduce compare with ground truth
	public static class evaluateReducer extends Reducer<Text, Text, Text, Text>{
		private HashMap<String, String> labels = new HashMap<String, String>();
		public void setup(Context context) throws IOException {
			// read image labels
			Path[] labelFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path path:labelFiles){
				if (path.getName().toLowerCase().contains("label")){// this is label file
					BufferedReader br = new BufferedReader(new FileReader(path.toString()));
					String line = null;
					while ((line = br.readLine()) != null){
						String[] tmp = line.split("\\s+");
						labels.put(tmp[0], tmp[1]);
					}
				}
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String query = key.toString();
			for (Text value:values){
				String[] items = value.toString().split("\\s+");
				String ret = "";
				for (String item:items){
					if (!item.equals(query)){
						context.getCounter(EvaluateSearch.evaluateCounter.all).increment(1);
						if (labels.get(query).equals(labels.get(item))){
							context.getCounter(EvaluateSearch.evaluateCounter.rights).increment(1);
						}
					}
					ret += item + "(" + labels.get(item) + ") ";
				}
				context.write(new Text(query + "(" + labels.get(query) + ")" ), new Text(ret));
			}
		
		}
	}
	
	public static void main(String[] args) throws Exception {
		// parse parameters
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    
		if (otherArgs.length < 10) {
		      System.err.println("Usage:evaluate inputPath labelPath centerPath codePath outputPath featDim nGroup nCluster topK");
		      System.exit(2);
		}
	
		String featDim = otherArgs[6], nGroup = otherArgs[7], nCluster = otherArgs[8], topk = otherArgs[9];
		String inputPath = otherArgs[1], labelPath =  otherArgs[2];
		String centerPath = otherArgs[3], codePath = otherArgs[4];
		String outputPath = otherArgs[5];
		conf.set("cKmeansParas", featDim + "-" + nGroup + "-" + nCluster);
		conf.set("topk", topk);
		
		Job job = new Job(conf, "evaluate");
	    job.setJarByClass(EvaluateSearch.class);
	    job.setMapperClass(evaluateMapper.class);
	    job.setReducerClass(evaluateReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
    	FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
    	DistributedCache.addCacheFile(new Path(labelPath).toUri(), job.getConfiguration());
	    DistributedCache.addCacheFile(new Path(centerPath).toUri(), job.getConfiguration());
	    DistributedCache.addCacheFile(new Path(codePath).toUri(), job.getConfiguration());
	    job.waitForCompletion(true);
	    
	    // put code files together
	    // merge several files together
	    FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), new Configuration());
	 	OutputStreamWriter os = new OutputStreamWriter(fs.create(new Path(outputPath)));
	 	BufferedWriter outWriter = new BufferedWriter(os);
	 	long all = job.getCounters().findCounter(EvaluateSearch.evaluateCounter.all).getValue();
	 	long rights = job.getCounters().findCounter(EvaluateSearch.evaluateCounter.rights).getValue();
	 	double acc = 1.0*rights/all;
	 	outWriter.write(Double.toString(acc));
	 	outWriter.close();
	}
}
