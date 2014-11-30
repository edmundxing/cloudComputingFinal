package mapred;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

public class PRMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {

	HashMap<String,Integer> dataLabel=new HashMap<String,Integer>();
	HashMap<String,ArrayList<Double>> features=new HashMap<String, ArrayList<Double>>();
	Integer tpCount=0;
	Integer tnCount=0;
	private static String FEATURE="cnn";
	private static String LABEL="labels";
	private static String INDEXFILE="indexfile";

	public void configure(JobConf job) 
	{
		Path[] paths = new Path[0];
		try 
		{
			paths = DistributedCache.getLocalCacheFiles(job);
		} catch (IOException ioe) 
		{
			System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			String featureFile="features/"+FEATURE+".txt";
			String labelFile="features/"+LABEL+".txt";

			dataLabel=getLabels(new Path(labelFile));
			features=getFeatures(new Path(featureFile));
			return;

		}

		if(paths!=null){
			for (Path eachPath : paths) {
				// Need to read all the files into hashmap here
				if (eachPath.getName().toString().trim().equals(LABEL)) 
				{
					// read and put in the dataLabel Hashmap
					getLabels(eachPath);
				}else if(eachPath.getName().toString().trim().equals(FEATURE)){
					// read and put in the features hashmap
					getFeatures(eachPath);
				}
			}
		}else{
			String featureFile="features/"+FEATURE+".txt";
			String labelFile="features/"+LABEL+".txt";

			dataLabel=getLabels(new Path(labelFile));
			features=getFeatures(new Path(featureFile));
		}
	}

	private Double calculateDistance(Object[] array1, Object[] array2)
	{
		//  sum( (xi-yi)^2 / (xi+yi) ) / 2
		double Sum = 0.0;
		for(int i=0;i<array1.length;i++) {
			if(((double)array1[i]+(double)array2[i]) != 0){
				Sum=Sum+ (Math.pow(((double)array1[i]-(double)array2[i]),2.0)/((double)array1[i]+(double)array2[i]));
			}
			
		}

		return Sum/2;
		
	}

	public HashMap<String,Integer> getLabels(Path labelpath)
	{
		try {
			HashMap<String,Integer> tempLabels=new HashMap<String, Integer>();
			BufferedReader fis = new BufferedReader(new FileReader(labelpath.toString()));
			String pattern;
			String[] data;
			while ((pattern = fis.readLine()) != null) {
				if(pattern!=null){
					data=pattern.split(" ");
					if(data.length>1)
					{
						if(Integer.parseInt(data[1].trim())==1)
							tpCount++;
						else
							tnCount++;

						tempLabels.put(data[0], Integer.parseInt(data[1]));
					}
				}
			}
			
			fis.close();
			
			return tempLabels;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public HashMap<String,ArrayList<Double>> getFeatures(Path labelpath)
	{
		try {
			HashMap<String,ArrayList<Double>> tempFeatures=new HashMap<String, ArrayList<Double>>();
			BufferedReader fis = new BufferedReader(new FileReader(labelpath.toString()));
			String pattern;
			String[] data;
			int count=0;
			String imageName="";
			
			while ((pattern = fis.readLine()) != null) {
				if(pattern!=null){
					ArrayList<Double> feat=new ArrayList<Double>();
					count=0;
					StringTokenizer tokenizer=new StringTokenizer(pattern);
					while(tokenizer.hasMoreTokens())
					{
						String token=tokenizer.nextToken().toString();
						if(token!="")
						{
							// first token is imagename
							if(count==0){
								imageName=token;
							}else{
								feat.add(Double.parseDouble(token));
							}
						}

						count++;
					}// end parsing each line

					tempFeatures.put(imageName,feat);

				}// end pattern check

			}// end file read while

			fis.close();
			
			return tempFeatures;
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// output from here should be
		// Query:Label:TPCount:TNCount ImageName:Distance:Label

		String line = value.toString();

		StringTokenizer tokenizer = new StringTokenizer(line);
		String queryName="";
		String imageName="";
		Double dist=0.0;

		ArrayList<Double> queryFeat=new ArrayList<Double>();
		ArrayList<Double> imageFeat =new ArrayList<Double>();
		Integer queryLabel=0;
		Integer imageLabel=0;
		Integer count=0;

		while(tokenizer.hasMoreTokens()){
			String token = tokenizer.nextToken();
			
			if(token!=null){
				if(count==0){

					queryName=token;
					count++;
				}else{
					imageName=token;
				}
			}
		}
		
		if(!queryName.isEmpty() && !imageName.isEmpty()){
			queryLabel=dataLabel.get(queryName);
			imageLabel=dataLabel.get(imageName);

			queryFeat=features.get(queryName);
			imageFeat=features.get(imageName);

			dist=calculateDistance(queryFeat.toArray(),imageFeat.toArray());

			output.collect(new Text(queryName+":"+queryLabel+":"+tpCount+":"+tnCount), new Text(imageName+":"+dist+":"+imageLabel));
		}

	}
}
