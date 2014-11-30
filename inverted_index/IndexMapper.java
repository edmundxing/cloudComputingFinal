package mapred;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

/*
 * Modified by: Manish Sapkota to create inverted indexing of the histogram features of image
 */

public class IndexMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {

	private HashMap<Integer,ArrayList<Double>> dictionary=new HashMap<Integer, ArrayList<Double>>(); 

	public HashMap<Integer,ArrayList<Double>> getFeatures(Path dictionaryPath)
	{
		try {
			HashMap<Integer,ArrayList<Double>> tempFeatures=new HashMap<Integer, ArrayList<Double>>();
			BufferedReader fis = new BufferedReader(new FileReader(dictionaryPath.toString()));
			String pattern;
			String[] data;
			int count=0;
			Integer wordindex=0;
			
			while ((pattern = fis.readLine()) != null) {
				if(pattern!=null){
					count=0;
					ArrayList<Double> feat=new ArrayList<Double>();
					StringTokenizer tokenizer=new StringTokenizer(pattern);
					while(tokenizer.hasMoreTokens())
					{
						String token=tokenizer.nextToken().toString();
						if(token!="")
						{
							// first token is word index
							if(count==0){
								wordindex=Integer.parseInt(token);
							}else{
								feat.add(Double.parseDouble(token));
							}
						}

						count++;
					}// end parsing each line

					tempFeatures.put(wordindex,feat);
									

				}// end pattern check

			}// end file read while

			return tempFeatures;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public void configure(JobConf job) 
	{
		String inputFile = job.get("map.input.file");
		Path[] paths = new Path[0];
		try 
		{
			paths = DistributedCache.getLocalCacheFiles(job);
		} catch (IOException ioe) 
		{
			System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			String dictionaryPath="dictionary/dictang.txt";
			dictionary=getFeatures(new Path(dictionaryPath));
		}

		if(paths!=null){
			for (Path eachPath : paths) {
				// Need to read all the files into hashmap here
				if (eachPath.getName().toString().trim().contains("dict")) 
				{
					// read dictionary values in Hashmap
					dictionary=getFeatures(eachPath);
				}
			}
		}else{
			String dictionaryPath="dictionary/dictang.txt";
			dictionary=getFeatures(new Path(dictionaryPath));
		}
	}

	/*
	 * @ Euclidean distance from two double arrays
	 */
	private Double calculateDistance(Object[] array1, Object[] array2)
	{
		if(array1.length!=array2.length)
		{
			System.out.println("Arrays are not of equal lengths must check it");
			return null;
		}

		//  sum( (xi-yi)^2 / (xi+yi) ) / 2
		double Sum = 0.0;

		for(int i=0;i<array1.length;i++) {
			if(((double)array1[i]+(double)array2[i]) != 0){
				Sum=Sum+ (Math.pow(((double)array1[i]-(double)array2[i]),2.0)/((double)array1[i]+(double)array2[i]));
			}
			//Sum = Sum + Math.pow((((double)array1[i])-((double)array2[i])),2.0);
		}

		return Sum/2;

		/*Math.sqrt(Sum);*/
	}

	public HashMap<Integer,Double> scaleValues(HashMap<Integer,Double> vals) {
		HashMap<Integer,Double> result = new HashMap<Integer,Double>();
		double min = minArray(vals.values().toArray());
		double max = maxArray(vals.values().toArray());
		double scaleFactor = max - min;
		// scaling between [0..1] for starters. Will generalize later.
		for (Entry<Integer, Double> entry  : vals.entrySet()) {
			result.put(entry.getKey(), ((entry.getValue() - min) / scaleFactor));
		}

		return result;
	}

	// The standard collection classes don't have array min and max.
	public double minArray(Object[] vals) {
		if(vals.length<=0) return 0;
		double min = (double)vals[0];
		for (int x = 1; x < vals.length; x++) {
			if ((double)vals[x] < min) {
				min = (double)vals[x];
			}
		}
		return min;
	}

	public double maxArray(Object[] vals) {
		if(vals.length<=0)return 0;
		double max = (double)vals[0];
		for (int x = 1; x < vals.length; x++) {
			if ((double)vals[x] > max) {
				max = (double)vals[x];
			}
		}
		return max;
	}

	private void createInvertedIndex(Object[] imageVal,String imageName,OutputCollector<Text,Text> output) {
		try {

			HashMap<Integer,Double> dictionaryStat=new HashMap<Integer, Double>();
			HashMap<Integer,Double> scaledValues=new HashMap<Integer, Double>();

			ArrayList<Double> dictionaryValue=new ArrayList<Double>(); 
			Double dist=0.0;
			Integer wordIndex=0;

			for(Entry<Integer,ArrayList<Double>> entry:dictionary.entrySet()){
				wordIndex=entry.getKey();
				dictionaryValue=entry.getValue();

				if(dictionaryValue.size()>0){
					if(dictionaryValue.size()!=imageVal.length)
					{
						System.out.println("I am not equal check me");

					}else{
						dist=calculateDistance(dictionaryValue.toArray(),imageVal);
						if(dist!=null){
							dictionaryStat.put(wordIndex,dist);
						}
					}
				}else
				{
					System.out.println("Dictionary entry null");
				}

				//dictionaryValue.clear();
			}

			scaledValues=scaleValues(dictionaryStat);

			for (Entry<Integer, Double> entry  : scaledValues.entrySet()) {
				// Scale and then output
				if(entry.getValue()<0.8){
					output.collect(new Text(entry.getKey().toString()), new Text(imageName+":"+entry.getValue().toString()));			
				}
			}


		} catch (IOException ioe) {
			System.err.println("Caught exception while dictionary : " + StringUtils.stringifyException(ioe));
		}

	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String imageName = "";

		String line = value.toString();
		//String dictionaryPath="dictionary/dictang.txt";

		StringTokenizer tokenizer = new StringTokenizer(line);
		Double val=0.0;
		Integer count=0;
		ArrayList<Double> gtvalues=new ArrayList<Double>();
		while(tokenizer.hasMoreTokens()){
			String token = tokenizer.nextToken();
			if(count==0)
			{
				imageName=token;
				count++;
				continue;
			}

			if(token!=""){
				val=Double.parseDouble(token);
				gtvalues.add(val);
			}// end if token!=""
			count++;
		}// end while

		createInvertedIndex(gtvalues.toArray(),imageName,output);

	}//end map
}
