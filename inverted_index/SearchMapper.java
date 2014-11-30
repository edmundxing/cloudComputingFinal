package mapred;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

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

public class SearchMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {

	HashMap<Integer,ArrayList<Double>> dictionary=new HashMap<Integer, ArrayList<Double>>(); 
	HashMap<Integer,ArrayList<String>> indexes=new HashMap<Integer, ArrayList<String>>();

	public void configure(JobConf job) 
	{
		Path[] paths = new Path[0];
		try 
		{
			paths = DistributedCache.getLocalCacheFiles(job);
		} catch (IOException ioe) 
		{
			System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			String dictionaryPath="dictionary/dictcnn.txt";
			String indexPath="IIcnn/indexfile";
			dictionary=getDictionaryFeatures(new Path(dictionaryPath));
			indexes=getIndexes(new Path(indexPath));
			return;
		}

		if(paths!=null){
			for (Path eachPath : paths) {
				// Need to read all the files into hashmap here
				if (eachPath.getName().toString().trim().contains("dict")) 
				{
					// read dictionary values in Hashmap
					dictionary=getDictionaryFeatures(eachPath);
				}else if(eachPath.getName().toString().trim().contains("index"))
				{
					indexes=getIndexes(eachPath);
				}
			}
			return;
		}else{
			String dictionaryPath="dictionary/dictcnn.txt";
			String indexPath="IIcnn/indexfile";
			dictionary=getDictionaryFeatures(new Path(dictionaryPath));
			indexes=getIndexes(new Path(indexPath));
		}
	}

	public HashMap<Integer,ArrayList<String>> getIndexes(Path indexPath)
	{
		try {

			HashMap<Integer,ArrayList<String>> tempIndex=new HashMap<Integer, ArrayList<String>>();

			BufferedReader fis = new BufferedReader(new FileReader(indexPath.toString()));
			
			String pattern = null;
			Integer count=0;
			int gtIndex=0;

			// create the pattern file has array for matching from the distributed cache
			while ((pattern = fis.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(pattern);
				count=0;
				ArrayList<String> fileNames=new ArrayList<String>();
				while (tokenizer.hasMoreTokens()) {
					String token = tokenizer.nextToken();
					if(count==0)
					{
						gtIndex=Integer.parseInt(token);
						count++;
						continue;
					}

					if(count>0 && token!=""){
						fileNames.add(token);
					}// end if token!=""

					count++;
				}
				tempIndex.put(gtIndex,fileNames);
				
			}

			fis.close();

			return tempIndex;

		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '" + indexPath.toUri().toString() + "' : " + StringUtils.stringifyException(ioe));
		}

		return null;
	}

	public HashMap<Integer,ArrayList<Double>> getDictionaryFeatures(Path dictionaryPath)
	{
		try {
			HashMap<Integer,ArrayList<Double>> tempFeatures=new HashMap<Integer, ArrayList<Double>>();
			BufferedReader fis = new BufferedReader(new FileReader(dictionaryPath.toString()));
			String pattern;

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

			fis.close();
			
			return tempFeatures;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	private Double calculateDistance(Object[] array1, Object[] array2)
	{
		//  sum( (xi-yi)^2 / (xi+yi) ) / 2
		double Sum = 0.0;
		for(int i=0;i<array1.length;i++) {
			if(((Double)array1[i]+(Double)array2[i]) != 0){
				Sum=Sum+ (Math.pow(((Double)array1[i]-(Double)array2[i]),2.0)/((Double)array1[i]+(Double)array2[i]));
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
	public Double minArray(Object[] vals) {
		Double min = (Double)vals[0];
		for (int x = 1; x < vals.length; x++) {
			if ((Double)vals[x] < min) {
				min = (Double)vals[x];
			}
		}
		return min;
	}

	public Double maxArray(Object[] vals) {
		Double max = (Double)vals[0];
		for (int x = 1; x < vals.length; x++) {
			if ((Double)vals[x] > max) {
				max = (Double)vals[x];
			}
		}
		return max;
	}

	private void getWords(Object[] imageVal,String queryName,OutputCollector<Text,Text> output)
	{
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
			}
			
		}

		scaledValues=scaleValues(dictionaryStat);
		dictionaryStat.clear();

		for (Entry<Integer, Double> entry  : scaledValues.entrySet()) {
			// Scale and then output
			if(entry.getValue()<0.8){
				dictionaryStat.put(entry.getKey(), entry.getValue());
			}
		}

		matchIndexFile(dictionaryStat,queryName,output);
	}

	private String matchIndexFile(HashMap<Integer,Double> index,String queryName,OutputCollector<Text,Text> output) {
		try {

			ArrayList<String> fileNames=new ArrayList<String>();

			// create the pattern file has array for matching from the distributed cache
			for(Entry<Integer,Double> entry:index.entrySet()){
				fileNames=indexes.get(entry.getKey());
				for(String s:fileNames){
					output.collect(new Text(queryName), new Text(s));
				}
			}

		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file : " + StringUtils.stringifyException(ioe));
		}

		return null;
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String queryName = "";
		String line = value.toString();

		StringTokenizer tokenizer = new StringTokenizer(line);
		Double val=0.0;
		int count=0;

		ArrayList<Double> gtvalues=new ArrayList<Double>();

		while(tokenizer.hasMoreTokens()){
			String token = tokenizer.nextToken();
			if(count==0)
			{
				queryName=token;
				count++;
				continue;
			}

			if(token!=""){
				val=Double.parseDouble(token);
				gtvalues.add(val);
			}// end if token!=""

			count++;
		}// end while

		if(queryName!=null && !queryName.isEmpty())
			getWords(gtvalues.toArray(),queryName,output);
	}//end map
}