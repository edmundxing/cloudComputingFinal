package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import writeable.DocSumWritable;

public class PRCombiner extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	public String GetTopKPR(Integer K)
	{

		return null;
	}

	private static HashMap<String, Double> sortByValues(HashMap map) { 
		List list = new LinkedList(map.entrySet());
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		// Here I am copying the sorted list in HashMap
		// using LinkedHashMap to preserve the insertion order
		HashMap sortedHashMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		} 
		return sortedHashMap;
	}


	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// Write precision and recall here
		String str=key.toString();

		HashMap<String,Double> imageData=new HashMap<String, Double>();
		HashMap<String,Integer> imageLabel=new HashMap<String, Integer>();
		String query="";
		Integer queryLabel=0;
		Integer tp=0;
		Integer tn=0;
		Integer totalImage=0;
		Integer[] k=new Integer[]{10,20,30,40,50,60,70,80,90,100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,
				1500,1600,1700,1800,1900,2000,2100,2200,2300,2400,2500,2600,2700,2800,2900,3000,3100,3200,3300,3400,
				3500,3600,3700,3800,3900,4000,4100,4200,4300,4400,4500,4600,4700,4800,4900,5000};

		String[] queryData=str.split(":");

		if(queryData.length>3){
			query=queryData[0];
			queryLabel=Integer.parseInt(queryData[1]);
			tp=Integer.parseInt(queryData[2]);
			tn=Integer.parseInt(queryData[3]);
		}

		String imageStr="";
		String[] tempImageData;

		while(values.hasNext())
		{
			imageStr=values.next().toString();
			tempImageData=imageStr.split(":");

			if(tempImageData.length>2)
			{
				imageData.put(tempImageData[0],Double.parseDouble(tempImageData[1]));
				imageLabel.put(tempImageData[0],Integer.parseInt(tempImageData[2]));
			}
		}

		Double tempK=0.0;

		imageData=sortByValues(imageData);
		Double matchQueryCount=0.0;
		Double missMatchQueryCount=0.0;
		Integer label=0;
		double precision=0.0;
		double recall=0.0;

		if(imageData.size()>0){
			ArrayList<String> imageNames=new ArrayList<String>(imageData.keySet());

			for(int i=0;i<k.length;i++){
				matchQueryCount=0.0;
				missMatchQueryCount=0.0;
				tempK=(double)(k[i]);

				if(tempK>imageData.size()) tempK=(double) imageData.size();

				for(int j=1;j<=tempK;j++)
				{
					imageStr=(String)imageNames.get(j);
					label=imageLabel.get(imageStr);
					if(label==queryLabel){
						matchQueryCount++;
					}else{
						missMatchQueryCount++;
					}
				}

				precision=matchQueryCount/tempK;
				
				if(queryLabel==1){
					recall=matchQueryCount/tp;
					if(matchQueryCount>missMatchQueryCount)
					{
						label=1;
					}
					else{
						label=2;
					}
				}else
				{
					recall=matchQueryCount/tn;
					
					if(matchQueryCount>missMatchQueryCount)
					{
						label=2;
					}
					else
					{
						label=1;
					}
				}
				
				output.collect(new Text(tempK.toString()), new Text(precision+":"+recall+":"+queryLabel+":"+label));			
			}
		}

	}
}


