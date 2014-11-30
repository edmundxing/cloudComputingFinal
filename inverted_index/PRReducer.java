package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import writeable.DocSumWritable;

public class PRReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	@Override
	   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
	           throws IOException {
		
			// Write precision and recall here
			String strVal;
			String[] strValues;
			double avgPrec=0.0;
			double avgRec=0.0;
			double acc=0.0;
			double count=0.0;
			double accCount=0.0;
			
			while(values.hasNext())
			{
				strVal = values.next().toString();
								
				strValues=strVal.split(":");
				
				if(strValues.length>3)
				{
					avgPrec=avgPrec+Double.parseDouble(strValues[0]);
					avgRec=avgRec+Double.parseDouble(strValues[1]);
					if(Integer.parseInt(strValues[2])==Integer.parseInt(strValues[3]))
						accCount++;
					
					count++;
				}
			}
			
			avgPrec=avgPrec/count;
			avgRec=avgRec/count;
			acc=accCount/count;
			
			if(accCount==20)
			{
				System.out.println("I am 100%");
			}
			
			output.collect(new Text(key), new Text(avgPrec+" "+avgRec+" "+acc));
			
		}
	}


