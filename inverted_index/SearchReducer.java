package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.eclipse.jdt.internal.compiler.ast.ArrayAllocationExpression;

import writeable.DocSumWritable;


public class SearchReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {

	private MultipleOutputs out;
	
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String str;
		ArrayList<String> outputVals=new ArrayList<String>();
		
		while(values.hasNext())
		{
			str = values.next().toString();
			
			if(!outputVals.contains(str))
			{
				outputVals.add(str);
			}
		}
		
		for(String s:outputVals){
			output.collect(new Text(key), new Text(s));
		}
	}

}