package mapred;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import writeable.DocSumWritable;

/**
*
* @author arifn
* Modified by: Manish Sapkota to create inverted indexing of the histogram features of image
*/
public class IndexReducer extends MapReduceBase implements Reducer<Text,Text,Text,DocSumWritable> {
	
	private HashMap<String, String> map;


	private void add(String tag) {
       String[] str=tag.split(":");
       map.put(str[0], str[1]);
   }
	
	@Override
   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DocSumWritable> output, Reporter reporter)
           throws IOException {
		 map = new HashMap<String, String>();

	        while(values.hasNext()){
	            add(values.next().toString());
	        }

	        output.collect(key, new DocSumWritable(map));
       	
   }
}