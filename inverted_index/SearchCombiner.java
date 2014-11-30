package mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;



public class SearchCombiner extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
	
	@Override
   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
           throws IOException {
		
		StringBuffer temp=new StringBuffer();
		
		// QueryImage|GroundTruthImage for key
		// index:val values
		String strKey=key.toString();
		String[] outKeys=strKey.split(":");// split to get the query and gt
		if(outKeys.length>1){
			String queryName=outKeys[0];
			String imageName=outKeys[1];
			/*Double minDistance=0.0;
			String v;
			
			while(values.hasNext()){
				v=values.next().toString();
				if(minDistance>Double.parseDouble(v))
				  minDistance=Double.parseDouble(v);
			}*/
				
			output.collect(new Text(queryName), new Text(imageName));
		}
   }
}
