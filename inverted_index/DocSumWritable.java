package writeable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * Modified by: Manish Sapkota to create inverted indexing of the histogram features of image
 * DocSumWritable custom class to create the inverted index file from the reducer data
 */
public class DocSumWritable implements Writable{

	private HashMap<String,String> map = new HashMap<String, String>();
	
	public DocSumWritable() {
    }

    public DocSumWritable(HashMap<String,String> map){

    	this.map = map;
    }
    
    private Float getCount(String tag){
    	String val=map.get(tag);
    	Float out=0.0f;
    	if(val!=null)
    	{
    		out=Float.parseFloat(val);
    	}
    	
        return out;
    }
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Iterator<String> it = map.keySet().iterator();
        Text tag = new Text();        

        while(it.hasNext()){
        	String t = it.next();
            tag = new Text(t);            
            tag.readFields(in);
            new FloatWritable(getCount(t)).readFields(in);
        }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Iterator<String> it = map.keySet().iterator();
        Text tag = new Text();
        IntWritable count = new IntWritable();
        
        while(it.hasNext()){            
        	String t = it.next();            
            new Text(t).write(out);
            new FloatWritable(getCount(t)).write(out);
        }
		
	}

	@Override
    public String toString() {        

		String output = "";
        
        for(String tag : map.keySet()){        	                                   
            //output += (tag+"=>"+getCount(tag).toString()+" ");
        	output += (tag+" ");
        }
        
        return output;
        
    }
	
}
