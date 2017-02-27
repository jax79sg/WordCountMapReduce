package stripes;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import pairs.CompositeBigramKey;
import util.CustomProperties;


/**
 * Does nothing. its here only to take in sorted input from execution frameowrk and emit.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step2Reduce extends Reducer<CompositeBigramKey, IntWritable, CompositeBigramKey, IntWritable> {
	HashMap<String,Integer> hashCounter=new HashMap<String,Integer>();
    private IntWritable result = new IntWritable();

    public void reduce(CompositeBigramKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
    	if (hashCounter.containsKey(key.getWord().toString()))
    	{
    		int currentCount=hashCounter.get(key.getWord().toString()).intValue();
    		if(currentCount<10)	//Limit to top 10
    		{
    	    	CustomProperties.printDebug("Reducer2: " + key.toString() + "\t"+ result.toString());
    	        context.write(key, new IntWritable(-2));
    	        hashCounter.put(key.getWord().toString(), new Integer(currentCount+1));
    		}
    	}
    	else
    	{
    		//First encounter
    		hashCounter.put(key.getWord().toString(), new Integer(1));
        	CustomProperties.printDebug("Reducer2: " + key.toString() + "\t"+ result.toString());
            context.write(key, new IntWritable(-2));
    	}
    	
    }
}