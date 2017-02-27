package pairs;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import util.CustomProperties;


/**
 * Does nothing. its here only to take in sorted input from execution framework and emit.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step2Reduce extends Reducer<CompositeBigramKey, IntWritable, CompositeBigramKey, IntWritable> {
	
	HashMap<String,Integer> hashCounter=new HashMap<String,Integer>(); //Used to keep track of how many w,w' comes in, based on w.
    private IntWritable result = new IntWritable();

    public void reduce(CompositeBigramKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
    	if (hashCounter.containsKey(key.word.toString()))
    	{
    		int currentCount=hashCounter.get(key.word.toString()).intValue();
    		if(currentCount<10)	//Guaranteed to come here sorted desc order, so its easy to limit to top 10
    		{
    	    	CustomProperties.printDebug("Reducer2: " + key.toString() + "\t"+ result.toString());
    	        context.write(key, new IntWritable(-1));
    	        hashCounter.put(key.word.toString(), new Integer(currentCount+1));
    		}
    	}
    	else
    	{
    		//First encounter
    		hashCounter.put(key.word.toString(), new Integer(1));
        	CustomProperties.printDebug("Reducer2: " + key.toString() + "\t"+ result.toString());
            context.write(key, new IntWritable(-1));
    	}
    	
    }
}