package stripes;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import pairs.Step1Map;




/**
 * Input: word,	hashmap(wordprime,1)
 * Output: 
 * Job 1's reducer.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step1ReduceStripes extends Reducer<Text, MapWritable, Text, MapWritable> {
	private MapWritable valueMap= new MapWritable();
	Logger logger = Logger.getLogger(Step1ReduceStripes.class);
    
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {        
    	
    	valueMap.clear();
    	//All the keys should have been grouped (E.g. 1 instance of fly, but values as 7 map entries)
    	//Double loop to build up the word, hashmap(wordprime',count)
    	for (MapWritable value : values)
    	{
    		Set<Writable> keysOfValue = value.keySet();	//All the wordprime' in the hashmap(wordprime, 1)
    		for (Writable keyOfValue : keysOfValue) //For each wordprime'
    		{    			
    			IntWritable thisCount = (IntWritable) value.get(keyOfValue); //This is typically one in our case. In actual co-occurance, this can be more than one.
    			if(valueMap.containsKey(keyOfValue))
    			{
    				IntWritable currentCount=(IntWritable)valueMap.get(keyOfValue);
    				currentCount.set(thisCount.get()+currentCount.get());
    			}
    			else
    			{
    				valueMap.put(keyOfValue, thisCount);	
    			}
    		}
    	}
    	context.write(key, valueMap);	//Return word, hashmap(wordprime',count)
    	    	
    }
}