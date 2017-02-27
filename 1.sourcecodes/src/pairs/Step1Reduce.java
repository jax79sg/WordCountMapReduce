package pairs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import util.CustomProperties;


/**
 * Job 1's reducer.
 * Simply sum up the counts for each [word,wordprime]=1 and [word,#]=1 for unique words and emit
 * Now i have all the count(word,wordprime) and count(word).
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step1Reduce extends Reducer<BigramKey, IntWritable, BigramKey, IntWritable> {
	
    private IntWritable result = new IntWritable();
    public void reduce(BigramKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {        
        int sum = 0;
        for(IntWritable v : values)
            sum += v.get();
        result.set(sum);
        CustomProperties.printDebug("Reducer1: " + key.toString()+ "\t"+ result.toString());
        context.write(key, result);
    }
}