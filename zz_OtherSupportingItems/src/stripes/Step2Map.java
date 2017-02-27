package stripes;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import pairs.CompositeBigramKey;
import util.CustomProperties;


/**
 * Job 2's mapper
 * Using Job 1's reducer's output as its input.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step2Map extends Mapper<Text, MapWritable, CompositeBigramKey, IntWritable>{
	Logger logger = Logger.getLogger(Step2Map.class);

    
    String currentWord="";
    String previousWord="";
    float countCurrentWord=0;
    float countCurrentWord_CurrentWordPrime=0;
    
    public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
    
    	//Key ->  Text: word		Value -> 	{wordprime}=count
    	
    	String currentWord = key.toString();
    	
    	//Compute count(word)
    	//Input here is word, hashmap(wordprime, count). 
    	//All the occurrance of word will be all here.
    	int count_word=0;
    	Iterator itrValues=value.values().iterator();    	
    	while (itrValues.hasNext())
    	{
    		IntWritable currentCount = (IntWritable)itrValues.next();
    		count_word=currentCount.get()+count_word;
    	}

    	
    	//Compute count(word,wordprime)
    	//Save to CompositeBigramKey for secondary sorting.
		Set<Writable> valuekeys = value.keySet();
		for (Writable valuekey : valuekeys)
		{	
			String currentWordPrime=valuekey.toString();
			IntWritable myCount = (IntWritable) value.get(valuekey);
			int count_word_wordprime=myCount.get();
			float prob = (float)count_word_wordprime/(float)count_word;
			CompositeBigramKey thisKey = new CompositeBigramKey(currentWord+CustomProperties.keySeparatorsStr+currentWordPrime, String.valueOf(prob));
			context.write(thisKey, new IntWritable(0));		
			
			if(thisKey.toString().compareTo("i")==0)
			{
				System.out.println("Mapper2: "+thisKey.generateKeyString());
			}
			    		
		}

    	
    }


    
    
}