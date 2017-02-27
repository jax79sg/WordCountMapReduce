package pairs;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import util.CustomProperties;


/**
 * Job 2's mapper
 * Using Job 1's reducer's output as its input.
 * Will compute prob and emit. Leave the final sorting to Hadoop Framework (Secondary sort technique) See CompositeBigramKey.java
 * Output of previous reducer ensured all sames [word] in [word,wordprime] come to this mapper.
	 * The sorting also ensured [word,#] is first. This contains the total count of the word.
	 * This mapper will compute;
	 * 	prob=count(word,wordprime)/count(word) for all lines.
	 * 	Then emit this
	 * 	key word+wordprime+prob
	 *  value = -1  //Just a placeholder.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step2Map extends Mapper<LongWritable, Text, CompositeBigramKey, IntWritable>{
	Logger logger = Logger.getLogger(Step2Map.class);
    private String words = new String();
    private int defaultValue = -1; 
    
    String currentWord="";
    String previousWord="";
    float countCurrentWord=0;
    float countCurrentWord_CurrentWordPrime=0;
    
	/* Output of reducer ensured all sames [word] in word,wordprime come to this mapper.
	 * The sorting also ensured word,# is first, since this character is stripped from vocab.
	 * This mapper just compute;
	 * 	prob=count(word,wordprime)/count(word) for all lines.
	 * 	key word+wordprime+prob
	 *  value = -1
	*/
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	//value ->  word**#		noOfCounts
    	//value ->  word**wordprime		noOfCounts
    	String line = value.toString();
    	float probCurrentWord_CurrentWordPrime=0;
    	
    	String[] lineSplitted = line.split("\t");
    	if (lineSplitted.length==2)
    	{
    		String previousWord=currentWord;
    		currentWord=lineSplitted[0].split(CustomProperties.keySeparatorsRegex)[0];
    		
    		if (currentWord.compareTo(previousWord)==0)
    		{
    			//Processing all sets of bigrams (w,w') with the same prior word (w)
    			countCurrentWord_CurrentWordPrime=Float.parseFloat(lineSplitted[1]);
    			probCurrentWord_CurrentWordPrime=countCurrentWord_CurrentWordPrime/countCurrentWord;
    			
    			//Using the compositekey for secondary sorting.
    			CompositeBigramKey myKey=new CompositeBigramKey(lineSplitted[0],String.valueOf(probCurrentWord_CurrentWordPrime));
    			context.write(myKey, new IntWritable(defaultValue));
    			
    			CustomProperties.printDebug("Commited: " + myKey.generateKeyString()+"\n");    			
    	    	CustomProperties.printDebug("previousword: " + previousWord);
    			CustomProperties.printDebug("currentword: " + currentWord);
    			CustomProperties.printDebug("countCurrentWord: " + countCurrentWord);
    			CustomProperties.printDebug("countCurrentWord_CurrentWordPrime: " + countCurrentWord_CurrentWordPrime);
    			CustomProperties.printDebug("probCurrentWord_CurrentWordPrime: " + probCurrentWord_CurrentWordPrime );    			
    			CustomProperties.printDebug("Commited: " + myKey.generateKeyString()+"\n");
    		}
    		else
    		{
    			//New prior word. This will contain w,#   count
    			countCurrentWord=Float.parseFloat(lineSplitted[1]);
    			
    			CustomProperties.printDebug("previousword: " + previousWord);
    			CustomProperties.printDebug("currentword: " + currentWord);
    			CustomProperties.printDebug("countCurrentWord: " + countCurrentWord);
    			CustomProperties.printDebug("countCurrentWord_CurrentWordPrime: " + countCurrentWord_CurrentWordPrime);
    			CustomProperties.printDebug("probCurrentWord_CurrentWordPrime: " + probCurrentWord_CurrentWordPrime +"\n");    			
    		}
    	}
    	else
    	{
    		logger.fatal("This input cannot be processed: " + line);
    	}
    	
    }


    
    
}