package pairs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import util.CustomProperties;


/**
 * Job 1's mapper.
 * Just read a file, tokenise, and emit [word,wordprime] with value of 1.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step1Map extends Mapper<LongWritable, Text, BigramKey, IntWritable>{
	
    private String words = new String();
    private String uniqueWordIdentifier="#";
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Logger logger = Logger.getLogger(Step1Map.class);
    	
    	//Read from one text file. A setting in the Driver code has made sure this comes in a paragraph instead of just lines.
    	String line = value.toString();
    	
    	if(line.trim().length()!=0)
    	{
    		//Content is not empty, start the algo here.
	    	CustomProperties.printDebug("Mapper1: line is " + line);
	    	ArrayList<String> tokens=tokenize(words,line);
	    	Object[] strTokens=tokens.toArray();
	    	
	    	for (int i=0;i<strTokens.length-1;i++)
	    	{
	    		//For each token,token' in the paragraph, place them into the Bigram class. 
				BigramKey bigramKey = new BigramKey((String)strTokens[i],(String)strTokens[i+1]);				
				context.write(bigramKey,new IntWritable(1));
				CustomProperties.printDebug("Mapper1: " +  bigramKey.toString() + "\t"+ 1);
				
				if(i!=strTokens.length-1)
				{
					//This is to keep track on all unique words processed.
					//Format pattern is  w,#
					BigramKey UniqueWordKey = new BigramKey((String)strTokens[i],uniqueWordIdentifier);
					context.write(UniqueWordKey,new IntWritable(1));
					CustomProperties.printDebug("Mapper1: " +  UniqueWordKey.toString() + "\t"+ 1);
				}
	
			}
	    	
    	}
    	else
    	{
    		CustomProperties.printDebug("Mapper1: Line is empty");
    	}
        
       
    }

    /**
     * Tokenisation rules
     * Note: Tried to make the results as English as possible.
     * - Removed all punctuation as seen in the code below.
     * E.g. __word__ is now word.
     * @param _words
     * @param _line
     * @return
     */
    private ArrayList<String> tokenize(String _words, String _line) {
		ArrayList<String> stringList = new ArrayList<String>();
		_line=_line.toLowerCase();
		_line=_line.replaceAll("\n", " ").replaceAll("\r", " ");
		_line=_line.replaceAll("\\.", "");
		_line=_line.replaceAll("\\?", "");
		_line=_line.replaceAll("\"", "");
		_line=_line.replaceAll(";", "");
		_line=_line.replaceAll("-", "");
		_line=_line.replaceAll("_", "");
		_line=_line.replaceAll("\\*", "");
		_line=_line.replaceAll("\\(", "");
		_line=_line.replaceAll("\\)", "");
		_line=_line.replaceAll(",", "");
		_line=_line.replaceAll("#", "");
		_line=_line.replaceAll("!", "");
		_line=_line.replaceAll(":", "");
		CustomProperties.printDebug("Replaced line: " + _line);
		
		StringTokenizer defaultTokenizer = new StringTokenizer(_line);
		while (defaultTokenizer.hasMoreTokens())
		{
			stringList.add(defaultTokenizer.nextToken());
		}
		return stringList;
	}
    
    
}