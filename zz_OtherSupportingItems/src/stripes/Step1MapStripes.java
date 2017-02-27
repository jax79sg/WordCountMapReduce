package stripes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import util.CustomProperties;


/**
 * Job 1's mapper.
 * Output: word, hashmap(wordprime,1)
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step1MapStripes extends Mapper<LongWritable, Text, Text, MapWritable>{
	
    private String words = new String();
    MapWritable mapValue = new MapWritable();
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Logger logger = Logger.getLogger(Step1MapStripes.class);
    	
    	//The line contains the paragraph from one of the input files. This is done by setting the delimiter in the Driver.
    	String line = value.toString();
    	
    	
    	if(line.trim().length()!=0)
    	{
	    	CustomProperties.printDebug("Mapper1: line is " + line);
	    	ArrayList<String> tokens=tokenize(words,line);
	    	Object[] strTokens=tokens.toArray();
	    	
	    	for (int i=0;i<strTokens.length-1;i++)
	    	{
	    		mapValue.clear();
	    		Text word=new Text((String)strTokens[i]);
	    		Text wordPrimeText= new Text((String)strTokens[i+1]);
	    		if(mapValue.containsKey(wordPrimeText))
	    		{	//Actually this case won't happen cos we only getting w', not really co-occurance. I just leave the code here for my personal use in future.
	    			IntWritable currentMapValue = (IntWritable)mapValue.get(wordPrimeText);
	    			currentMapValue.set(currentMapValue.get()+1);
	    		}
	    		else
	    		{
	    			mapValue.put(wordPrimeText, new IntWritable(1));
	    		}
	    		context.write(word, mapValue);
	    	}		    		
    	}
    	else
    	{
    		CustomProperties.printDebug("Mapper1: Line is empty");
    	}
        
       
    }

    /**
     * Tokenisation code to make it as english as possible. Same as the one in pairs.
     * Removed all punctuation, see code below.
     * @param _words Redundant now.
     * @param _line
     * @return
     */
	private ArrayList<String> tokenize(String _words, String _line) {
		
		ArrayList<String> stringList = new ArrayList<String>();
		_line=_line.toLowerCase();
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
		//CustomProperties.printDebug("Replaced line: " + _line);
		
		StringTokenizer defaultTokenizer = new StringTokenizer(_line);
		while (defaultTokenizer.hasMoreTokens())
		{
			stringList.add(defaultTokenizer.nextToken());
		}
		return stringList;
	}
    
    
}