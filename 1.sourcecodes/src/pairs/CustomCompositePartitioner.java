package pairs;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Partitioner for job 2 to send to same reducer amongst the many based on prior word
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class CustomCompositePartitioner extends Partitioner<CompositeBigramKey, IntWritable> {

	
	public int getPartition(CompositeBigramKey key, IntWritable value, int numReduceTasks) {
		String leftElement = key.getWord();  //Only interested in grouping the prior word into the same reducer.	
		return (leftElement.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}