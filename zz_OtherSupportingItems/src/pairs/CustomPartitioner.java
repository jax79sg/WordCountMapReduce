package pairs;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner for job 1 to send to same reducer amongst the many based on prior word
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class CustomPartitioner extends Partitioner<BigramKey, IntWritable> {

	
	public int getPartition(BigramKey key, IntWritable value, int numReduceTasks) {
		String leftElement = key.getWord();  
		return (leftElement.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
	
}