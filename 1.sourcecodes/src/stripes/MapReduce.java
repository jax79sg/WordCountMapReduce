package stripes;
import java.io.IOException;
import org.apache.log4j.Logger;

import pairs.CompositeBigramKey;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 Word Stripe version
Job 1
- Read in all the text files, set delimiter in job for paragraph
- Perform tokenization
- Map:
	- emit all w	hash(w',count)	 
- Reduce
	- emit word, hashmap(wordprime',count)

Job 2
- Map:
	- Compute count(word) and count(word,word') 
	- Compute prob
	- emit compositekey
- Reduce
	- Received sorted by secondary sorting techniques, mapper placed prob value in key and let Hadoop framework do the sorting.
	- Limit to top ten
	- emit word**word'**	prob
 */
public class MapReduce extends Configured implements Tool {

	Logger logger = Logger.getLogger(MapReduce.class);
    public static void main(String[] args) throws Exception {
    	
    	long start = System.currentTimeMillis();
        int res = ToolRunner.run(new Configuration(), new MapReduce(), args);
        long duration = System.currentTimeMillis() - start;
        String durationStr=DurationFormatUtils.formatDuration(duration, "mm:ss:SS");
        System.out.println("Duration:"+durationStr);
        System.exit(res);
	}

    
    /**
     * Coordinate the jobs
     */
    public int run(String args[]) {
    	
    	
    	
        try {
        	boolean jobStatus=runJob1(args[0],args[1]);

            if (jobStatus)
            {
            	jobStatus=runJob2(args[2],args[3]);
            }
            else
            {
            	return 1;
            }
            
            if(jobStatus)
            {
            	return 0;
            }else
            {
            	return 1;
            }
            
        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }

    /**
     * Driver for job 2
     * @param input
     * @param output
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
	private boolean runJob2(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	//Step 1        	
		System.out.println("Running Job Two");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(stripes.MapReduce.class);
        // specify mapper class
        job.setMapperClass(stripes.Step2Map.class);
        // specify reducer class
        job.setReducerClass(stripes.Step2Reduce.class);
        job.setNumReduceTasks(5);
        // specify output types
        job.setOutputKeyClass(CompositeBigramKey.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(pairs.CustomCompositePartitioner.class);
        // specify input and output DIRECTORIES
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job2status=job.waitForCompletion(true);
        System.out.println("Job two sucess:"+job2status);
        return job2status;
	}

	/**
	 * Driver for Job 1
	 * @param input
	 * @param output
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private boolean runJob1(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	//Step 1        	
		System.out.println("Running job one");
        Configuration conf = new Configuration();
        //Set the default reader to read in by paragraphs instead of lines only
        conf.set("textinputformat.record.delimiter","\r\n\r\n");	
        Job job = Job.getInstance(conf);
        job.setJarByClass(stripes.MapReduce.class);
        // specify mapper class
        job.setMapperClass(stripes.Step1MapStripes.class);
        // specify reducer class
        job.setReducerClass(stripes.Step1ReduceStripes.class);
        // specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setNumReduceTasks(5);
        // specify input and output DIRECTORIES
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        boolean job1status=job.waitForCompletion(true);
        System.out.println("Job one sucess:"+job1status);
        return job1status;
	}
}