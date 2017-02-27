package pairs;
import java.io.IOException;
import org.apache.log4j.Logger;



import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
Word pairs version 
Job 1
- Read in all the text files, set delimiter in job for paragraph
- Perform tokenization
- Map:
	- emit all w,w'	 1
	- emit all w,# 	 1  (This will be used to count the total number of w for computation later)
- Reduce: 
	- sum of all w,w' and w,#

Job 2
- Calculate Probability of occurrence
- Map:
	- Calculate prob of w,w'/w
	- emit w,w',prob	1
- Reduce
	- Used secondary sorting techniques, placed prob value in key and let Hadoop framework do the sorting.
	- Limit to top ten
 */
public class MapReduce extends Configured implements Tool {

	Logger logger = Logger.getLogger(MapReduce.class);
    public static void main(String[] args) throws Exception {
    	
    	long start = System.currentTimeMillis();
        int res = ToolRunner.run(new Configuration(), new MapReduce(), args);
        long duration = System.currentTimeMillis() - start;
        String durationStr=DurationFormatUtils.formatDuration(duration, "mm:ss:SS");
//        System.out.println("Duration:"+durationStr);
        System.exit(res);
	}

    public int run(String args[]) {
    	
    	/**
    	 * Coordinate the jobs
    	 */
    	
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

	private boolean runJob2(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	/**    	
    	 * Driver for job 2
    	 */
		System.out.println("Running Job Two");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(pairs.MapReduce.class);
        // specify mapper class
        job.setMapperClass(pairs.Step2Map.class);
        // specify reducer class
        job.setReducerClass(pairs.Step2Reduce.class);
//        job.setNumReduceTasks(5);
        // specify customised partitioner
        job.setPartitionerClass(pairs.CustomCompositePartitioner.class);
        // specify output types
        job.setOutputKeyClass(pairs.CompositeBigramKey.class);
        job.setOutputValueClass(IntWritable.class);
        // specify input and output DIRECTORIES
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job2status=job.waitForCompletion(true);
        System.out.println("Job two sucess:"+job2status);
        return job2status;
	}

	private boolean runJob1(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	/**
    	 * Driver for job 1        	
    	 */
		System.out.println("Running job one");
        Configuration conf = new Configuration();
        
        //Configure the default recordreader to use "\r\n\r\n" delimiter. 
        //This will effectively read in paragraphs from the input instead of just lines.
        conf.set("textinputformat.record.delimiter","\r\n\r\n");
        
        Job job = Job.getInstance(conf);
        job.setJarByClass(pairs.MapReduce.class);
        // specify mapper class
        job.setMapperClass(pairs.Step1Map.class);
        // specify reducer class
        job.setReducerClass(pairs.Step1Reduce.class);
        // specify customised partitioner
        job.setPartitionerClass(pairs.CustomPartitioner.class);
        // specify output types
        job.setOutputKeyClass(pairs.BigramKey.class);
        job.setOutputValueClass(IntWritable.class);
        
        //To manipulate number of reducer tasks 
        //job.setNumReduceTasks(5);
        
        // specify input and output DIRECTORIES
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        // specify the kind of output for job
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job1status=job.waitForCompletion(true);
        System.out.println("Job one sucess:"+job1status);
        return job1status;
	}
}