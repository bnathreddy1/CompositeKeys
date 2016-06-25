

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
 
//import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  


public class CountryStateCompositeKeysDriver {
	
	 /**
     * This main method call the MapReduce Job. Before calling the job we need to set the MapperClass, ReducerClas,
     * OutputKeyClass and OutputValueClass. We can also set the FileInputFormat and FileOutputFormat.
     * 
     * FileInputFormat use to decide number of map tasks we can also set input split size which also take part while
     * deciding number of map task.
     * 
     * Split size can min between maxSize and blockSizeMath.min(maxSize, blockSize)
     * 
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
 
       // FileUtils.deleteDirectory(new File("/Local/data/output"));
        
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "CompositeKeyDriver");
        
        //first argument is job itself
        //second argument is location of the input dataset
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        //first argument is the job itself
        //second argument is the location of the output path	    
        FileOutputFormat.setOutputPath(job, new Path(args[1]));        

        
        job.setJarByClass(CountryStateCompositeKeysDriver.class);
        
        job.setMapperClass(CountryStateCompositeKeysMapper.class);
        
        job.setReducerClass(CountryStateCompositeKeysReducer.class);
        
        job.setOutputKeyClass(Country.class);
        
        job.setOutputValueClass(IntWritable.class);
        
        //setting number of reducers to zero for toubleshooting purposes
        //job.setNumReduceTasks(0);
        
       // FileInputFormat.setMaxInputSplitSize(job, 10);
        
      //  FileInputFormat.setMinInputSplitSize(job, 100);
        
        //setting the second argument as a path in a path variable	         
        Path outputPath = new Path(args[1]);
        
        //deleting the output path automatically from hdfs so that we don't have delete it explicitly	         
        outputPath.getFileSystem(conf).delete(outputPath);
        
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


