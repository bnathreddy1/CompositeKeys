


import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;


//import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  


//Remember the two output parameters of the Mapper class will become the first two input parameters to the reducer class.
//


 public  class CountryStateCompositeKeysReducer extends Reducer<Country, IntWritable, Country, IntWritable> {
 
  // The first parameter to reduce method is "Country". The country object has country name and state name (look at the Country.java class for more details.
  // The second parameter "values" 	 is the collection of population for Country+State (this is a composite Key)
	  @Override
        public void reduce(Country key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 
            //System.out.println("Reducer started  nath");
            
        	int numberofelements = 0;
        	
           int cnt = 0;
       
           //System.out.println("Country name is " + key.country + " and state name is " + key.state);
           
        // to see which method calledd "write" method
       	   System.out.println("Reduce method in reducer is called by " + Thread.currentThread().getStackTrace()[2].getMethodName()); // output : main
           
           for (IntWritable val : values) 
           {
        	  // System.out.println(val.get());
        	   cnt = cnt+val.get();
        	   
           }
           
            	
     	
     	
 	    context.write(key, new IntWritable(cnt));
 
        }
 
    }