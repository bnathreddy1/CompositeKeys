

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


//	First two parameters are Input Key and Input Value. Input Key = offset of each line (remember each line is a record). Input value = Line itself
//  Second two parameters are Output Key and Output value of the Mapper. BTW, the outputs of the mapper are stored in the local file system and not on HDFS. 
//  Output Key = Country object is sent. Output Value = population in millions in that country + state combination


    public class CountryStateCompositeKeysMapper extends Mapper<LongWritable, Text, Country, IntWritable> {
    	 
        /** The cntry. */
        Country cntry = new Country();
 
        /** The cnt text. */
        Text cntText = new Text();
 
        /** The state text. */
        Text stateText = new Text();
        
        //population in a Country + State
        IntWritable populat = new IntWritable();
 
        /**
         * 
         * Reducer are optional in Map-Reduce if there is no Reducer defined in program then the output of the Mapper
         * directly write to disk without sorting.
         * 
         */
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
        	//Reader will give each record in a line to the Mapper.
        	//That line is split with the de-limiter ","
            String line = value.toString();
            
            String[] keyvalue = line.split(",");
 
            
            //Country is the first item in the line in each record
            cntText.set(new Text(keyvalue[0]));
            
            //State is the second item in the line in each record
            stateText.set(keyvalue[1]);
            
            //This is the population. BTW, we can't send Java primitive datatypes into Context object. Java primitive data types are not effective in Serialization and De-serialization.
            //So we have to use the equivalent Writable datatypes provided by mapreduce framework
            
            populat.set(Integer.parseInt(keyvalue[3]));
            
   		    System.out.println("Country = " + keyvalue[0] + "State = " + keyvalue[1] + "Population = " + keyvalue[3]);
            
            //Here you are creating an object of Country class and in the constructor assigning the country name and state
            Country cntry = new Country(cntText, stateText);
            
            //Here you are passing the country object and their population to the context object.
            //Remember that country object already implements "WritableComparable" interface which is equivalient to "Comparable" interface in Java. That implementation is in Country.java class
            //Because it implements the WritableComparable interface, the Country objects can be sorted in the shuffle phase. If WritableComparable interface is not implemented, we 
            //can't sort the objects.
            
            context.write(cntry, populat);
 
        }
    }


