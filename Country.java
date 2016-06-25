
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
/**
     * 
     * The Country class implements WritabelComparator to implements custom sorting to perform group by operation. It
     * sorts country and then state.
     * 
     */
    public class Country implements WritableComparable<Country> {
 
        Text country;
        Text state;
 
        public Country(Text country, Text state) {
            this.country = country;
            this.state = state;
        }
        public Country() {
        	
        	
            this.country = new Text();
            this.state = new Text();
            
           // System.out.println("basam " + this.country);
            
           // System.out.println("basam " +this.state);
 
        }
 
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
         */
        
        
        public void write(DataOutput out) throws IOException {
        	
        	// to see which method calledd "write" method
        	System.out.println("Write method is called by" + Thread.currentThread().getStackTrace()[2].getMethodName()); // output : main
        	
            this.country.write(out);
            this.state.write(out);
 
        }
 
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
         */
        
        public void readFields(DataInput in) throws IOException {
 
         	//System.out.println("in readFields");
         	
        	// to see which method calledd "write" method
        	System.out.println("read method is called by" + Thread.currentThread().getStackTrace()[2].getMethodName()); // output : main
        	
            this.country.readFields(in);
            this.state.readFields(in);
            ;
 
        }
 
        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        
        
        public int compareTo(Country pop) {
        	
        	System.out.println("CompareTo method is called by   "+ Thread.currentThread().getStackTrace()[2].getMethodName()); // output : main
        	if (pop == null)      	
            	
                return 0;
            
            int intcnt = country.compareTo(pop.country);
            
            if (intcnt != 0) {
            	
                return intcnt;
                
            } else {
            	
                return state.compareTo(pop.state);
 
            }
        }
        
 
        /*
         * (non-Javadoc)
         * out
         * @see java.lang.Object#toString()
         */
        
//If you don't implement "toString()" or "hashCode()" methods, then the output will look like
        //Country@7877897 10
        //Country&7877897 20
        
        //meaning the hashCode generated for "Country" objects are 7877897. 
        
        
        @Override
        public String toString() {
 
        	
        	//To see which method called "toString()"
        	//Write method calls toString()
        	System.out.println("toString method is called by   " + Thread.currentThread().getStackTrace()[2].getMethodName()); // output : main
        	
        	
        	//System.out.println("toSTring");
        	
            return country.toString() + ":" + state.toString();
        }
        
        
        
        
        @Override
        public int hashCode() {
        	//what ever logic you write in hashCode doesn't matter
        	
        	//System.out.println("in hashCode");
        	
        	/*
            final int prime = 31;
            int result = 1;
            result = prime * result + ((country == null) ? 0 : country.hashCode());
            result = prime * result + ((state == null) ? 0 : state.hashCode());
            
            System.out.println(result);
            
            */
        	
            return 1 ;//hashCode;
        }
        


 
    }
