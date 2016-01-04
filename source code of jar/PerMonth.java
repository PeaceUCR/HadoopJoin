package classes;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.GenericOptionsParser;


public class PerMonth{
    public static class LeftOutJoinMapper extends Mapper<Object, Text, Text, CombineValues> {
        private  CombineValues combineValues = new CombineValues();
        private Text flag = new Text();
        private Text joinKey = new Text();
        private Text secondPart = new Text();
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
           
           
                String[] valueItems = value.toString().split("\\s+");
             
                if(valueItems.length < 7){
                    return;
                }

                flag.set("0");
                joinKey.set(valueItems[0]);
                secondPart.set(valueItems[2]+"\t"+valueItems[3]+"\t"+valueItems[6]);
                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(combineValues.getJoinKey(), combineValues);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
      
           
        }
    }
    public static class LeftOutJoinReducer extends Reducer<Text, CombineValues, Text, Text> {
      

       
        @Override
        protected void reduce(Text key, Iterable<CombineValues> value, Context context)
                throws IOException, InterruptedException {
	    Text output = new Text();
	    String state = new String();
            int m =0;
            float p[] = new float[13];
	    float temper[] = new float[13];
	    int c[] = new int[13];
	    for(int i =0; i<13;i++)
	    {
		p[i]=0.0f;
		temper[i]=0.0f;
		c[i]=0;
	     }
            for(CombineValues cv : value){
                String secondPar = cv.getSecondPart().toString();
		String[] valueItems = secondPar.toString().split("\\s+");
		state = valueItems[2];
		m = Integer.parseInt(valueItems[0])/100;
	        m = m%100;
             	switch(m)
		{
		   case 1:
			p[1]+= Float.parseFloat(valueItems[1]);
			c[1]++;
			break;
		   case 2:
			p[2]+= Float.parseFloat(valueItems[1]);
			c[2]++;
			break;
		   case 3:
			p[3]+= Float.parseFloat(valueItems[1]);
			c[3]++;
			break;
		   case 4:
			p[4]+= Float.parseFloat(valueItems[1]);
			c[4]++;
			break;
		   case 5:
			p[5]+= Float.parseFloat(valueItems[1]);
			c[5]++;
			break;
		   case 6:
			p[6]+= Float.parseFloat(valueItems[1]);
			c[6]++;
			break;
		   case 7:
			p[7]+= Float.parseFloat(valueItems[1]);
			c[7]++;
			break;
		   case 8:
			p[8]+= Float.parseFloat(valueItems[1]);
			c[8]++;
			break;
		   case 9:
			p[9]+= Float.parseFloat(valueItems[1]);
			c[9]++;
			break;
		   case 10:
			p[10]+= Float.parseFloat(valueItems[1]);
			c[10]++;
			break;
		   case 11:
			p[11]+= Float.parseFloat(valueItems[1]);
			c[11]++;
			break;
		   case 12:
			p[12]+= Float.parseFloat(valueItems[1]);
			c[12]++;
			break;
		}

            }
	    for(int i=1;i<13;i++)
	    {	
		if(c[i]!=0)
		{
		    temper[i] = p[i]/c[i];
		    output.set(String.valueOf(i)+"\t"+String.valueOf(temper[i])+"\t"+state);
		    context.write(key, output);
		}

	    }
	
	             
    //           
                    
                       
     //           
               
	    
        }
    }
public static class CombineValues implements WritableComparable<CombineValues>{
    //private static final Logger logger = LoggerFactory.getLogger(CombineValues.class);
    private Text joinKey;
    private Text flag;
    private Text secondPart;
    public void setJoinKey(Text joinKey) {
        this.joinKey = joinKey;
    }
    public void setFlag(Text flag) {
        this.flag = flag;
    }
    public void setSecondPart(Text secondPart) {
        this.secondPart = secondPart;
    }
    public Text getFlag() {
        return flag;
    }
    public Text getSecondPart() {
        return secondPart;
    }
    public Text getJoinKey() {
        return joinKey;
    }
    public CombineValues() {
        this.joinKey =  new Text();
        this.flag = new Text();
        this.secondPart = new Text();
    }
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
    @Override
    public void write(DataOutput out) throws IOException {
        this.joinKey.write(out);
        this.flag.write(out);
        this.secondPart.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.joinKey.readFields(in);
        this.flag.readFields(in);
        this.secondPart.readFields(in);
    }
    @Override
    public int compareTo(CombineValues o) {
        return this.joinKey.compareTo(o.getJoinKey());
    }
    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "[flag="+this.flag.toString()+",joinKey="+this.joinKey.toString()+",secondPart="+this.secondPart.toString()+"]";
    }
}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"PerMonth");
		
		job.setJarByClass(PerMonth.class);
		job.setMapperClass(LeftOutJoinMapper.class);
		job.setReducerClass(LeftOutJoinReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class); 
            	job.setOutputFormatClass(TextOutputFormat.class);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
            
            	job.setMapOutputKeyClass(Text.class);
            	job.setMapOutputValueClass(CombineValues.class);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
            
            	job.setOutputKeyClass(Text.class);
            	job.setOutputValueClass(Text.class);		
		

		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	

		System.exit(job.waitForCompletion(true)?0:1);	
		

		
		
	}	
	
}
