package classes;
import java.util.HashMap;
import java.util.Arrays;
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


public class PerState{
    public static class LeftOutJoinMapper extends Mapper<Object, Text, Text, CombineValues> {
        private  CombineValues combineValues = new CombineValues();
        private Text flag = new Text();
        private Text joinKey = new Text();
        private Text secondPart = new Text();
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
           
           
                String[] valueItems = value.toString().split("\\s+");
             
                if(valueItems.length < 4){
                    return;
                }

                flag.set("0");
                joinKey.set(valueItems[3]);
                secondPart.set(valueItems[1]+"\t"+valueItems[2]);
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
	    int count = 0;
	    float high =0.0f;
	    float low = 0.0f;
       	    Text doutput = new Text();
	    Text output = new Text();
	    
	    String state = new String();
	    HashMap<Float,String> hash = new HashMap<Float,String>();//record the index of temp
	    float[] sortt = new float[12];//use for sort
            int m =0;//month
            float p[] = new float[13];//total temp
	    float temper[] = new float[13];//avg temp
	    int c[] = new int[13];// temp number
	    for(int i =0; i<13;i++)
	    {
		p[i]=0.0f;
		temper[i]=0.0f;
		c[i]=0;
	     }
            for(CombineValues cv : value){
                String secondPar = cv.getSecondPart().toString();
		String[] valueItems = secondPar.toString().split("\\s+");
		m = Integer.parseInt(valueItems[0]);
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
		    
		    switch(i)
		   {
		      case 1:
			hash.put(temper[i],"Jan.");
			break;
		      case 2:
			hash.put(temper[i],"Feb.");
			break;
		      case 3:
			hash.put(temper[i],"Mar.");
			break; 
		      case 4:
			hash.put(temper[i],"Apr.");
			break;
		      case 5:
			hash.put(temper[i],"May.");
			break; 
		      case 6:
			hash.put(temper[i],"Jun.");
			break;
		      case 7:
			hash.put(temper[i],"Jul.");
			break;
		      case 8:
			hash.put(temper[i],"Aug.");
			break;
		      case 9:
			hash.put(temper[i],"Sep.");
			break;
		      case 10:
			hash.put(temper[i],"Oct.");
			break;
		      case 11:
			hash.put(temper[i],"Nov.");
			break;
		      case 12:
			hash.put(temper[i],"Dec.");
			break;
		     }   
		     sortt[i-1]=temper[i];     
		/*    output.set(String.valueOf(i)+"\t"+String.valueOf(temper[i]));
		    context.write(key, output);*/
		}

	    }
	    Arrays.sort(sortt);
	    for(int i =0;i<12;i++)
	    {
		if(hash.containsKey(sortt[i]))
		{   
		    count= count+1;
		    
		    output.set(hash.get(sortt[i])+"\t"+String.valueOf(sortt[i]));
		    high = sortt[i];
	
		    context.write(key, output);

		    if(count ==1)
		    {
                        low = sortt[i];
		    }
		}
	     }
	     
	     doutput.set("difference"+"\t"+(high-low)+"\t"+high+"\t"+hash.get(high)+"\t"+low+"\t"+hash.get(low));
	     context.write(key, doutput);
	     

	             
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
		Job job = new Job(conf,"PerState");
		
		job.setJarByClass(PerState.class);
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
