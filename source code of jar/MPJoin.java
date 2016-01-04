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


public class MPJoin{
    public static class LeftOutJoinMapper extends Mapper<Object, Text, Text, CombineValues> {
        private  CombineValues combineValues = new CombineValues();
        private Text flag = new Text();
        private Text joinKey = new Text();
        private Text secondPart = new Text();
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
           
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            
            if(pathName.contains("u2006_process")||pathName.contains("u2007_process")||pathName.contains("u2008_process")||pathName.contains("u2009_process")){
                String[] valueItems = value.toString().split("\\s+");
             
                if(valueItems.length < 4){
                    return;
                }
		    if(valueItems[0].equals("STN---"))
		    {
			  return;
		    }
                flag.set("0");
                joinKey.set(valueItems[0]);
                secondPart.set(valueItems[1]+"\t"+valueItems[2]+"\t"+valueItems[3]);
                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(combineValues.getJoinKey(), combineValues);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
            }
            else if(pathName.contains("location_process")){
                String[] valueItems = value.toString().split("\\s+");
               
                if(valueItems.length < 4){
                    return;
                }
		     if(valueItems[0].equals("USAF"))
		    {
			  return;
		    }
                flag.set("1");
                joinKey.set(valueItems[0]);
                secondPart.set(valueItems[1]+"\t"+valueItems[2]+"\t"+valueItems[3]);
                combineValues.setFlag(flag);
                combineValues.setJoinKey(joinKey);
                combineValues.setSecondPart(secondPart);
                context.write(combineValues.getJoinKey(), combineValues);
            }
        }
    }
    public static class LeftOutJoinReducer extends Reducer<Text, CombineValues, Text, Text> {
      
        private ArrayList<Text> leftTable = new ArrayList<Text>();
       
        private ArrayList<Text> rightTable = new ArrayList<Text>();
        private Text secondPar = null;
        private Text output = new Text();
       
        @Override
        protected void reduce(Text key, Iterable<CombineValues> value, Context context)
                throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();
          
            for(CombineValues cv : value){
                secondPar = new Text(cv.getSecondPart().toString());
             
                if("0".equals(cv.getFlag().toString().trim())){
                    leftTable.add(secondPar);
                }
             
                else if("1".equals(cv.getFlag().toString().trim())){
                    rightTable.add(secondPar);
                }
            }
	    
	    if(rightTable.size()>=1)
	    {
            	Text rightPart = rightTable.get(0);
            	for(Text leftPart : leftTable){
    //             for(Text rightPart : rightTable){
                       output.set(leftPart+ "\t" + rightPart);
                       context.write(key, output);
     //            }
                }
	     }
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
		Job job = new Job(conf,"MPJoin");
		
		job.setJarByClass(MPJoin.class);
		job.setMapperClass(LeftOutJoinMapper.class);
		job.setReducerClass(LeftOutJoinReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class); 
            	job.setOutputFormatClass(TextOutputFormat.class);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
            
            	job.setMapOutputKeyClass(Text.class);
            	job.setMapOutputValueClass(CombineValues.class);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
            
            	job.setOutputKeyClass(Text.class);
            	job.setOutputValueClass(Text.class);		
		

		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileInputFormat.addInputPath(job,new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));	

		System.exit(job.waitForCompletion(true)?0:1);	
		

		
		
	}	
	
}
