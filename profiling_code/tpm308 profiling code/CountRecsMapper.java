import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) 
	throws IOException, InterruptedException {
		String line = value.toString();
		String val = "Total number of records in file:";
		context.write(new Text(val), new IntWritable(1));
	}
}
