import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public static class CleanMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable();
		private Text word = new Text();
		final String DELIMITER = ",";
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringBuilder clean = new StringBuilder();
			String[] tokens = value.toString().split(DELIMITER);
			for(int i=0; i< tokens.length;i++){
				String token = tokens[i];
				token=token.trim();
				token = token.replaceAll("^\"\"", "");
				clean.append(token);
				if(i<tokens.length-1)
					clean.append(DELIMITER);  
			}
			word.set(clean.toString());
			context.write(word, one);
		}
	}
