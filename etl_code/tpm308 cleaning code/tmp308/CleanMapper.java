import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) 
	throws IOException, InterruptedException {
		String line = value.toString();
		String[] cols = line.split(",");
		String[] cleanedCols = new String[10];
		//filling cleaned cols array with only necessary columns
		cleanedCols[0] = cols[0];//unique id
		cleanedCols[1] = cols[1];//indicator
		cleanedCols[2] = cols[2];//name
		cleanedCols[3] = cols[3];//measure
		cleanedCols[4] = cols[4];//measure info
		cleanedCols[5] = cols[5];//geo type
		cleanedCols[6] = cols[7];//geo place
		cleanedCols[7] = cols[8];//time period
		String[] d = cols[9].split("/");
		if(d.length == 3) {
			String date = d[0] + "/" + d[2]; 
			cleanedCols[8] = date;//start date
		}
		else {
			cleanedCols[8] = cols[9];//start date, first line
		}
		cleanedCols[9] = cols[10];//data value
		String cleanedLine = String.join(",", cleanedCols);
		context.write(new Text(cleanedLine), new Text(""));
	}
}
