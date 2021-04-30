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
		String[] cleanedCols = new String[8];
		//filling cleaned cols array with only necessary columns
		if(cols.length >= 10){
			cleanedCols[0] = cols[1];//sample date
			cleanedCols[1] = cols[3];//sample site
			cleanedCols[2] = cols[5];//location
			cleanedCols[3] = cols[6];//residual
			cleanedCols[4] = cols[7];//turbidity
			cleanedCols[5] = cols[8];//fluoride
			cleanedCols[6] = cols[9];//coliform
			cleanedCols[7] = cols[10];//ecoli period
			String[] d = cols[1].split("/");
			if(d.length == 3) {
				String date = d[0] + "/" + d[2]; 
				cleanedCols[0] = date;//start date
			}
			else {
				cleanedCols[0] = cols[1];//start date, first line
			}
			String cleanedLine = String.join(",", cleanedCols);
			context.write(new Text(cleanedLine), new Text(""));
		}
	}
}
