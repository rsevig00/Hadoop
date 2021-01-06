import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxFilmRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private Parser parser = new Parser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		parser.parse(value);
    	context.write(new Text(parser.getMovieId()), new DoubleWritable(parser.getRating()));
    	
	}
}
