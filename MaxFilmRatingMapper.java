import java.io.IOException;

import org.apache.hadoop.io.DoubleWriteable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxFilmRatingMapper extends MaxFilmRatingMapper<LongWritable, Text, Text, DoubleWriteable> {
	
	private Parser parser = new Parser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		parser.parse(value);
    	context.write(new Text(parser.getMovieId()), new DoubleWriteable(parser.getRating()));
    	
	}
}
