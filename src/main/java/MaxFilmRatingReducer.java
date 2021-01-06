import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxFilmRatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double average = 0;
		int elements = 0;
		for (DoubleWritable value : values) {
			average += value.get();
			elements+=1;
	    }
		average /= elements;
	    context.write(key, new DoubleWritable(average));
	}
}
