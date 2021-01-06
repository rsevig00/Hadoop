import org.apache.hadoop.io.Text;

public class Parser {
	
	private String movieId;
	private double rating;

	public void parse(String record) {
		String[] recordSplit;
		recordSplit = record.split(",");
		
		movieId = recordSplit[1];
		rating = Double.parseDouble(recordSplit[2]);
	}
	
	public void parse(Text record) {
		parse(record.toString());
	}
	
	public String getMovieId() {
		return this.movieId;
	}
	
	public double getRating() {
		return this.rating;
	}
}
