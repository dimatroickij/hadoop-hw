package homework.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddNameAvgRatingMapper extends Mapper<Object, Text, Text, Text> {

    private Text prodId = new Text();
    private Text avgRating = new Text();

    public void map(Object key, Text value, Context context) {
        try {
            String prodId = value.toString().split(",")[0];
            String avgRating = value.toString().split(",")[1];

            // Set the key of output
            this.prodId.set(prodId);

            // Set the value of output
            this.avgRating.set(avgRating);

            // Emit the key-value pair
            context.write(this.prodId, this.avgRating);

            // Increment counter for correct review
            context.getCounter(AvgRatingState.CORRECT).increment(1);
        } catch (Exception e) {
            context.getCounter(AvgRatingState.MISSING_VALUE).increment(1);
        }
    }
}
