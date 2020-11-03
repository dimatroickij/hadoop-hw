package homework.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SearchMapper extends Mapper<Object, Text, Text, SearchTupleWritable> {

    private Text prodId = new Text();
    private SearchTupleWritable search = new SearchTupleWritable();


    public void map(Object key, Text value, Context context) {
        try {
            String prodId = value.toString().split(",")[0];
            String name = value.toString().split(",")[1];
            String avgRating = value.toString().split(",")[2];

            String query = context.getConfiguration().get("query");

            // Set the key of output
            this.prodId.set(prodId);
            search.set(name, avgRating);
            context.getCounter(ParceState.CORRECT).increment(1);
            if (name.toLowerCase().contains(query.toLowerCase())) {
                // Emit the key-value pair
                context.write(this.prodId, search);
                // Increment counter for correct review
                context.getCounter(SearchState.FOUND).increment(1);
            }
            else
            {
                context.getCounter(SearchState.OTHER).increment(1);
            }
        } catch (Exception e) {
            context.getCounter(ParceState.MISSING_VALUE).increment(1);
        }

    }
}
