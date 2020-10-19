package homework.task3;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SearchReducer extends Reducer<Text, SearchTupleWritable, Text, SearchTupleWritable> {

    public void reduce(Text key, Iterable<SearchTupleWritable> values, Context context) throws IOException, InterruptedException {
        String query = context.getConfiguration().get("query");

        // Set Name and Rating value
        for (SearchTupleWritable val : values) {
            if (val.toString().toLowerCase().contains(query.toLowerCase())) {
                // Emit final values
                context.write(key, val);

                // Increment counter for correct records
                context.getCounter(SearchState.FOUND).increment(1);
            } else {
                // Increment counter for correct records
                context.getCounter(SearchState.OTHER).increment(1);
            }
        }
    }
}
