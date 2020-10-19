package homework.task2;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AddNameMetaMapper extends Mapper<Object, Text, Text, Text> {

    private Text prodId = new Text();
    private Text name = new Text();
    private Gson gson = new Gson();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Review variable
        ReviewMeta reviewMeta;

        try {
            // Assign a review instance to the variable
            reviewMeta = gson.fromJson(value.toString(), ReviewMeta.class);
        } catch (JsonParseException e) {
            // Increment counter for bad malformed json
            context.getCounter(MetaState.INVALID_JSON).increment(1);
            return;
        }

        if (reviewMeta.getProdId() == null || reviewMeta.getTitle() == null) {
            // Increment counter for review json with missing values
            context.getCounter(MetaState.MISSING_VALUE).increment(1);
            return;
        }

        // Set the key of output
        prodId.set(reviewMeta.getProdId());

        // Set the value of output
        name.set(reviewMeta.getTitle());

        // Emit the key-value pair
        context.write(prodId, name);

        // Increment counter for correct meta json
        context.getCounter(MetaState.CORRECT).increment(1);

    }
}
