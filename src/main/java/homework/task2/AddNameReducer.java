package homework.task2;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AddNameReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Text name = new Text("");
        double rating = -1;

        // Set Name and Rating value
        for (Text val : values) {
            try {
                rating = Double.parseDouble(val.toString());
            } catch (NumberFormatException e) {
                name = new Text(val.toString());
            }
        }

        // Проверка на заполненность всех полей
        if (name.toString().equals("") || rating == -1) {
            context.getCounter(ResultState.MISSING_VALUE).increment(1);
            return;
        }

        // Emit final values
        context.write(key, new Text(name + "," + rating));

        // Increment counter for correct records
        context.getCounter(ResultState.CORRECT).increment(1);
    }
}
