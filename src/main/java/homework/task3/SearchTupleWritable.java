package homework.task3;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SearchTupleWritable implements Writable {

    // Sum of ratings of the given product
    private String name;
    private String rating;

    public SearchTupleWritable() {
        name = "";
        rating = "";
    }

    public SearchTupleWritable(String name, String rating){
        this.name = name;
        this.rating = rating;
    }

    public void set(String name, String rating) {
        this.name = name;
        this.rating = rating;
    }

    public String getName() {
        return name;
    }

    public String getRating() {
        return rating;
    }


    /**
     * Serialization
     *
     * @param out   output byte stream
     * @throws IOException
     */

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(rating);
    }

    /**
     * Deserialization
     *
     * @param in    input byte stream
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        rating = in.readUTF();
    }
    
    /**
     * Output for TextOutputFormat
     *
     * Average rating per product
     *
     */
    @Override
    public String toString() {
        return String.valueOf(this.name + "," + this.rating);
    }


}
