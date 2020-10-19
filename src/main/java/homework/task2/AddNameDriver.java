package homework.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AddNameDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Homework1");

        job.setJarByClass(AddNameDriver.class);

        // Обработка meta файла
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AddNameMetaMapper.class);
        // Обработка файла avg_rating.csv
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AddNameAvgRatingMapper.class);

        job.setReducerClass(AddNameReducer.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new AddNameDriver(), args));
    }

}
