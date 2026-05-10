package task20;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ServerDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: ServerDriver <input_path> <output_path> [num_reducers]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 20: Server Location Partitioning");
        job.setJarByClass(ServerDriver.class);

        job.setMapperClass(ServerMapper.class);
        job.setPartitionerClass(LocationPartitioner.class);
        job.setReducerClass(ServerReducer.class);

        int numReducers = 4;
        if (args.length == 3) {
            try {
                numReducers = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid reducer count. Defaulting to 4.");
            }
        }
        job.setNumReduceTasks(numReducers);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
