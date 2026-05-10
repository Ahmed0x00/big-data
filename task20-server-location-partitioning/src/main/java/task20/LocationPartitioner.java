package task20;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LocationPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        if (numReduceTasks == 0) {
            return 0;
        }
        return (key.toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
