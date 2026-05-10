package task8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CourseMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    enum Counters {
        MALFORMED_COURSE_LINES
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // courseId, courseName, instructor
        String[] parts = value.toString().split(",");

        if (parts.length == 3) {
            String courseId = parts[0].trim();
            String courseName = parts[1].trim();
            String instructor = parts[2].trim();

            outKey.set(courseId);
            outValue.set("course~" + courseName + "," + instructor);
            context.write(outKey, outValue);
        } else {
            context.getCounter(Counters.MALFORMED_COURSE_LINES).increment(1);
        }
    }
}
