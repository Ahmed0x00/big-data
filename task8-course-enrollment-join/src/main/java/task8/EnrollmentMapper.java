package task8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EnrollmentMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    enum Counters {
        MALFORMED_ENROLLMENT_LINES
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // enrollmentId, courseId, studentName, enrollmentDate
        String[] parts = value.toString().split(",");

        if (parts.length == 4) {
            String courseId = parts[1].trim();
            String studentName = parts[2].trim();
            String enrollmentDate = parts[3].trim();

            outKey.set(courseId);
            outValue.set("enroll~" + studentName + "," + enrollmentDate);
            context.write(outKey, outValue);
        } else {
            context.getCounter(Counters.MALFORMED_ENROLLMENT_LINES).increment(1);
        }
    }
}
