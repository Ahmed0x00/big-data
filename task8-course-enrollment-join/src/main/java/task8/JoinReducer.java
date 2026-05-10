package task8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String courseName = "UNKNOWN_COURSE";
        String instructor = "UNKNOWN_INSTRUCTOR";
        List<String> enrollments = new ArrayList<String>();

        for (Text val : values) {
            String tagged = val.toString();
            if (tagged.startsWith("course~")) {
                String payload = tagged.substring("course~".length());
                String[] parts = payload.split(",", 2);
                if (parts.length == 2) {
                    courseName = parts[0].trim();
                    instructor = parts[1].trim();
                }
            } else if (tagged.startsWith("enroll~")) {
                enrollments.add(tagged.substring("enroll~".length()));
            }
        }

        for (String enrollment : enrollments) {
            String[] parts = enrollment.split(",", 2);
            if (parts.length >= 1) {
                String studentName = parts[0].trim();
                outKey.set(courseName + ", " + instructor + ", " + studentName);
                outValue.set("");
                context.write(outKey, outValue);
            }
        }
    }
}
