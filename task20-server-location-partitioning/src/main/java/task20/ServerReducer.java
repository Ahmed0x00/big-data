package task20;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class ServerReducer extends Reducer<Text, Text, Text, Text> {

    private Text outValue = new Text();
    private DecimalFormat decimalFormat = new DecimalFormat("0.##");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long requestCount = 0;
        long sumResponseTime = 0;
        long errorCount = 0;

        for (Text val : values) {
            String[] parts = val.toString().split("\\|");
            if (parts.length == 2) {
                int responseTime = Integer.parseInt(parts[0]);
                int statusCode = Integer.parseInt(parts[1]);

                requestCount++;
                sumResponseTime += responseTime;
                if (statusCode >= 500) {
                    errorCount++;
                }
            }
        }

        if (requestCount > 0) {
            double avg = (double) sumResponseTime / requestCount;
            String result = "Avg: " + decimalFormat.format(avg)
                    + "\tRequests: " + requestCount
                    + "\tErrors: " + errorCount;
            outValue.set(result);
            context.write(key, outValue);
        }
    }
}
