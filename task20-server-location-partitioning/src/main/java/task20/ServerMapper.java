package task20;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ServerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    enum Counters {
        MALFORMED_LINES,
        INVALID_RESPONSE_TIME,
        INVALID_STATUS_CODE
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // request_id, server_location, response_time, status_code, timestamp
        String[] parts = value.toString().split(",");

        if (parts.length == 5) {
            String serverLocation = parts[1].trim();
            String responseTimeStr = parts[2].trim();
            String statusCodeStr = parts[3].trim();

            try {
                int responseTime = Integer.parseInt(responseTimeStr);
                int statusCode = Integer.parseInt(statusCodeStr);

                if (responseTime <= 0) {
                    context.getCounter(Counters.INVALID_RESPONSE_TIME).increment(1);
                    return;
                }

                outKey.set(serverLocation);
                outValue.set(responseTime + "|" + statusCode);
                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
                if (!responseTimeStr.matches("-?\\d+")) {
                    context.getCounter(Counters.INVALID_RESPONSE_TIME).increment(1);
                } else {
                    context.getCounter(Counters.INVALID_STATUS_CODE).increment(1);
                }
            }
        } else {
            context.getCounter(Counters.MALFORMED_LINES).increment(1);
        }
    }
}
