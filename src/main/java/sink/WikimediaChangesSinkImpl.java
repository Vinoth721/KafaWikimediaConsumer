package sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.util.LongAccumulator;
import java.util.concurrent.TimeoutException;

public class WikimediaChangesSinkImpl extends AbstractWikimediaChangesSink {
    @Override
    public StreamingQuery writeToSink(Dataset<Row> batch, LongAccumulator totalRecords) {
        try {
            return batch.writeStream()
                    .foreachBatch((batchFunction, batchId) -> {
                        long count = batchFunction.count();
                        System.out.println("BatchId: " + batchId + ", Count: " + count);
                        System.out.println("Total records processed: " + totalRecords.value());
                        batchFunction.show(false);
                        totalRecords.add(count);
                    })
                    .outputMode("update")
                    .trigger(Trigger.ProcessingTime("1 seconds"))
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
