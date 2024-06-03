package sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.util.LongAccumulator;

public abstract class AbstractWikimediaChangesSink {
    public abstract StreamingQuery writeToSink(Dataset<Row> batch, LongAccumulator totalRecords);
}
