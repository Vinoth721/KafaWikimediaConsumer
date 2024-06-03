package source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class WikimediaChangesSourceImpl extends AbstractWikimediaChangesSource {
    @Override
    public Dataset<String> createSourceStream(SparkSession spark)
    {
        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "wikimedia")
                .load()
                .selectExpr("CAST(value AS STRING) as json_value")
                .as(Encoders.STRING());
    }
}
