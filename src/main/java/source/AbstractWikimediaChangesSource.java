package source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractWikimediaChangesSource {
    public abstract Dataset<String> createSourceStream(SparkSession spark);
}
