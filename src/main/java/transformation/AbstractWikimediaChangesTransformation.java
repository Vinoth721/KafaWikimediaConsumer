package transformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;



public abstract class AbstractWikimediaChangesTransformation {
    public abstract StructType createSchema();

    public abstract Dataset<Row> processBatch(Dataset<String> batch, StructType schema);
}
