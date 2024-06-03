package transformation;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.Collections;


public class WikimediaChangesTransformationImpl extends AbstractWikimediaChangesTransformation {
    @Override
    public StructType createSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Schema Details", DataTypes.StringType, true),
                DataTypes.createStructField("URL", DataTypes.StringType, true),
                DataTypes.createStructField("Request ID", DataTypes.StringType, true),
                DataTypes.createStructField("Timestamp Type", DataTypes.StringType, true),
                DataTypes.createStructField("Domain", DataTypes.StringType, true),
                DataTypes.createStructField("Stream", DataTypes.StringType, true),
                DataTypes.createStructField("Feild n", DataTypes.StringType, true)
        });
    }

    @Override
    public Dataset<Row> processBatch(Dataset<String> batch, StructType schema) {
        return batch.flatMap((FlatMapFunction<String, Row>) s -> {
            String[] parts = s.split(",");
            if (parts.length >= 7) {
                return Collections.singletonList(RowFactory.create(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6])).iterator();
            } else {
                System.err.println("Unexpected data format: " + s);
                return Collections.emptyIterator();
            }
        }, RowEncoder.apply(schema));
    }
}