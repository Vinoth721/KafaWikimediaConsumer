package org.Kafkastream;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import sink.AbstractWikimediaChangesSink;
import sink.WikimediaChangesSinkImpl;
import source.AbstractWikimediaChangesSource;
import source.WikimediaChangesSourceImpl;
import transformation.AbstractWikimediaChangesTransformation;
import transformation.WikimediaChangesTransformationImpl;

public class WikimediaChangesConsumerMain {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaStreamingExample")
                .config("spark.master", "local")
                .getOrCreate();

        LongAccumulator totalRecords = spark.sparkContext().longAccumulator("TotalRecords");

        AbstractWikimediaChangesSource source = new WikimediaChangesSourceImpl();
        Dataset<String> dfJson = source.createSourceStream(spark);

        AbstractWikimediaChangesTransformation transformation = new WikimediaChangesTransformationImpl();
        StructType schema = transformation.createSchema();
            Dataset<Row> processedData = transformation.processBatch(dfJson, schema);

        AbstractWikimediaChangesSink sink = new WikimediaChangesSinkImpl();
        StreamingQuery query = sink.writeToSink(processedData, totalRecords);


        query.awaitTermination();
    }
}
