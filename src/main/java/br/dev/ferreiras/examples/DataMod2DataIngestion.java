package br.dev.ferreiras.examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

public class DataMod2DataIngestion {

    private final static Logger logger = LoggerFactory.getLogger(DataMod2DataIngestion.class);

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("DataMod2DataIngestion")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        logger.info("::: Spark version: {} :::", sparkSession.version());
        logger.info("::: Spark context: {} :::", sparkSession.sparkContext().appName());

        String restaurantsPathJson = "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl";
        logger.info("::: File Path at: {} :::", restaurantsPathJson);



//        val schema = StructType(List(
//                StructField("id", IntegerType, true),
//                StructField("name", StringType, true)
//        ))


        StructType rest_sch_json = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("country", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("restaurant_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("phone_number", DataTypes.StringType, true),
                DataTypes.createStructField("cnpj", DataTypes.StringType, true),
                DataTypes.createStructField("average_rating", DataTypes.DoubleType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("uuid", DataTypes.StringType, true),
                DataTypes.createStructField("address", DataTypes.StringType, true),
                DataTypes.createStructField("opening_time", DataTypes.StringType, true),
                DataTypes.createStructField("cuisine_type", DataTypes.StringType, true),
                DataTypes.createStructField("closing_time", DataTypes.StringType, true),
                DataTypes.createStructField("num_reviews", DataTypes.IntegerType, true),
                DataTypes.createStructField("dt_current_timestamp", DataTypes.StringType, true)
        ));

        Dataset<Row> df = sparkSession.read()
                .schema(rest_sch_json)
                .json(restaurantsPathJson);

        logger.info("::: Total: {} ::: ", df.count());
        logger.info("::: Schema: {} ::: ", df.schema());

        df.show();
        sparkSession.stop();
    }
}
