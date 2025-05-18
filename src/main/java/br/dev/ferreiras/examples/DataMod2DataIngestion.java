package br.dev.ferreiras.examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMOd2DataIngestion {

    private final static Logger logger = LoggerFactory.getLogger(DataMOd2DataIngestion.class);

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("DataMOd2DataIngestion")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        logger.info("::: Spark version: {} :::", spark.version());
        logger.info("::: Spark context: {} :::", spark.sparkContext().appName());


    }
}
