package br.dev.ferreiras.examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import scala.Tuple2;

import java.util.Arrays;

public class DataMod2PracticeOne {

    private static final Logger logger = LoggerFactory.getLogger(DataMod2PracticeOne.class);

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("DataMod2PracticeOne")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        logger.info("::: Spark version: {} :::", sparkSession.version());
        logger.info("::: Spark context: {} :::", sparkSession.sparkContext().appName());

        for (Tuple2<String, String> string : sparkSession.sparkContext().getConf().getAll()) {

            logger.info("::: Key: {}, Value: {} :::", string._1, string._2);
        }

        sparkSession.stop();
    }
}
