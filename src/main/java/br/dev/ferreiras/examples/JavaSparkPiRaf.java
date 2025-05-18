package br.dev.ferreiras.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class JavaSparkPiRaf {

    private final static Logger logger = LoggerFactory.getLogger(JavaSparkPiRaf.class);
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPiRaf")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        logger.info("::: Spark version: {} :::", spark.version());
//        logger.info("Spark context: {}", Arrays.toString(spark.sparkContext().getConf().getAll()));
//        System.out.println("Available cores" + jsc.getConf().get("spark.executor.cores") + " cores");

        for(Tuple2<String, String> string : spark.sparkContext().getConf().getAll()) {
            logger.info("::: Key: {}, Value: {} :::", string._1, string._2);
        }

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 100000 * slices;

        logger.info("::: Number of slices: {} :::", slices);
        List<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(list, slices);

        int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y <= 1) ? 1 : 0;
        }).reduce((integer, integer2) -> integer + integer2);

        logger.info("Pi is roughly {}", 4.0 * count / n);

        spark.stop();
    }
}
