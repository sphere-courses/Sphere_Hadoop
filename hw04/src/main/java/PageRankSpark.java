import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

import java.util.ArrayList;

public class PageRankSpark {
    public static void main(String[] args) {
        long nIterations = Long.parseLong(args[0]);
        final long nPages = 4847571;
        final double d = 0.85;

        SparkSession spark = SparkSession
                .builder()
                .appName("PageRankSpark")
                .config("spark.master", "local[*]")
                .config("spark.sql.warehouse.dir", "file:///spark-warehouse")
                .getOrCreate();

        JavaPairRDD<Long, Iterable<Long>> pageToPages = spark
                .read()
                .textFile(args[1])
                .javaRDD()
                .filter(line -> !line.contains("#"))
                .flatMapToPair(
                        line -> {
                            String[] value = line.split("\t");
                            ArrayList <Tuple2<Long, Long>> edges = new ArrayList<>();
                            long from = Long.parseLong(value[0]), to = Long.parseLong(value[1]);
                            edges.add(new Tuple2<>(from, to));
                            edges.add(new Tuple2<>(from, -1L));
                            edges.add(new Tuple2<>(to, -1L));

                            return edges.iterator();
                        })
                .distinct()
                .groupByKey()
                .cache();

        JavaPairRDD<Long, Double> pagesPR = pageToPages
                .mapValues(value -> 1. / nPages);

        DoubleAccumulator leakedPR = spark.sparkContext().doubleAccumulator();
        DoubleAccumulator leakedPRNew = spark.sparkContext().doubleAccumulator();

        for(int i = 0; i < nIterations; ++i){
            pagesPR = pageToPages
                    .cogroup(pagesPR)
                    .flatMapToPair(
                            page -> {
                                long currentNPages = 0;
                                ArrayList<Long> toIds = new ArrayList<>();
                                ArrayList<Tuple2<Long, Double>> result = new ArrayList<>();
                                for(Iterable<Long> iterable : page._2()._1()){
                                    for(long to : iterable){
                                        if(to != -1){
                                            ++currentNPages;
                                            toIds.add(to);
                                        }
                                    }
                                }

                                double currentPR = page._2()._2().iterator().next() + leakedPR.value() / nPages;
                                if(toIds.size() == 0) {
                                    leakedPRNew.add(currentPR);
                                } else {
                                    for (Long to : toIds) {
                                        result.add(new Tuple2<>(to, currentPR / currentNPages));
                                    }
                                }
                                result.add(new Tuple2<>(page._1(), 0.));
                                return result.iterator();
                            }
                    )
                    .reduceByKey((a, b) -> a + b)
                    .mapValues(a -> (1 - d) / nPages + d * a);
            leakedPR.setValue(leakedPRNew.value());
            leakedPRNew.reset();
        }
        pagesPR
                .mapValues(a -> a + leakedPR.value() / nPages)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .saveAsTextFile(args[2]);
        spark.stop();
    }
}