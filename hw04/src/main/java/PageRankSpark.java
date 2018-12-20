import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import static com.google.common.collect.Iterables.concat;

public class PageRankSpark {
    public static void main(String[] args) {
        long nIterations = Long.parseLong(args[0]);
        final long nPages = 4847571;
        final double d = 0.85;

        final SparkConf conf = new SparkConf()
                .setAppName("PageRankSpark");
        final JavaSparkContext spark = new JavaSparkContext(conf);
        final int nExecutors = spark.getConf().getInt("spark.executor.instances", 20);

        JavaPairRDD<Long, Iterable<Long>> pageToPages = spark
                .textFile(args[1])
                .repartition(2 * nExecutors)
                .filter(line -> !line.contains("#"))
                .flatMapToPair(
                        line -> {
                            String[] value = line.split("\t");
                            ArrayList <Tuple2<Long, Iterable<Long>>> edges = new ArrayList<>();
                            long from = Long.parseLong(value[0]), to = Long.parseLong(value[1]);
                            edges.add(new Tuple2<>(from, new ArrayList<>(Arrays.asList(to, -1L))));
                            edges.add(new Tuple2<>(to, new ArrayList<>(Arrays.asList(-1L))));

                            return edges;
                        })
                .reduceByKey(
                        (left, right) -> {
                            HashSet<Long> result = new HashSet<>();
                            for (Long val: concat(left, right)){
                                result.add(val);
                            }
                            return new ArrayList<>(result);
                        }
                )
                .cache();

        JavaPairRDD<Long, Double> pagesPR = pageToPages
                .mapValues(value -> 1. / nPages);

        Accumulator<Double> leakedPR = spark.accumulator(0.0, new DoubleAccumulatorSpark());
        Accumulator<Double> leakedPRNew = spark.accumulator(0.0, new DoubleAccumulatorSpark());

        for(int i = 0; i < nIterations; ++i){
            Double leakedPRValue = leakedPR.value();
            pagesPR = pageToPages
                    .cogroup(pagesPR)
                    .flatMapToPair(
                            page -> {
                                long currentNPages = 0;
                                double leakedPRLocal = 0.;
                                ArrayList<Tuple2<Long, Double>> result = new ArrayList<>();
                                for(Long to : page._2()._1().iterator().next()){
                                    if(to != -1){
                                        ++currentNPages;
                                    }
                                }

                                double currentPR = leakedPRValue / nPages;
                                if(page._2()._2().iterator().hasNext()) {
                                    currentPR += page._2()._2().iterator().next();
                                }
                                if(currentNPages == 0) {
                                    leakedPRLocal += currentPR;
                                } else {
                                    for(Long to : page._2()._1().iterator().next()){
                                        if(to != -1){
                                            result.add(new Tuple2<>(to, currentPR / currentNPages));
                                        }
                                    }
                                }
                                leakedPRNew.add(leakedPRLocal);
                                return result;
                            }
                    )
                    .reduceByKey((a, b) -> a + b)
                    .mapValues(a -> (1 - d) / nPages + d * a);
            leakedPR.setValue(leakedPRNew.value());
            leakedPRNew.setValue(0.);
        }
        Double leakedPRValue = leakedPR.value();
        pagesPR
                .mapValues(a -> a + leakedPRValue / nPages)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .saveAsTextFile(args[2]);
        spark.stop();
    }
}