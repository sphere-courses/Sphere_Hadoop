import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;


public class SeoJob extends Configured implements Tool{
    private static final String MINCLICKS = "seo.minclicks";

    private static int getMinClicks(@Nonnull Configuration configuration){
        return configuration.getInt(MINCLICKS, 0);
    }

    public static void main(String[] args) throws Exception{
        FileUtils.deleteDirectory(new File("output"));

        int exitCode = ToolRunner.run(new SeoJob(), args);
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SeoJob.class);
        job.setJobName(SeoJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setPartitionerClass(SeoPartitioner.class);

        job.setSortComparatorClass(PairWritable.PairWritableCompator.class);
        job.setGroupingComparatorClass(PairWritable.PairWritableGroupCompator.class);

        job.setMapperClass(SeoMapper.class);
        job.setReducerClass(SeoReducer.class);
        job.setNumReduceTasks(35);

        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SeoPartitioner extends Partitioner<PairWritable<Text, Text>, Text> {
        @Override
        public int getPartition(PairWritable<Text, Text> key, Text value, int nPartitions) {
            float firstChar = key.getFirst().toString().charAt(0);
            if(firstChar < 'a') return 0;
            else if(firstChar > 'z') return nPartitions - 1;
            else return (int) ((firstChar - 'a') / ('z' - 'a' + 1) * nPartitions);
        }
    }

    public static class SeoMapper extends Mapper<Text, Text, PairWritable<Text, Text>, Text>{
        @Override
        protected void map(Text query, Text url, Context context) throws IOException, InterruptedException {
            try {
                String host = (new URL(url.toString())).getHost();
                context.write(new PairWritable<>(new Text(host), query), query);
            } catch (MalformedURLException exception){
                System.out.println("MalformedURLException: " + url.toString());
            }
        }
    }

    public static class SeoReducer extends Reducer<PairWritable<Text, Text>, Text, PairWritable, LongWritable>{
        @Override
        protected void reduce(PairWritable<Text, Text> line, Iterable<Text> queries, Context context) throws IOException, InterruptedException {
            long bestNClicks = getMinClicks(context.getConfiguration()) - 1, currentNClicks = 0;
            String bestQuery = "";
            String previousQuery = "";
            for(Text query : queries){
                if(previousQuery.compareTo(query.toString()) == 0){
                    currentNClicks += 1;
                } else {
                    if(currentNClicks > bestNClicks){
                        bestNClicks = currentNClicks;
                        bestQuery = previousQuery;
                    }
                    currentNClicks = 1;
                    previousQuery = query.toString();
                }
            }
            if(currentNClicks > bestNClicks){
                bestQuery = previousQuery;
                bestNClicks = currentNClicks;
            }
            context.write(new PairWritable<>(line.getFirst(), new Text(bestQuery)), new LongWritable(bestNClicks));
        }
    }

}
