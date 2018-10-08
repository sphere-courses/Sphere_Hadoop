import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountJob extends Configured implements Tool{
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        static final LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("\\p{L}+");
            Matcher matcher = pattern.matcher(value.toString());

            Set<String> uniques = new HashSet<>();

            while (matcher.find()){
                uniques.add(matcher.group().toLowerCase());
            }

            for (String word : uniques){
                context.write(new Text(word), one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
            int result = 0;
            for(LongWritable value : values){
                result += value.get();
            }
            context.write(key, new LongWritable(result));
        }
    }

    private static Job GetJobConf(Configuration conf, String input, String output) throws IOException{
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountJob.class);
        job.setJobName(WordCountJob.class.getCanonicalName());

        job.setInputFormatClass(InflaterInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountJob(), args);
        System.exit(exitCode);
    }
}