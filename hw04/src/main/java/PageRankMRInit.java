import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PageRankMRInit extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new PageRankMRInit(),
                new String[]{
                    args[0],
                    args[1] + "/0/",
                    args[1] + "/leakedPR/"
                }
                );
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir, String leakedPRFile) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageRankMRInit.class);
        job.setJobName(PageRankMRInit.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(PageRankMRInitMapper.class);
        job.setReducerClass(PageRankMRInitReducer.class);
        job.setNumReduceTasks(32);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setLong("nPages", 4847571);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path[] leakedPRPath = {new Path(leakedPRFile + "0"), new Path(leakedPRFile + "1")};
        FSDataOutputStream[] outputStream = {fs.create(leakedPRPath[0]), fs.create(leakedPRPath[1])};
        outputStream[0].writeBytes(Double.toString(0.));
        outputStream[1].writeBytes(Double.toString(0.));

        outputStream[0].close();
        outputStream[1].close();

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1], args[2]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PageRankMRInitMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text fromId, Text toId, Context context) throws IOException, InterruptedException {
            if(fromId.toString().startsWith("#")){
                return;
            }
            context.write(fromId, toId);
        }
    }

    public static class PageRankMRInitReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text fromId, Iterable<Text> toIds, Context context) throws IOException, InterruptedException {
            long nPages = context.getConfiguration().getLong("nPages", 0);
            String key = fromId + "," + String.valueOf(1. / nPages);
            StringBuilder value = new StringBuilder();
            for(Text toId : toIds){
                value.append(toId.toString()).append(",");
            }
            // get substring is ok because all fromId vertices non-leave
            context.write(new Text(key), new Text(value.substring(0, value.length() - 1)));
        }
    }
}
